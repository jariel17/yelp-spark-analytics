"""
Pipeline ETL completo

Spark maneja lectura, transformación y escritura de extremo a extremo.

Ejecutar:
    uv run etl/pipeline.py

Flujo:
    1. Configuración    — variables de entorno y constantes
    2. Spark            — sesión con drivers JDBC y MongoDB
    3. Lectura          — JSON crudos del dataset Yelp
    4. Limpieza         — filtros, trim, fillna sobre business
    5. Transformación   — fragmentación en 5 DataFrames por destino
    6. Validación       — conteos y muestra antes de escribir
    7. Carga            — escritura a CockroachDB y MongoDB Atlas
"""
import os
from urllib.parse import urlparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from dotenv import load_dotenv

# =============================================================================
# 1. CONFIGURACIÓN
# =============================================================================

load_dotenv()

CRDB_CONNECTION         = os.getenv("CRDB_CONNECTION")
ATLAS_URI_DIMENSIONS    = os.getenv("ATLAS_URI_DIMENSIONS")
ATLAS_URI_CHECKINS_TIPS = os.getenv("ATLAS_URI_CHECKINS_TIPS")

CRDB_DATA_LOADED            = os.getenv("CRDB_DATA_LOADED",            "false").lower() == "true"
ATLAS_DIMENSIONS_LOADED     = os.getenv("ATLAS_DIMENSIONS_LOADED",     "false").lower() == "true"
ATLAS_BUSINESS_FACTS_LOADED = os.getenv("ATLAS_BUSINESS_FACTS_LOADED", "false").lower() == "true"
ATLAS_CHECKINS_TIPS_LOADED  = os.getenv("ATLAS_CHECKINS_TIPS_LOADED",  "false").lower() == "true"

# Si el cluster de dimensions se acerca al límite de 512 MB, bajar a 0.95
DIMENSIONS_SAMPLE_FRACTION = float(os.getenv("DIMENSIONS_SAMPLE_FRACTION", "1.0"))

DATA_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "raw")

# =============================================================================
# 2. SESIÓN DE SPARK
# =============================================================================

# org.postgresql       → driver JDBC para CockroachDB (protocolo PostgreSQL)
# mongo-spark-connector → conector oficial MongoDB para Spark 3.x / Scala 2.13
spark = SparkSession.builder \
    .appName("YelpBigData-Pipeline") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.jars.packages",
            "org.postgresql:postgresql:42.7.3,"
            "org.mongodb.spark:mongo-spark-connector_2.13:10.3.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =============================================================================
# 3. LECTURA DE FUENTES
# =============================================================================

print("\n=== Lectura de fuentes ===")

# business.json se usa para tres destinos distintos (listings, business_facts, dimensions)
df_business = spark.read.json(os.path.join(DATA_PATH, "yelp_academic_dataset_business.json"))
df_checkin  = spark.read.json(os.path.join(DATA_PATH, "yelp_academic_dataset_checkin.json"))
df_tip      = spark.read.json(os.path.join(DATA_PATH, "yelp_academic_dataset_tip.json"))

print("Archivos leídos — plan de ejecución construido, datos aún no procesados")

# =============================================================================
# 4. LIMPIEZA
# =============================================================================

print("\n=== Limpieza ===")

# Limpieza base de business — se aplica una sola vez antes de fragmentar.
#   - Filtro de PK: business_id nulo rompería el PRIMARY KEY en CockroachDB
#   - trim(): evita que " Nevada" y "Nevada" sean valores distintos en GROUP BY
#   - fillna(): evita que nulls se propaguen a agregaciones de SparkSQL
df_business = (
    df_business
    .filter(F.col("business_id").isNotNull())
    .withColumn("name",  F.trim(F.col("name")))
    .withColumn("city",  F.trim(F.col("city")))
    .withColumn("state", F.trim(F.col("state")))
    .fillna({"is_open": 0, "review_count": 0})
)

# checkins: solo descartar eventos sin negocio
df_checkin = df_checkin.filter(F.col("business_id").isNotNull())

# tips: descartar eventos sin claves de referencia + fillna en la métrica
df_tip = (
    df_tip
    .filter(F.col("business_id").isNotNull() & F.col("user_id").isNotNull())
    .fillna({"compliment_count": 0})
)

print("Limpieza aplicada")

# =============================================================================
# 5. TRANSFORMACIÓN
# =============================================================================

print("\n=== Transformación ===")

# listings — DIMENSIONES en CockroachDB
# Solo atributos descriptivos estáticos: quién es el negocio y dónde está.
# stars y review_count se excluyen (son métricas dinámicas), irán a business_facts.
df_listings = df_business.select(
    F.col("business_id"),
    F.col("name"),
    F.col("city"),
    F.col("state"),
    F.col("is_open").cast("int"),
)

# business_facts — HECHOS en MongoDB (cluster Ariel)
# Métricas que cambian con cada nueva reseña. Estos son datos que en teoría vendrían de kafka
df_business_facts = df_business.select(
    F.col("business_id"),
    F.col("stars").cast("decimal(2,1)").alias("avg_stars"),
    F.col("review_count").cast("int"),
)

# dimensions — DIMENSIONES semiestructuradas en MongoDB (cluster Ariel)
# attributes, hours y categories tienen esquema irregular entre negocios,
# MongoDB es el destino natural para estos campos que no siguen un esquema fijo.
df_dimensions = df_business.select(
    F.col("business_id"),
    F.col("attributes"),
    F.col("hours"),
    F.col("categories"),
)
# if DIMENSIONS_SAMPLE_FRACTION < 1.0:
#     print(f"  Aplicando sample a dimensions: {DIMENSIONS_SAMPLE_FRACTION:.0%}")
#     df_dimensions = df_dimensions.sample(fraction=DIMENSIONS_SAMPLE_FRACTION, seed=42)

# checkins — HECHOS transaccionales en MongoDB (cluster B)
# Se carga sin explode — el campo date es un string con timestamps separados
# por coma. El explode ocurre en Spark al leer
# evitar 13M filas en la BD de free tier.
df_checkins = df_checkin

# tips — HECHOS transaccionales en MongoDB (cluster B)
# Cada documento es un evento: usuario, negocio, texto, fecha y métrica.
df_tips = df_tip.select(
    F.col("user_id"),
    F.col("business_id"),
    F.col("text"),
    F.col("date"),
    F.col("compliment_count").cast("int"),
)

print("Transformaciones aplicadas")

# =============================================================================
# 6. VALIDACIÓN
# =============================================================================

print("\n=== Validación ===")

# Las acciones count() disparan la ejecución del plan de Spark aquí.
# Si algo falla (archivo corrupto, campo faltante), falla antes de escribir.
datasets = {
    "listings       (CockroachDB)": df_listings,
    "business_facts (MongoDB)":     df_business_facts,
    "dimensions     (MongoDB)":     df_dimensions,
    "checkins       (MongoDB)":     df_checkins,
    "tips           (MongoDB)":     df_tips,
}

for nombre, df in datasets.items():
    print(f"  {nombre}: {df.count():,} registros")
    df.show(2, truncate=80)

# =============================================================================
# 7. CARGA
# =============================================================================

print("\n=== Carga ===")

# Verificar que todos los destinos ya están cargados antes de continuar
todo_cargado = (
    CRDB_DATA_LOADED
    and ATLAS_DIMENSIONS_LOADED
    and ATLAS_BUSINESS_FACTS_LOADED
    and (ATLAS_CHECKINS_TIPS_LOADED or not ATLAS_URI_CHECKINS_TIPS)
)
if todo_cargado:
    print("Todos los destinos ya cargados. Saliendo sin cambios.")
    spark.stop()
    exit(0)

if not CRDB_CONNECTION:
    raise ValueError("CRDB_CONNECTION no está definido en .env")
if not ATLAS_URI_DIMENSIONS:
    raise ValueError("ATLAS_URI_DIMENSIONS no está definido en .env")


def construir_jdbc(crdb_conn: str) -> tuple[str, dict]:
    """
    Convierte la cadena de conexión a formato JDBC para Spark.

    psycopg2 acepta: postgresql://user:pass@host:port/db?sslmode=verify-full
    JDBC necesita:   jdbc:postgresql://host:port/db?sslmode=verify-full
    con user/password como propiedades separadas.
    """
    parsed   = urlparse(crdb_conn)
    cert     = os.path.expanduser("~/.postgresql/root.crt")
    jdbc_url = (
        f"jdbc:postgresql://{parsed.hostname}:{parsed.port}{parsed.path}"
        f"?sslmode=verify-full&sslrootcert={cert}"
    )
    props = {
        "driver":   "org.postgresql.Driver",
        "user":     parsed.username,
        "password": parsed.password,
    }
    return jdbc_url, props


def cargar_listings(df: DataFrame, jdbc_url: str, props: dict) -> None:
    """
    Escribe df_listings a CockroachDB tabla 'listings' via JDBC.

    createTableColumnTypes le da a Spark los tipos SQL exactos para el DDL
    generado.
    """
    print("  Escribiendo listings → CockroachDB...")
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "listings") \
        .option("driver", props["driver"]) \
        .option("user", props["user"]) \
        .option("password", props["password"]) \
        .option("numPartitions", "4") \
        .mode("overwrite") \
        .save()
    print("  listings: cargado")


def cargar_mongo(df: DataFrame, uri: str, coleccion: str) -> None:
    """
    Escribe un DataFrame a una colección de MongoDB Atlas.

    Recibe el URI como parámetro para poder escribir a clusters distintos
    desde el mismo script. mode overwrite garantiza idempotencia.
    """
    print(f"  Escribiendo {coleccion} → MongoDB Atlas...")
    df.write \
        .format("mongodb") \
        .option("connection.uri", uri) \
        .option("database",       "yelp") \
        .option("collection",     coleccion) \
        .mode("overwrite") \
        .save()
    print(f"  {coleccion}: cargado")


# --- CockroachDB ---
if not CRDB_DATA_LOADED:
    print("\n[CockroachDB]")
    jdbc_url, jdbc_props = construir_jdbc(CRDB_CONNECTION)
    cargar_listings(df_listings, jdbc_url, jdbc_props)
    print("  → Actualiza CRDB_DATA_LOADED=true en .env")
else:
    print("[CockroachDB] CRDB_DATA_LOADED=true — omitiendo")

# --- MongoDB Atlas — cluster Ariel (dimensions + business_facts) ---
if not ATLAS_DIMENSIONS_LOADED:
    print("\n[MongoDB Atlas — cluster propio — dimensions]")
    cargar_mongo(df_dimensions, ATLAS_URI_DIMENSIONS, "dimensions")
    print("  → Actualiza ATLAS_DIMENSIONS_LOADED=true en .env")
else:
    print("[MongoDB Atlas — cluster propio] ATLAS_DIMENSIONS_LOADED=true — omitiendo")

if not ATLAS_BUSINESS_FACTS_LOADED:
    print("\n[MongoDB Atlas — cluster propio — business_facts]")
    cargar_mongo(df_business_facts, ATLAS_URI_DIMENSIONS, "business_facts")
    print("  → Actualiza ATLAS_BUSINESS_FACTS_LOADED=true en .env")
else:
    print("[MongoDB Atlas — cluster propio] ATLAS_BUSINESS_FACTS_LOADED=true — omitiendo")

# --- MongoDB Atlas — cluster B (checkins + tips) ---
if not ATLAS_CHECKINS_TIPS_LOADED:
    if ATLAS_URI_CHECKINS_TIPS:
        print("\n[MongoDB Atlas — cluster B]")
        cargar_mongo(df_checkins, ATLAS_URI_CHECKINS_TIPS, "checkins")
        cargar_mongo(df_tips,     ATLAS_URI_CHECKINS_TIPS, "tips")
        print("  → Actualiza ATLAS_CHECKINS_TIPS_LOADED=true en .env")
    else:
        print("\n[MongoDB Atlas — cluster B] omitido — ATLAS_URI_CHECKINS_TIPS no definido")
else:
    print("[MongoDB Atlas — cluster B] ATLAS_CHECKINS_TIPS_LOADED=true — omitiendo")

# =============================================================================

print("\n=== Pipeline completado ===")
spark.stop()
