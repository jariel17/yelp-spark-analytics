"""

Ejecución:
    uv run python analytics/consultas.py
"""
import os
import time
import math
import pandas as pd
import matplotlib
import seaborn as sns
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FILTERED = os.path.join(BASE_DIR, "..", "data", "filtered_trimmed")
GRAFICOS = os.path.join(BASE_DIR, "..", "graficos")

os.makedirs(GRAFICOS, exist_ok=True)

# =============================================================================
# SPARK SESSION
# =============================================================================

spark = SparkSession.builder \
    .appName("YelpBigData-Queries") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config(
        "spark.jars.packages",
        "org.mongodb.spark:mongo-spark-connector_2.13:10.4.0,"
        "org.postgresql:postgresql:42.7.3",
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =============================================================================
# CARGA DE DATOS DESDE ARCHIVOS LOCALES
# =============================================================================

def cargar_vistas_local() -> None:
    """
    Lee los 5 archivos JSON filtrados con spark.read.json() y los registra
    como vistas temporales SparkSQL.  El campo date de checkin.json es un string
    con timestamps separados por coma; se explota aquí una sola vez para que
    todas las queries reciban una fila por evento.
    """
    print("Cargando archivos JSON desde data/filtered_trimmed/ ...")
    t0 = time.time()

    # business — dimensiones y hechos de negocio
    df_business = spark.read.json(
        f"{FILTERED}/yelp_academic_dataset_business.json"
    )
    df_business.createOrReplaceTempView("business")

    # reviews — hechos transaccionales de reseñas
    df_reviews = spark.read.json(
        f"{FILTERED}/yelp_academic_dataset_review.json"
    )
    df_reviews.createOrReplaceTempView("reviews")

    # tips — hechos transaccionales de tips
    df_tips = spark.read.json(
        f"{FILTERED}/yelp_academic_dataset_tip.json"
    )
    df_tips.createOrReplaceTempView("tips")

    # users — dimensión de usuarios
    df_users = spark.read.json(
        f"{FILTERED}/yelp_academic_dataset_user.json"
    )
    df_users.createOrReplaceTempView("users")

    # checkins — el campo date es un string con timestamps separados por coma.
    # SPLIT + EXPLODE convierte cada timestamp en una fila individual.
    # Este explode ocurre en Spark en memoria local — nunca se materializa en la nube.
    df_checkins_raw = spark.read.json(
        f"{FILTERED}/yelp_academic_dataset_checkin.json"
    )
    df_checkins = df_checkins_raw \
        .withColumn("checkin_ts", F.explode(F.split(F.col("date"), ", "))) \
        .select(
            F.col("business_id"),
            F.col("checkin_ts").cast("timestamp").alias("checkin_ts"),
        )
    df_checkins.createOrReplaceTempView("checkins")

    print(f"Vistas listas en {time.time() - t0:.1f}s\n")


# =============================================================================
# CARGA DE DATOS DESDE LOS CLUSTERS
# =============================================================================

def cargar_vistas_clusters() -> None:
    """
    Lee las 5 tablas/colecciones directamente desde los clusters en la nube y las
    registra como vistas temporales SparkSQL con los mismos nombres que
    cargar_vistas_locales(), para que las queries funcionen con ambas fuentes.

    Fuentes por colección:
        business  → MongoDB Atlas M0 #1  (ATLAS_URI_BUSINESS_READ)
        tips      → MongoDB Atlas M0 #2  (ATLAS_URI_TIPS_READ)
        checkins  → MongoDB Atlas M0 #3  (ATLAS_URI_CHECKINS_READ)
        reviews   → Aiven PostgreSQL     (AIVEN_CONNECTION_READ)
        users     → CockroachDB          (CRDB_CONNECTION_READ)
    """
    from urllib.parse import urlparse
    from dotenv import load_dotenv
    load_dotenv()

    print("Cargando datos desde los clusters en la nube ...")
    t0 = time.time()

    # -------------------------------------------------------------------------
    # MongoDB Atlas — business (cluster #1)
    # Contiene los campos de dimensión y hechos del negocio.
    # -------------------------------------------------------------------------
    atlas_business = os.getenv("ATLAS_URI_BUSINESS_READ")
    df_business = (
        spark.read.format("mongodb")
        .option("connection.uri", atlas_business)
        .option("database", "yelp")
        .option("collection", "business")
        .load()
    )
    df_business.createOrReplaceTempView("business")
    print("  [OK] business  → vista registrada")

    # -------------------------------------------------------------------------
    # MongoDB Atlas — tips (cluster #2)
    # Eventos transaccionales de texto corto.
    # -------------------------------------------------------------------------
    atlas_tips = os.getenv("ATLAS_URI_TIPS_READ")
    df_tips = (
        spark.read.format("mongodb")
        .option("connection.uri", atlas_tips)
        .option("database", "yelp")
        .option("collection", "tips")
        .load()
    )
    df_tips.createOrReplaceTempView("tips")
    print("  [OK] tips      → vista registrada")

    # -------------------------------------------------------------------------
    # MongoDB Atlas — checkins (cluster #3)
    # -------------------------------------------------------------------------
    atlas_checkins = os.getenv("ATLAS_URI_CHECKINS_READ")
    df_checkins_raw = (
        spark.read.format("mongodb")
        .option("connection.uri", atlas_checkins)
        .option("database", "yelp")
        .option("collection", "checkins")
        .load()
    )
    df_checkins = (
        df_checkins_raw
        .withColumn("checkin_ts", F.explode(F.split(F.col("date"), ", ")))
        .select(
            F.col("business_id"),
            F.col("checkin_ts").cast("timestamp").alias("checkin_ts"),
        )
    )
    df_checkins.createOrReplaceTempView("checkins")
    print("  [OK] checkins  → vista registrada (explode lazy)")

    # -------------------------------------------------------------------------
    # Aiven PostgreSQL — reviews (JDBC)
    # sslmode=verify-ca si AIVEN_SSL_CERT está definido, sslmode=require si no.
    # -------------------------------------------------------------------------
    aiven_conn  = os.getenv("AIVEN_CONNECTION_READ")
    aiven_cert  = os.getenv("AIVEN_SSL_CERT", "")
    aiven_p     = urlparse(aiven_conn)

    if aiven_cert:
        aiven_ssl  = "verify-ca"
        aiven_cert = os.path.expanduser(aiven_cert)
        aiven_jdbc = (
            f"jdbc:postgresql://{aiven_p.hostname}:{aiven_p.port}"
            f"{aiven_p.path}?sslmode={aiven_ssl}&sslrootcert={aiven_cert}"
        )
    else:
        aiven_jdbc = (
            f"jdbc:postgresql://{aiven_p.hostname}:{aiven_p.port}"
            f"{aiven_p.path}?sslmode=require"
        )

    df_reviews = (
        spark.read.format("jdbc")
        .option("url", aiven_jdbc)
        .option("dbtable", "reviews")
        .option("driver", "org.postgresql.Driver")
        .option("user", aiven_p.username)
        .option("password", aiven_p.password)
        .load()
    )
    df_reviews.createOrReplaceTempView("reviews")
    print("  [OK] reviews   → vista registrada")

    # -------------------------------------------------------------------------
    # CockroachDB Serverless — users (JDBC)
    # -------------------------------------------------------------------------
    crdb_conn = os.getenv("CRDB_CONNECTION_READ")
    crdb_cert = os.path.expanduser(
        os.getenv("CRDB_SSL_CERT", "~/.postgresql/root.crt")
    )
    crdb_p    = urlparse(crdb_conn)
    crdb_jdbc = (
        f"jdbc:postgresql://{crdb_p.hostname}:{crdb_p.port}"
        f"{crdb_p.path}?sslmode=verify-full&sslrootcert={crdb_cert}"
    )

    df_users = (
        spark.read.format("jdbc")
        .option("url", crdb_jdbc)
        .option("dbtable", "users")
        .option("driver", "org.postgresql.Driver")
        .option("user", crdb_p.username)
        .option("password", crdb_p.password)
        .load()
    )
    df_users.createOrReplaceTempView("users")
    print("  [OK] users     → vista registrada")

    print(f"\nVistas listas en {time.time() - t0:.1f}s\n")


# =============================================================================
# DEFINICIÓN DE CONSULTAS Y GRÁFICOS
# =============================================================================

def query_01() -> None:
    """
    Q01 — Ranking de Ciudades por Índice de Oportunidad de Mercado

    Pregunta ejecutiva: ¿Qué ciudades combinan calidad, demanda y viabilidad?
    Fuentes:  business
    Técnicas: KPI compuesto (avg_stars × tasa_supervivencia × LOG(demanda)),
              HAVING, LOG()
    Gráfico:  Scatter plot — avg_stars (X) vs % abiertos (Y), burbuja = total_reviews
    """
    print("\n" + "=" * 60)
    print("Q01 — Índice de oportunidad de mercado por ciudad")
    print("=" * 60)

    df = spark.sql("""
        SELECT
            b.city, b.state,
            COUNT(*)                                              AS total_negocios,
            ROUND(AVG(b.stars), 2)                                AS avg_stars,
            ROUND(SUM(b.is_open) / COUNT(*) * 100, 1)             AS pct_abiertos,
            SUM(b.review_count)                                   AS total_reviews,
            ROUND(
                AVG(b.stars)
                * (SUM(b.is_open) / COUNT(*))
                * LOG(SUM(b.review_count) + 1),
                3
            ) AS market_score
        FROM business b
        GROUP BY b.city, b.state
        HAVING COUNT(*) >= 30
        ORDER BY market_score DESC
        LIMIT 20
    """)

    df.show(20, truncate=False)

    pdf = df.toPandas()
    # Escalar tamaño de burbuja para legibilidad
    sizes = (pdf["total_reviews"] / pdf["total_reviews"].max() * 800) + 50

    fig, ax = plt.subplots(figsize=(11, 7))
    scatter = ax.scatter(
        pdf["avg_stars"], pdf["pct_abiertos"],
        s=sizes, alpha=0.7,
        c=pdf["market_score"], cmap="viridis",
        edgecolors="black", linewidths=0.4,
    )
    plt.colorbar(scatter, ax=ax, label="Market Score")

    for _, row in pdf.iterrows():
        ax.annotate(
            f"{row['city']}, {row['state']}",
            (row["avg_stars"], row["pct_abiertos"]),
            fontsize=7, ha="left", va="bottom",
            xytext=(3, 3), textcoords="offset points",
        )

    ax.set_title("Q01 — Índice de oportunidad de mercado (top-20 ciudades)", pad=12)
    ax.set_xlabel("Rating promedio (avg_stars)")
    ax.set_ylabel("% de negocios aún abiertos")
    ax.grid(linestyle="--", alpha=0.4)
    plt.tight_layout()

    ruta = os.path.join(GRAFICOS, "q01_oportunidad_mercado_ciudad.png")
    plt.savefig(ruta, dpi=150)
    plt.close()
    print(f"  Gráfico guardado: {ruta}")

def query_02() -> None:
    """
    Q02 — Análisis de Franjas Horarias y Tráfico de Check-ins

    Pregunta ejecutiva: ¿En qué franja del día concentran más visitas los
                        negocios más populares por ciudad?
    Fuentes:  checkins + business
    Técnicas: EXTRACT(HOUR) · CASE/WHEN franjas · 3 CTEs ·
              RANK() particionado por ciudad y franja · filtro de 4 ciudades
    Gráfico:  Bar chart seaborn — franja horaria vs checkins, hue = ciudad
    """
    print("\n" + "=" * 60)
    print("Q02 — Análisis de franjas horarias y tráfico de check-ins")
    print("=" * 60)

    df = spark.sql("""
        WITH CheckinMoments AS (
            SELECT
                business_id,
                EXTRACT(HOUR FROM checkin_ts) AS checkin_hour,
                CASE
                    WHEN EXTRACT(HOUR FROM checkin_ts) BETWEEN 6 AND 11  THEN 'Morning (6-12)'
                    WHEN EXTRACT(HOUR FROM checkin_ts) BETWEEN 12 AND 17 THEN 'Afternoon (12-18)'
                    WHEN EXTRACT(HOUR FROM checkin_ts) BETWEEN 18 AND 23 THEN 'Evening (18-24)'
                    ELSE 'Night/Early Morning (0-6)'
                END AS day_segment
            FROM checkins
        ),
        BusinessTraffic AS (
            SELECT
                b.city,
                b.name,
                cm.day_segment,
                COUNT(*) AS checkin_count
            FROM CheckinMoments cm
            JOIN business b ON cm.business_id = b.business_id
            GROUP BY b.city, b.name, cm.day_segment
        ),
        RankedTraffic AS (
            SELECT
                city,
                name,
                day_segment,
                checkin_count,
                RANK() OVER (
                    PARTITION BY city, day_segment
                    ORDER BY checkin_count DESC
                ) AS traffic_rank
            FROM BusinessTraffic
        )
        SELECT
            city,
            day_segment,
            name,
            checkin_count
        FROM RankedTraffic
        WHERE traffic_rank <= 3
          AND city IN ('Philadelphia', 'Tampa', 'Tucson', 'Nashville')
        ORDER BY city ASC, checkin_count DESC
    """)

    df.show(40, truncate=False)

    pandas_traffic = df.toPandas()

    plt.figure(figsize=(15, 8))
    sns.barplot(
        data=pandas_traffic,
        x="checkin_count",
        y="day_segment",
        hue="city",
        errorbar=None,
    )
    plt.title("Check-ins Totales por Franja Horaria y Ciudad (Top 3 Negocios)")
    plt.xlabel("Volumen de Check-ins")
    plt.ylabel("Franja Horaria")
    plt.legend(title="Ciudad", bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.grid(axis="x", linestyle="--", alpha=0.6)
    plt.tight_layout()

    ruta = os.path.join(GRAFICOS, "q02_franjas_horarias_trafico.png")
    plt.savefig(ruta, dpi=150)
    plt.close()
    print(f"  Gráfico guardado: {ruta}")

def query_03() -> None:
    """
    Q03 — Categorías mejor valoradas con volumen significativo

    Pregunta ejecutiva: ¿Qué categorías de negocio mantienen
                        las mejores calificaciones promedio
                        con una cantidad relevante de reseñas?
    Fuentes: business + reviews
    Técnicas: EXPLODE(), JOIN, agregaciones, filtro por volumen
    """
    print("\n" + "=" * 60)
    print("Q03 — Categorías mejor valoradas con volumen significativo")
    print("=" * 60)

    df = spark.sql("""
        WITH categories_expanded AS (
            SELECT
                b.business_id,
                TRIM(cat) AS category
            FROM business b
            LATERAL VIEW explode(split(b.categories, ', ')) c AS cat
            WHERE b.categories IS NOT NULL
        ),
        review_base AS (
            SELECT
                c.category,
                r.stars
            FROM reviews r
            JOIN categories_expanded c
              ON r.business_id = c.business_id
        )
        SELECT
            category,
            COUNT(*) AS total_reviews,
            ROUND(AVG(stars), 2) AS avg_stars,
            ROUND(STDDEV_POP(stars), 2) AS stddev_stars
        FROM review_base
        GROUP BY category
        HAVING COUNT(*) >= 1000
        ORDER BY avg_stars DESC, total_reviews DESC
        LIMIT 15
    """)

    df.show(15, truncate=False)

    # =============================
    # GRÁFICO — Barra horizontal
    # =============================
    pdf = df.toPandas()

    plt.figure(figsize=(12, 7))

    plt.barh(
        pdf["category"],
        pdf["avg_stars"]
    )

    plt.gca().invert_yaxis()

    plt.title("Q03 — Top 15 categorías mejor valoradas con volumen significativo")
    plt.xlabel("Promedio de estrellas")
    plt.ylabel("Categoría")

    plt.grid(axis="x", linestyle="--", alpha=0.4)
    plt.tight_layout()

    ruta = os.path.join(GRAFICOS, "q03_categorias_mejor_valoradas.png")
    plt.savefig(ruta, dpi=150)
    plt.close()

    print(f"  Gráfico guardado: {ruta}")

def query_04() -> None:
    """
    Q04 — Precio vs. Rating Promedio por Categoría

    Pregunta ejecutiva: ¿Qué categorías tienen mejor rating según su nivel de precio?
    Fuentes:  business (portado desde la versión cloud — ahora usa business.stars directamente)
    Técnicas: LATERAL VIEW EXPLODE, acceso a struct anidado, HAVING, JOIN implícito en misma vista
    Gráfico:  Heatmap — categorías (Y) vs rango de precio (X), color = avg_rating
    """
    print("\n" + "=" * 60)
    print("Q04 — Precio vs. rating por categoría")
    print("=" * 60)

    df = spark.sql("""
        SELECT
            TRIM(cat.category)                         AS categoria,
            b.attributes.RestaurantsPriceRange2        AS rango_precio,
            ROUND(AVG(b.stars), 2)                     AS avg_rating,
            COUNT(*)                                   AS total_negocios
        FROM business b
        LATERAL VIEW EXPLODE(SPLIT(b.categories, ',')) cat AS category
        WHERE b.attributes.RestaurantsPriceRange2 IS NOT NULL
          AND b.is_open = 1
        GROUP BY TRIM(cat.category), b.attributes.RestaurantsPriceRange2
        HAVING COUNT(*) >= 30
        ORDER BY total_negocios DESC, categoria
    """)

    df.show(20, truncate=False)

    pdf = df.toPandas()
    pdf["rango_precio"] = pdf["rango_precio"].astype(int)

    pivot = pdf.pivot_table(
        index="categoria",
        columns="rango_precio",
        values="avg_rating",
        aggfunc="mean",
    )
    pivot = pivot.astype(float)

    top_cats = (
        pdf.groupby("categoria")["total_negocios"]
        .sum().nlargest(20).index
    )
    pivot = pivot.loc[pivot.index.isin(top_cats)].sort_index()

    fig, ax = plt.subplots(figsize=(8, 10))
    im = ax.imshow(pivot.values, aspect="auto", cmap="RdYlGn", vmin=3.0, vmax=4.5)

    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels([f"${c}" for c in pivot.columns], fontsize=10)
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index, fontsize=8)

    for i in range(len(pivot.index)):
        for j in range(len(pivot.columns)):
            val = pivot.values[i, j]
            if not math.isnan(val):
                ax.text(j, i, f"{val:.2f}", ha="center", va="center",
                        fontsize=7, color="black")

    plt.colorbar(im, ax=ax, label="Rating promedio")
    ax.set_title("Q04 — Rating promedio por categoría y rango de precio", pad=12)
    ax.set_xlabel("Rango de precio")
    ax.set_ylabel("Categoría")
    plt.tight_layout()

    ruta = os.path.join(GRAFICOS, "q04_precio_vs_rating.png")
    plt.savefig(ruta, dpi=150)
    plt.close()
    print(f"  Gráfico guardado: {ruta}")

def query_05() -> None:
    """
    Q05 — Ranking de Líderes de Mercado por Categoría (Top 3)

    Pregunta ejecutiva: ¿Quiénes son los referentes en cada nicho de negocio
                        según popularidad y calidad?
    Fuentes:  business + reviews
    Técnicas: EXPLODE(SPLIT(categories, ', ')) para expandir el string de categorías ·
              JOIN · DENSE_RANK() particionado por categoría · HAVING por volumen mínimo
    Gráfico:  Bar chart seaborn — total_reviews por categoría, hue = negocio
    """
    print("\n" + "=" * 60)
    print("Q05 — Ranking de líderes de mercado por categoría (Top 3)")
    print("=" * 60)

    df = spark.sql("""
        WITH ExpandedCategories AS (
            SELECT
                b.business_id,
                b.name,
                TRIM(cat.category_name) AS category_name
            FROM business b
            LATERAL VIEW EXPLODE(SPLIT(b.categories, ',')) cat AS category_name
            WHERE b.categories IS NOT NULL
        ),
        BusinessStats AS (
            SELECT
                ec.category_name,
                ec.name,
                COUNT(r.review_id) AS total_reviews,
                AVG(r.stars) AS avg_rating
            FROM ExpandedCategories ec
            JOIN reviews r ON ec.business_id = r.business_id
            GROUP BY ec.category_name, ec.name
            HAVING COUNT(r.review_id) >= 20
        ),
        RankedBusinesses AS (
            SELECT
                category_name,
                name,
                total_reviews,
                ROUND(avg_rating, 2) AS avg_rating,
                DENSE_RANK() OVER (
                    PARTITION BY category_name
                    ORDER BY total_reviews DESC, avg_rating DESC
                ) AS market_rank
            FROM BusinessStats
        )
        SELECT
            category_name,
            market_rank,
            name,
            total_reviews,
            avg_rating
        FROM RankedBusinesses
        WHERE market_rank <= 3
        ORDER BY category_name ASC, market_rank ASC
    """)

    df.show(30, truncate=False)

    pandas_leaders = df.limit(30).toPandas()

    plt.figure(figsize=(14, 8))
    sns.barplot(
        data=pandas_leaders,
        x="total_reviews",
        y="category_name",
        hue="name",
        dodge=True,
        errorbar=None,
    )
    plt.title("Top 3 Negocios por Categoría (Basado en Total de Reseñas)")
    plt.xlabel("Cantidad de Reseñas")
    plt.ylabel("Categoría")
    plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left", title="Negocios")
    plt.grid(axis="x", linestyle="--", alpha=0.6)
    plt.tight_layout()

    ruta = os.path.join(GRAFICOS, "q05_lideres_mercado_categoria.png")
    plt.savefig(ruta, dpi=150)
    plt.close()
    print(f"  Gráfico guardado: {ruta}")

def query_06() -> None:
    """
    Q06 — Usuarios más influyentes por impacto de reseñas

    Pregunta ejecutiva: ¿Qué usuarios tienen mayor influencia dentro de Yelp
                        según volumen, utilidad y diversidad de negocios reseñados?
    Fuentes: users + reviews + business
    Técnicas: JOINs, agregaciones, score compuesto
    """
    print("\n" + "=" * 60)
    print("Q06 — Usuarios más influyentes por impacto de reseñas")
    print("=" * 60)

    df = spark.sql("""
        WITH review_stats AS (
            SELECT
                r.user_id,
                COUNT(*) AS total_reviews_realizadas,
                COUNT(DISTINCT r.business_id) AS negocios_distintos,
                SUM(r.useful) AS total_useful,
                ROUND(AVG(r.stars), 2) AS avg_stars_given
            FROM reviews r
            GROUP BY r.user_id
        ),
        user_enriched AS (
            SELECT
                u.user_id,
                u.name,
                u.review_count AS review_count_profile,
                u.average_stars,
                COALESCE(rs.total_reviews_realizadas, 0) AS total_reviews_realizadas,
                COALESCE(rs.negocios_distintos, 0) AS negocios_distintos,
                COALESCE(rs.total_useful, 0) AS total_useful,
                COALESCE(rs.avg_stars_given, u.average_stars) AS avg_stars_given
            FROM users u
            LEFT JOIN review_stats rs
                ON u.user_id = rs.user_id
        )
        SELECT
            user_id,
            name,
            total_reviews_realizadas,
            negocios_distintos,
            total_useful,
            avg_stars_given,
            ROUND(
                total_reviews_realizadas * 0.4 +
                negocios_distintos * 0.3 +
                total_useful * 0.3, 2
            ) AS influence_score
        FROM user_enriched
        WHERE total_reviews_realizadas >= 50
        ORDER BY influence_score DESC
        LIMIT 20
    """)

    df.show(20, truncate=False)

    
    # GRÁFICO — Bar chart horizontal
  
    pdf = df.toPandas()

    plt.figure(figsize=(12, 7))

    plt.barh(
        pdf["name"],
        pdf["influence_score"]
    )

    plt.gca().invert_yaxis()  # para que el mayor quede arriba

    plt.title("Top 20 usuarios más influyentes en Yelp")
    plt.xlabel("Influence Score")
    plt.ylabel("Usuario")

    plt.grid(axis="x", linestyle="--", alpha=0.4)
    plt.tight_layout()

    ruta = os.path.join(GRAFICOS, "q06_usuarios_mas_influyentes.png")
    plt.savefig(ruta, dpi=150)
    plt.close()

    print(f"  Gráfico guardado: {ruta}")

def query_07() -> None:
    """
    Q07 — Impacto de la Pandemia y Recuperación por Estado (2018–2022)

    Pregunta ejecutiva: ¿Qué estados fueron más golpeados por el COVID
                        y cuáles se recuperaron más rápido?
    Fuentes:  reviews + business
    Técnicas: Window LAG(), JOIN, YEAR(CAST()), YoY % change
    Gráfico:  Multi-línea — año (X) vs volumen de reseñas (Y), top-5 estados
    """
    print("\n" + "=" * 60)
    print("Q07 — Impacto pandemia y recuperación por estado")
    print("=" * 60)

    df = spark.sql("""
        WITH yearly AS (
            SELECT b.state,
                   YEAR(CAST(r.date AS TIMESTAMP)) AS yr,
                   COUNT(*)                         AS reviews
            FROM reviews r
            JOIN business b ON r.business_id = b.business_id
            GROUP BY b.state, YEAR(CAST(r.date AS TIMESTAMP))
        ),
        with_lag AS (
            SELECT *,
                   LAG(reviews) OVER (PARTITION BY state ORDER BY yr) AS prev_reviews
            FROM yearly
        )
        SELECT state, yr, reviews,
               ROUND((reviews - prev_reviews) / prev_reviews * 100, 1) AS yoy_pct_change
        FROM with_lag
        ORDER BY state, yr
    """)

    df.show(50, truncate=False)

    # Gráfico: top-5 estados por volumen total
    pdf = df.toPandas()
    top_states = (
        pdf.groupby("state")["reviews"].sum()
        .nlargest(5).index.tolist()
    )
    pdf_top = pdf[pdf["state"].isin(top_states)]

    fig, ax = plt.subplots(figsize=(10, 6))
    for state in top_states:
        datos = pdf_top[pdf_top["state"] == state].sort_values("yr")
        ax.plot(datos["yr"], datos["reviews"], marker="o", label=state, linewidth=2)

    ax.set_title("Q07 — Volumen de reseñas por año y estado (top 5)", pad=12)
    ax.set_xlabel("Año")
    ax.set_ylabel("Número de reseñas")
    ax.set_xticks([2018, 2019, 2020, 2021, 2022])
    ax.axvspan(2019.9, 2021.5, alpha=0.08, color="red", label="COVID-19")
    ax.legend()
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    plt.tight_layout()

    ruta = os.path.join(GRAFICOS, "q07_pandemia_recuperacion_estado.png")
    plt.savefig(ruta, dpi=150)
    plt.close()
    print(f"  Gráfico guardado: {ruta}")

def query_08() -> None:
    """
    Q08 — Negocios con alto tráfico pero baja satisfacción

    Pregunta ejecutiva: ¿Qué negocios reciben mucho tráfico,
                        pero no convierten eso en satisfacción del cliente?
    Fuentes: business + checkins + reviews
    Técnicas: agregación, percentiles relativos con PERCENT_RANK()
    """
    print("\n" + "=" * 60)
    print("Q08 — Negocios con alto tráfico pero baja satisfacción")
    print("=" * 60)

    df = spark.sql("""
        WITH checkin_counts AS (
            SELECT business_id, COUNT(*) AS total_checkins
            FROM checkins
            GROUP BY business_id
        ),
        review_counts AS (
            SELECT business_id,
                   COUNT(*) AS total_reviews,
                   ROUND(AVG(stars), 2) AS avg_review_stars
            FROM reviews
            GROUP BY business_id
        ),
        combined AS (
            SELECT
                b.business_id,
                b.name,
                b.city,
                b.state,
                b.stars AS business_stars,
                b.review_count,
                COALESCE(ch.total_checkins, 0) AS total_checkins,
                COALESCE(r.total_reviews, 0) AS total_reviews_real,
                COALESCE(r.avg_review_stars, b.stars) AS avg_review_stars
            FROM business b
            LEFT JOIN checkin_counts ch ON b.business_id = ch.business_id
            LEFT JOIN review_counts r   ON b.business_id = r.business_id
        ),
        ranked AS (
            SELECT *,
               ROUND(PERCENT_RANK() OVER (PARTITION BY state ORDER BY total_checkins), 4) AS traffic_pct_rank,
               ROUND(PERCENT_RANK() OVER (PARTITION BY state ORDER BY avg_review_stars), 4) AS rating_pct_rank
            FROM combined
            WHERE total_checkins > 0
              AND total_reviews_real >= 30
        )
        SELECT
            name,
            city,
            state,
            total_checkins,
            total_reviews_real,
            avg_review_stars,
            traffic_pct_rank,
            rating_pct_rank
        FROM ranked
        WHERE traffic_pct_rank >= 0.90
          AND rating_pct_rank <= 0.25
        ORDER BY total_checkins DESC, avg_review_stars ASC
        LIMIT 30
    """)

    df.show(30, truncate=False)

    # =============================
    # GRÁFICO — Scatter Plot
    # =============================
    pdf = df.toPandas()

    plt.figure(figsize=(10, 6))

    plt.scatter(
        pdf["total_checkins"],
        pdf["avg_review_stars"],
        alpha=0.7
    )

    # Etiquetas de algunos puntos (top 10)
    for i, row in pdf.head(10).iterrows():
        plt.text(
            row["total_checkins"],
            row["avg_review_stars"],
            row["name"][:15],
            fontsize=8
        )

    plt.title("Negocios con alto tráfico pero baja satisfacción")
    plt.xlabel("Total de Check-ins (Tráfico)")
    plt.ylabel("Promedio de Estrellas (Satisfacción)")

    plt.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()

    ruta = os.path.join(GRAFICOS, "q08_alto_trafico_baja_satisfaccion.png")
    plt.savefig(ruta, dpi=150)
    plt.close()

    print(f"  Gráfico guardado: {ruta}")

def query_09() -> None:
    """
    Q09 — Análisis de Tendencias de Calidad y Brecha de Rendimiento Mensual

    Pregunta ejecutiva: ¿Qué negocios muestran las caídas más pronunciadas
                        en calidad respecto a su propia tendencia reciente?
    Fuentes:  reviews + business
    Técnicas: DATE_TRUNC, Window AVG rolling (ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),
              performance gap como diferencia entre mes actual y media móvil 3 meses
    Gráfico:  Bar chart horizontal — nombre + mes vs brecha de rendimiento
    """
    print("\n" + "=" * 60)
    print("Q09 — Tendencias de calidad y brecha de rendimiento mensual")
    print("=" * 60)

    df = spark.sql("""
        WITH ResenasMensuales AS (
            SELECT
                c.business_id,
                c.name AS name,
                DATE_TRUNC('month', r.date) AS review_month,
                AVG(r.stars) AS monthly_avg_stars,
                COUNT(r.review_id) AS total_reviews_month
            FROM reviews r
            JOIN business c ON r.business_id = c.business_id
            GROUP BY 1, 2, 3
        ),
        MediaMovil AS (
            SELECT
                business_id,
                name,
                review_month,
                monthly_avg_stars,
                total_reviews_month,
                AVG(monthly_avg_stars) OVER (
                    PARTITION BY business_id
                    ORDER BY review_month
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ) AS rolling_3m_avg
            FROM ResenasMensuales
        )
        SELECT
            name,
            review_month,
            ROUND(monthly_avg_stars, 2) AS current_month_rating,
            ROUND(rolling_3m_avg, 2) AS rolling_avg_rating,
            ROUND(monthly_avg_stars - rolling_3m_avg, 2) AS performance_gap
        FROM MediaMovil
        WHERE total_reviews_month >= 5
        ORDER BY performance_gap ASC
        LIMIT 15
    """)

    df.show(15, truncate=False)

    pandas_perf = df.withColumn(
        "review_month", F.col("review_month").cast("string")
    ).toPandas()
    pandas_perf["review_month"] = pd.to_datetime(pandas_perf["review_month"])

    plt.figure(figsize=(12, 6))
    plt.barh(
        pandas_perf["name"] + " (" + pandas_perf["review_month"].dt.strftime("%Y-%m") + ")",
        pandas_perf["performance_gap"],
        color="salmon",
    )
    plt.xlabel("Brecha de Rendimiento (Estrellas)")
    plt.title("Top 15 Mayores Caídas de Rendimiento Mensual vs Media Móvil")
    plt.gca().invert_yaxis()
    plt.grid(axis="x", linestyle="--", alpha=0.7)
    plt.tight_layout()

    ruta = os.path.join(GRAFICOS, "q09_brecha_rendimiento_mensual.png")
    plt.savefig(ruta, dpi=150)
    plt.close()
    print(f"  Gráfico guardado: {ruta}")



def query_10():
    """
    Q10 — Clustering de negocios (segmentación)

    Pregunta ejecutiva: ¿Existen segmentos naturales de negocios según popularidad,
                        calidad y tráfico? ¿En qué clúster compite cada negocio?
    Fuentes:  business + checkins
    Técnicas: K-Means (k=4), StandardScaler, SparkSQL para extracción de features,
              toPandas() para clustering con scikit-learn
    Gráfico:  Scatter plot — review_count (X) vs total_checkins (Y), color = clúster
    """
    print("\n" + "=" * 60)
    print("Q10 — Clustering (K-Means)")
    print("=" * 60)

    df = spark.sql("""
        WITH checkins_count AS (
            SELECT business_id,
                   COUNT(*) AS total_checkins
            FROM checkins
            GROUP BY business_id
        )
        SELECT
            b.name,
            b.stars,
            b.review_count,
            COALESCE(c.total_checkins, 0) AS total_checkins
        FROM business b
        LEFT JOIN checkins_count c
          ON b.business_id = c.business_id
        WHERE b.review_count IS NOT NULL
          AND b.stars IS NOT NULL
    """)

    pdf = df.toPandas()

    from sklearn.cluster import KMeans
    from sklearn.preprocessing import StandardScaler

    X = pdf[["stars", "review_count", "total_checkins"]]

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
    pdf["cluster"] = kmeans.fit_predict(X_scaled)

    print("\nCentroides de clusters:")
    print(kmeans.cluster_centers_)

    print("\nEjemplos por cluster:")
    print(pdf.groupby("cluster")[["stars", "review_count", "total_checkins"]].mean())

    plt.figure(figsize=(8, 6))

    scatter = plt.scatter(
        pdf["review_count"],
        pdf["total_checkins"],
        c=pdf["cluster"],
        cmap="viridis",
        alpha=0.6
    )

    plt.xlabel("Número de reseñas")
    plt.ylabel("Número de check-ins")
    plt.title("Segmentación de negocios (Clustering)")
    plt.colorbar(scatter, label="Cluster")
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()

    ruta = os.path.join(GRAFICOS, "q10_clustering.png")
    plt.savefig(ruta, dpi=200)
    plt.close()
    print(f"Gráfico guardado en: {ruta}")


# =============================================================================
# EJECUCIÓN DE CONSULTAS
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("  Análisis SparkSQL — Dataset Yelp")
    print("=" * 60)

    t_total = time.time()

    # cargar_vistas_local()
    cargar_vistas_clusters()

    for fn, nombre in [
        (query_01, "Q01"),
        (query_02, "Q02"),
        (query_03, "Q03"),
        (query_04, "Q04"),
        (query_05, "Q05"),
        (query_06, "Q06"),
        (query_07, "Q07"),
        (query_08, "Q08"),
        (query_09, "Q09"),
        (query_10, "Q10")
    ]:
        t = time.time()
        fn()
        print(f"  {nombre} completado en {time.time() - t:.1f}s")

    print(f"\nTiempo total: {time.time() - t_total:.1f}s")
    spark.stop()