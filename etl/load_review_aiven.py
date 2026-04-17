"""
Carga review.json → Aiven PostgreSQL, tabla `reviews`.

Fuente : data/filtered_trimmed/yelp_academic_dataset_review.json
Destino: Aiven PostgreSQL — tabla reviews (238,415 registros, ~171 MB)

Ejecución:
    uv run python etl/load_review_aiven.py

Variables de entorno requeridas (.env):
    AIVEN_CONNECTION        — cadena de conexión PostgreSQL de Aiven
                              formato: postgresql://avnadmin:<pwd>@host:port/defaultdb?sslmode=require
    AIVEN_SSL_CERT          — (opcional) ruta al CA cert descargado desde la consola Aiven
                              si se define, usa sslmode=verify-ca; si no, usa sslmode=require
    AIVEN_REVIEWS_LOADED    — poner "true" para saltear la carga (default: false)
"""
import json
import os
import time
from urllib.parse import urlparse

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

AIVEN_CONNECTION  = os.getenv("AIVEN_CONNECTION")
AIVEN_SSL_CERT    = os.getenv("AIVEN_SSL_CERT", "")
ALREADY_LOADED    = os.getenv("AIVEN_REVIEWS_LOADED", "false").lower() == "true"

SOURCE     = "data/filtered_trimmed/yelp_academic_dataset_review.json"
BATCH_SIZE = 500

# =============================================================================
# GUARDIA — saltar si ya fue cargado
# =============================================================================

if ALREADY_LOADED:
    print("[SKIP] AIVEN_REVIEWS_LOADED=true — cambiar a false en .env para recargar")
    raise SystemExit(0)

# =============================================================================
# CONEXIÓN
# =============================================================================

parsed = urlparse(AIVEN_CONNECTION)

ssl_kwargs = {}
if AIVEN_SSL_CERT:
    ssl_kwargs["sslmode"]     = "verify-ca"
    ssl_kwargs["sslrootcert"] = os.path.expanduser(AIVEN_SSL_CERT)
else:
    ssl_kwargs["sslmode"]     = "require"
    ssl_kwargs["sslrootcert"] = "disable"   # evita que libpq verifique el CA del sistema

conn = psycopg2.connect(
    host     = parsed.hostname,
    port     = parsed.port,
    dbname   = parsed.path.lstrip("/"),
    user     = parsed.username,
    password = parsed.password,
    **ssl_kwargs,
)
conn.autocommit = False
cur = conn.cursor()
print(f"[OK] conectado a Aiven PostgreSQL  ({parsed.hostname})")

# =============================================================================
# DDL — crear tabla si no existe
# =============================================================================

cur.execute("""
    CREATE TABLE IF NOT EXISTS reviews (
        review_id   TEXT PRIMARY KEY,
        user_id     TEXT,
        business_id TEXT,
        stars       REAL,
        useful      INT,
        funny       INT,
        cool        INT,
        text        TEXT,
        date        TEXT
    )
""")
conn.commit()
print("[OK] tabla reviews lista")

# =============================================================================
# CARGA — streaming line-by-line, inserts en batches de 500
# =============================================================================

print(f"\nCargando {SOURCE} ...")
t0      = time.time()
batch   = []
total   = 0
COLS    = ("review_id","user_id","business_id","stars","useful","funny","cool","text","date")
SQL     = f"INSERT INTO reviews ({','.join(COLS)}) VALUES %s ON CONFLICT DO NOTHING"

def flush(batch):
    execute_values(cur, SQL, batch)
    conn.commit()

with open(SOURCE) as f:
    for line in f:
        obj = json.loads(line)
        batch.append(tuple(obj.get(c) for c in COLS))
        total += 1

        if len(batch) == BATCH_SIZE:
            flush(batch)
            batch = []

        if total % 50_000 == 0:
            print(f"  {total:,} registros  ({time.time()-t0:.0f}s)")

if batch:
    flush(batch)

# =============================================================================
# RESUMEN
# =============================================================================

cur.execute("SELECT COUNT(*) FROM reviews")
n_db = cur.fetchone()[0]
cur.close()
conn.close()

print(f"\n{'='*50}")
print(f"  Registros leídos  : {total:,}")
print(f"  Registros en tabla: {n_db:,}")
print(f"  Tiempo total       : {time.time()-t0:.1f}s")
print(f"{'='*50}")
print("\n[DONE] Actualizar AIVEN_REVIEWS_LOADED=true en .env")
