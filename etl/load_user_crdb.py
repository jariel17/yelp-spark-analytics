"""
Carga user.json → CockroachDB Serverless, tabla `users`.

Fuente : data/filtered_trimmed/yelp_academic_dataset_user.json
Destino: CockroachDB — tabla users

Ejecución:
    uv run python etl/load_user_crdb.py

Variables de entorno requeridas (.env):
    CRDB_CONNECTION       — cadena de conexión PostgreSQL de CockroachDB
    CRDB_USERS_LOADED     — poner "true" para saltear la carga (default: false)

Certificado SSL:
    CockroachDB requiere ~/.postgresql/root.crt
    Descargar desde: CockroachDB Cloud Console → Connect → Download CA Cert
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

CRDB_CONNECTION  = os.getenv("CRDB_CONNECTION")
ALREADY_LOADED   = os.getenv("CRDB_USERS_LOADED", "false").lower() == "true"

SOURCE     = "data/filtered_trimmed/yelp_academic_dataset_user.json"
BATCH_SIZE = 500

# =============================================================================
# GUARDIA — saltar si ya fue cargado
# =============================================================================

if ALREADY_LOADED:
    print("[SKIP] CRDB_USERS_LOADED=true — cambiar a false en .env para recargar")
    raise SystemExit(0)

# =============================================================================
# CONEXIÓN
# =============================================================================

parsed = urlparse(CRDB_CONNECTION)
conn = psycopg2.connect(
    host     = parsed.hostname,
    port     = parsed.port,
    dbname   = parsed.path.lstrip("/"),
    user     = parsed.username,
    password = parsed.password,
    sslmode  = "verify-full",
    sslrootcert = os.path.expanduser("~/.postgresql/root.crt"),
)
conn.autocommit = False
cur = conn.cursor()
print(f"[OK] conectado a CockroachDB  ({parsed.hostname})")

# =============================================================================
# DDL — crear tabla si no existe
# =============================================================================

cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        user_id       TEXT PRIMARY KEY,
        name          TEXT,
        review_count  INT,
        yelping_since TEXT,
        useful        INT,
        funny         INT,
        cool          INT,
        elite         TEXT,
        fans          INT,
        average_stars REAL
    )
""")
conn.commit()
print("[OK] tabla users lista")

# =============================================================================
# CARGA — streaming line-by-line, inserts en batches de 500
# =============================================================================

print(f"\nCargando {SOURCE} ...")
t0      = time.time()
batch   = []
total   = 0
COLS    = ("user_id","name","review_count","yelping_since",
           "useful","funny","cool","elite","fans","average_stars")
SQL     = f"INSERT INTO users ({','.join(COLS)}) VALUES %s ON CONFLICT DO NOTHING"

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

        if total % 100_000 == 0:
            print(f"  {total:,} registros  ({time.time()-t0:.0f}s)")

if batch:
    flush(batch)

# =============================================================================
# RESUMEN
# =============================================================================

cur.execute("SELECT COUNT(*) FROM users")
n_db = cur.fetchone()[0]
cur.close()
conn.close()

print(f"\n{'='*50}")
print(f"  Registros leídos  : {total:,}")
print(f"  Registros en tabla: {n_db:,}")
print(f"  Tiempo total       : {time.time()-t0:.1f}s")
print(f"{'='*50}")
print("\n[DONE] Actualizar CRDB_USERS_LOADED=true en .env")
