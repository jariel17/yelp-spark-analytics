"""
Carga business.json → MongoDB Atlas M0, colección `business`.

Fuente : data/raw_trimmed/yelp_academic_dataset_business.json
Destino: MongoDB Atlas M0 #1 — colección business (150,346 documentos, ~110 MB)

Se usa raw_trimmed/ (no filtered_trimmed/) porque business no tiene user_id
ni campo de fecha — no aplica cascade ni corte temporal. Ya tiene eliminados
address, postal_code, latitude y longitude (pipeline).

Los campos anidados attributes y hours se almacenan como subdocumentos
nativos de MongoDB.

Ejecución:
    uv run python etl/load_business_mongo.py

Variables de entorno requeridas (.env):
    ATLAS_URI_BUSINESS      — URI de conexión del clúster M0 para business
    ATLAS_BUSINESS_LOADED   — poner "true" para saltear la carga (default: false)
"""
import json
import os
import time

from pymongo import MongoClient, ASCENDING
from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

ATLAS_URI        = os.getenv("ATLAS_URI_BUSINESS")
ALREADY_LOADED   = os.getenv("ATLAS_BUSINESS_LOADED", "false").lower() == "true"

SOURCE     = "data/raw_trimmed/yelp_academic_dataset_business.json"
DB_NAME    = "yelp"
COL_NAME   = "business"
BATCH_SIZE = 500

# =============================================================================
# GUARDIA — saltar si ya fue cargado
# =============================================================================

if ALREADY_LOADED:
    print("[SKIP] ATLAS_BUSINESS_LOADED=true — cambiar a false en .env para recargar")
    raise SystemExit(0)

# =============================================================================
# CONEXIÓN
# =============================================================================

client = MongoClient(ATLAS_URI)
db     = client[DB_NAME]
col    = db[COL_NAME]
print(f"[OK] conectado a Atlas — base: {DB_NAME}, colección: {COL_NAME}")

# =============================================================================
# SETUP — limpiar colección anterior e índice
# =============================================================================

col.drop()
col.create_index([("business_id", ASCENDING)], unique=True)
print("[OK] colección limpia, índice en business_id creado")

# =============================================================================
# CARGA — streaming line-by-line, insert_many en batches de 500
# =============================================================================

print(f"\nCargando {SOURCE} ...")
t0     = time.time()
batch  = []
total  = 0

with open(SOURCE) as f:
    for line in f:
        obj = json.loads(line)
        obj.pop("_id", None)          # evitar conflicto si existiera un _id previo
        batch.append(obj)
        total += 1

        if len(batch) == BATCH_SIZE:
            col.insert_many(batch, ordered=False)
            batch = []

        if total % 25_000 == 0:
            print(f"  {total:,} documentos  ({time.time()-t0:.0f}s)")

if batch:
    col.insert_many(batch, ordered=False)

# =============================================================================
# RESUMEN
# =============================================================================

n_db = col.count_documents({})
client.close()

print(f"\n{'='*50}")
print(f"  Documentos leídos    : {total:,}")
print(f"  Documentos en Atlas  : {n_db:,}")
print(f"  Tiempo total         : {time.time()-t0:.1f}s")
print(f"{'='*50}")
print("\n[DONE] Actualizar ATLAS_BUSINESS_LOADED=true en .env")
