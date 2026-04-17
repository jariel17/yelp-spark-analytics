"""
Carga tip.json → MongoDB Atlas M0, colección `tips`.

Fuente : data/filtered_trimmed/yelp_academic_dataset_tip.json
Destino: MongoDB Atlas M0 #2 — colección tips (77,744 documentos, ~15 MB)

Ejecución:
    uv run python etl/load_tip_mongo.py

Variables de entorno requeridas (.env):
    ATLAS_URI_TIPS      — URI de conexión del clúster M0 para tips
    ATLAS_TIPS_LOADED   — poner "true" para saltear la carga (default: false)
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

ATLAS_URI       = os.getenv("ATLAS_URI_TIPS")
ALREADY_LOADED  = os.getenv("ATLAS_TIPS_LOADED", "false").lower() == "true"

SOURCE     = "data/filtered_trimmed/yelp_academic_dataset_tip.json"
DB_NAME    = "yelp"
COL_NAME   = "tips"
BATCH_SIZE = 500

# =============================================================================
# GUARDIA — saltar si ya fue cargado
# =============================================================================

if ALREADY_LOADED:
    print("[SKIP] ATLAS_TIPS_LOADED=true — cambiar a false en .env para recargar")
    raise SystemExit(0)

# =============================================================================
# CONEXIÓN
# =============================================================================

client = MongoClient(ATLAS_URI)
db     = client[DB_NAME]
col    = db[COL_NAME]
print(f"[OK] conectado a Atlas — base: {DB_NAME}, colección: {COL_NAME}")

# =============================================================================
# SETUP — limpiar colección anterior e índices
# =============================================================================

col.drop()
col.create_index([("business_id", ASCENDING)])
col.create_index([("user_id", ASCENDING)])
print("[OK] colección limpia, índices en business_id y user_id creados")

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
        obj.pop("_id", None)
        batch.append(obj)
        total += 1

        if len(batch) == BATCH_SIZE:
            col.insert_many(batch, ordered=False)
            batch = []

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
print("\n[DONE] Actualizar ATLAS_TIPS_LOADED=true en .env")
