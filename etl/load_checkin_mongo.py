"""
Carga checkin.json → MongoDB Atlas M0, colección `checkins`.

Fuente : data/filtered_trimmed/yelp_academic_dataset_checkin.json
Destino: MongoDB Atlas M0 #3 — colección checkins (95,871 documentos, ~68 MB)

El campo `date` se almacena tal como está — un string de timestamps separados
por comas (ej: "2018-03-01 22:12:01, 2019-06-15 14:30:00, ...").

Ejecución:
    uv run python etl/load_checkin_mongo.py

Variables de entorno requeridas (.env):
    ATLAS_URI_CHECKINS      — URI de conexión del clúster M0 para checkins
    ATLAS_CHECKINS_LOADED   — poner "true" para saltear la carga (default: false)
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

ATLAS_URI       = os.getenv("ATLAS_URI_CHECKINS")
ALREADY_LOADED  = os.getenv("ATLAS_CHECKINS_LOADED", "false").lower() == "true"

SOURCE     = "data/filtered_trimmed/yelp_academic_dataset_checkin.json"
DB_NAME    = "yelp"
COL_NAME   = "checkins"
BATCH_SIZE = 500

# =============================================================================
# GUARDIA — saltar si ya fue cargado
# =============================================================================

if ALREADY_LOADED:
    print("[SKIP] ATLAS_CHECKINS_LOADED=true — cambiar a false en .env para recargar")
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
col.create_index([("business_id", ASCENDING)])
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
print("\n[DONE] Actualizar ATLAS_CHECKINS_LOADED=true en .env")
