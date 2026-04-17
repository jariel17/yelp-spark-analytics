"""
Orquestador del pipeline ETL completo.

Ejecuta cada paso en orden, usa el mismo intérprete Python de la sesión
actual y aborta si algún paso falla con código de salida distinto de 0.
Las guardias ATLAS_*_LOADED / CRDB_*_LOADED en .env permiten saltear
cargas ya completadas sin modificar este script.

Orden de ejecución:
    1. pipeline_v3_alt  — recorte de campos, muestreo y filtrado temporal
    2. load_business_mongo
    3. load_checkin_mongo
    4. load_tip_mongo
    5. load_user_crdb
    6. load_review_aiven

Ejecución:
    uv run python etl/run_pipeline.py
"""
import subprocess
import sys
import time
from pathlib import Path

# =============================================================================
# PASOS DEL PIPELINE (en orden)
# =============================================================================

ETL_DIR = Path(__file__).parent

STEPS = [
    ETL_DIR / "pipeline.py",
    ETL_DIR / "load_business_mongo.py",
    ETL_DIR / "load_checkin_mongo.py",
    ETL_DIR / "load_tip_mongo.py",
    ETL_DIR / "load_user_crdb.py",
    ETL_DIR / "load_review_aiven.py",
]

# =============================================================================
# RUNNER
# =============================================================================

def run_step(script: Path, step_num: int, total: int) -> None:
    """Ejecuta un script y aborta el pipeline si devuelve código de error."""
    print(f"\n{'=' * 62}")
    print(f"  [{step_num}/{total}] {script.name}")
    print(f"{'=' * 62}")

    t0     = time.time()
    result = subprocess.run([sys.executable, str(script)])
    elapsed = time.time() - t0

    if result.returncode != 0:
        print(f"\n[ERROR] {script.name} terminó con código {result.returncode} "
              f"({elapsed:.1f}s) — pipeline abortado.")
        sys.exit(result.returncode)

    print(f"\n[OK] {script.name} completado en {elapsed:.1f}s")


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    t_total = time.time()
    total   = len(STEPS)

    print("=" * 62)
    print("  Pipeline ETL — Yelp Dataset")
    print(f"  {total} pasos")
    print("=" * 62)

    for i, script in enumerate(STEPS, start=1):
        run_step(script, i, total)

    print(f"\n{'=' * 62}")
    print(f"  Pipeline completado en {time.time() - t_total:.1f}s")
    print(f"{'=' * 62}")
