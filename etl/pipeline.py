"""
Pipeline de Extracción y Transformación


Ejecución:
    uv run python etl/pipeline.py
"""
import json
import os
import random
import time

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

RAW_DIR = "/home/tux/big-data-project/data/raw"
TRIMMED_DIR = "/home/tux/big-data-project/data/raw_trimmed"
FILTERED_TRIMMED_DIR = "/home/tux/big-data-project/data/filtered_trimmed"

YEAR_MIN = 2018
YEAR_MAX = 2021

USER_SAMPLE_FRACTION = 0.40
REVIEW_SAMPLE_FRACTION = 0.20
SAMPLE_SEED = 42

USER_DROP = {"friends"}
USER_DROP_PREFIX = "compliment_"
BUSINESS_DROP = {"address", "postal_code", "latitude", "longitude"}

TRIM_FILES = [
    "yelp_academic_dataset_user.json",
    "yelp_academic_dataset_business.json",
]

FINAL_FILES = [
    "yelp_academic_dataset_user.json",
    "yelp_academic_dataset_review.json",
    "yelp_academic_dataset_tip.json",
    "yelp_academic_dataset_checkin.json",
    "yelp_academic_dataset_business.json",
]

# =============================================================================
# FUNCIONES AUXILIARES
# =============================================================================

def in_range(date_str: str, year_min: int, year_max: int) -> bool:
    """True si los primeros 4 caracteres de date_str pertenece a un rango de años"""
    if not date_str or len(date_str) < 4 or not date_str[:4].isdigit():
        return False
    esta_en_rango = year_min <= int(date_str[:4]) <= year_max
    return esta_en_rango


def file_mb(path: str) -> float:
    """Tamaño del archivo en MB."""
    megabytes = os.path.getsize(path) / 1e6
    return megabytes


def count_lines(path: str) -> int:
    """Número de líneas en un archivo JSON (= número de registros)."""
    with open(path) as file_input:
        total_lineas = sum(1 for _ in file_input)
    return total_lineas


def print_file_table(paths: list[str], label: str) -> None:
    """Imprime tabla resumen: archivo | registros | MB para cada path."""
    print(f"\n{'=' * 62}")
    print(f"  {label}")
    print(f"  {'Archivo':<30} {'Registros':>12}  {'Tamaño':>8}")
    print(f"  {'-'*30} {'-'*12}  {'-'*8}")
    total_mb = 0
    total_n = 0
    for path in paths:
        if not os.path.exists(path):
            continue
        mb = file_mb(path)
        n = count_lines(path)
        nombre = (os.path.basename(path)
                  .replace("yelp_academic_dataset_", "")
                  .replace(".json", ""))
        total_mb += mb
        total_n += n
        print(f"  {nombre:<30} {n:>12,}  {mb:>7.1f} MB")
    print(f"  {'-'*30} {'-'*12}  {'-'*8}")
    print(f"  {'TOTAL':<30} {total_n:>12,}  {total_mb:>7.1f} MB")
    print(f"{'=' * 62}")


# =============================================================================
# OPERACIONES
# =============================================================================

def trim_fields(
    source: str,
    destination: str,
    drop: set[str],
    drop_prefix: str = "",
) -> int:
    """
    Reduce el peso de un archivo JSON eliminando campos innecesarios antes de procesarlo.
    Soporta eliminación por nombre exacto y por prefijo, recorriendo el archivo línea a línea.
    """
    total = 0
    # Recorre el archivo de origen línea a línea (un JSON por línea) y escribe
    # cada registro ya limpio en el destino, sin cargar el archivo completo en memoria
    with open(source) as file_input, open(destination, "w") as file_output:
        for line in file_input:
            registro = json.loads(line)
            # Elimina campos exactos (ej. "friends")
            for field in drop:
                registro.pop(field, None)
            # Elimina campos por prefijo (ej. todos los "compliment_*")
            if drop_prefix:
                registro = {k: v for k, v in registro.items()
                            if not k.startswith(drop_prefix)}
            file_output.write(json.dumps(registro, ensure_ascii=False) + "\n")
            total += 1
    return total


def sample_users(
    source: str,
    destination: str,
    fraction: float,
    seed: int,
) -> set[str]:
    """
    Selecciona un subconjunto reproducible de usuarios para reducir el volumen del dataset.
    Usa muestreo aleatorio con semilla fija y retorna los user_ids elegidos, que luego
    sirven como filtro de integridad referencial para reviews y tips.
    """
    # Semilla fija para que el muestreo sea reproducible entre ejecuciones
    generador = random.Random(seed)
    user_ids: set[str] = set()
    leidos = escritos = 0

    # Recorre todos los usuarios y escribe solo los seleccionados al destino,
    # acumulando sus IDs en memoria para usarlos como filtro en cascade_filter
    with open(source) as file_input, open(destination, "w") as file_output:
        for line in file_input:
            leidos += 1
            # generador.random() produce un float uniforme en [0, 1); se incluye
            # el registro si cae por debajo del umbral (fracción deseada)
            if generador.random() < fraction:
                file_output.write(line)
                user_ids.add(json.loads(line)["user_id"])
                escritos += 1

    mb_origen = file_mb(source)
    mb_destino = file_mb(destination)
    print(f"[OK] user: {leidos:,} -> {escritos:,} ({int(fraction*100)}%)  "
          f"{mb_origen:.1f} MB -> {mb_destino:.1f} MB")
    print(f"     user_ids en memoria: {len(user_ids):,}")
    return user_ids


def cascade_filter(
    source: str,
    destination: str,
    user_ids: set[str],
    progress_every: int = 0,  # si > 0, imprime una línea de progreso cada N registros leídos
) -> tuple[int, int]:
    """
    Garantiza integridad referencial entre colecciones: solo pasan los registros
    cuyo user_id existe en el subconjunto de usuarios ya muestreado. 
    """
    leidos = escritos = 0
    inicio = time.time()

    # Recorre el archivo fuente completo y copia al destino solo los registros
    # que pasan el filtro de user_id, descartando el resto
    with open(source) as file_input, open(destination, "w") as file_output:
        for line in file_input:
            leidos += 1
            # Solo se conservan registros cuyo user_id pertenece al subconjunto muestreado
            if json.loads(line).get("user_id") in user_ids:
                file_output.write(line)
                escritos += 1
            # Progreso opcional para archivos grandes (ej. reviews con 7M+ líneas)
            if progress_every and leidos % progress_every == 0:
                print(f"  {leidos/1e6:.0f}M leídas / {escritos:,} escritas  "
                      f"({time.time() - inicio:.0f}s)")

    # Reporte final: muestra registros leídos vs escritos, tamaño resultante y tiempo total
    mb_destino = file_mb(destination)
    nombre = (os.path.basename(source)
              .replace("yelp_academic_dataset_", "").replace(".json", ""))
    print(f"[OK] {nombre}: {leidos:,} -> {escritos:,}  "
          f"{mb_destino:.1f} MB  ({time.time()-inicio:.1f}s)")
    return leidos, escritos


def filter_date_inplace(
    path: str,
    field: str,
    year_min: int,
    year_max: int,
) -> tuple[int, int]:
    """
    Recorta un archivo JSON al período de interés sin duplicar el archivo de origen.
    Escribe en un temporal y hace un replace atómico al final para evitar
    dejar el archivo en estado inconsistente si el proceso es interrumpido.
    """
    # Se escribe en un archivo temporal para no corromper el original si el proceso falla
    ruta_temporal = path + ".tmp"
    leidos = escritos = 0
    inicio = time.time()

    # Lee el archivo original y escribe en el temporal solo los registros
    # cuya fecha cae dentro del período definido
    with open(path) as file_input, open(ruta_temporal, "w") as file_output:
        for line in file_input:
            leidos += 1
            # in_range extrae el año del campo y verifica que esté dentro del rango
            if in_range(json.loads(line).get(field, ""), year_min, year_max):
                file_output.write(line)
                escritos += 1

    # Reemplaza el original con el filtrado — os.replace es atómico en la mayoría de SO
    os.replace(ruta_temporal, path)
    # Reporte final: muestra registros leídos vs escritos, tamaño resultante y tiempo total
    mb = file_mb(path)
    nombre = (os.path.basename(path)
              .replace("yelp_academic_dataset_", "").replace(".json", ""))
    print(f"[OK] {nombre}: {leidos:,} -> {escritos:,}  "
          f"{mb:.1f} MB  ({time.time()-inicio:.1f}s)")
    return leidos, escritos


def filter_checkins(
    source: str,
    destination: str,
    year_min: int,
    year_max: int,
) -> tuple[int, int]:
    """
    Filtra checkins al período de interés respetando su formato especial: el campo
    date contiene múltiples timestamps en un solo string separado por comas, por lo
    que el filtrado opera timestamp a timestamp dentro de cada registro, no por registro.
    """
    leidos = escritos = 0
    inicio = time.time()

    # Recorre cada negocio y filtra su lista de timestamps individualmente,
    # descartando el registro completo solo si ningún timestamp queda dentro del rango
    with open(source) as file_input, open(destination, "w") as file_output:
        for line in file_input:
            leidos += 1
            registro = json.loads(line)
            # El campo "date" es un string con timestamps separados por coma
            timestamps_validos = [
                d for d in (s.strip() for s in registro.get("date", "").split(","))
                if in_range(d, year_min, year_max)
            ]
            if timestamps_validos:
                registro["date"] = ", ".join(timestamps_validos)
                file_output.write(json.dumps(registro, ensure_ascii=False) + "\n")
                escritos += 1

    # Reporte final: muestra registros leídos vs escritos, tamaño resultante y tiempo total
    mb = file_mb(destination)
    print(f"[OK] checkin: {leidos:,} -> {escritos:,}  "
          f"{mb:.1f} MB  ({time.time()-inicio:.1f}s)")
    return leidos, escritos


def sample_inplace(
    path: str,
    fraction: float,
    seed: int,
) -> tuple[int, int]:
    """
    Reduce el volumen de un archivo ya filtrado mediante muestreo aleatorio reproducible.
    Opera in-place con el mismo patrón de archivo temporal que filter_date_inplace.
    """
    # Mismo patrón que filter_date_inplace: escribir en temporal antes de reemplazar
    ruta_temporal = path + ".tmp"
    generador = random.Random(seed)
    leidos = escritos = 0
    inicio = time.time()

    # Lee el archivo ya filtrado y escribe en el temporal solo la fracción seleccionada,
    # reduciendo el volumen antes de la carga a la nube
    with open(path) as file_input, open(ruta_temporal, "w") as file_output:
        for line in file_input:
            leidos += 1
            # generador.random() produce un float uniforme en [0, 1); se incluye
            # el registro si cae por debajo del umbral (fracción deseada)
            if generador.random() < fraction:
                file_output.write(line)
                escritos += 1

    os.replace(ruta_temporal, path)
    # Reporte final: muestra registros leídos vs escritos, tamaño resultante y tiempo total
    mb = file_mb(path)
    nombre = (os.path.basename(path)
              .replace("yelp_academic_dataset_", "").replace(".json", ""))
    print(f"[OK] {nombre}: {leidos:,} -> {escritos:,} ({int(fraction*100)}%)  "
          f"{mb:.1f} MB  ({time.time()-inicio:.1f}s)")
    return leidos, escritos


# =============================================================================
# PIPELINE
# =============================================================================

if __name__ == "__main__":
    os.makedirs(TRIMMED_DIR, exist_ok=True)
    os.makedirs(FILTERED_TRIMMED_DIR, exist_ok=True)

    # --- 1. Recorte de campos: user.json -------
    # Se eliminan: "friends" (lista masiva sin uso analítico) y todos los "compliment_*"
    print("\n=== [1] user.json — drop friends + compliment_* ===")
    inicio = time.time()
    source_user = os.path.join(RAW_DIR, "yelp_academic_dataset_user.json")
    destination_user = os.path.join(TRIMMED_DIR, "yelp_academic_dataset_user.json")

    total = trim_fields(source_user, destination_user, drop=USER_DROP, drop_prefix=USER_DROP_PREFIX)

    print(f"[OK] user: {total:,} registros  "
          f"{file_mb(source_user):.1f} MB -> {file_mb(destination_user):.1f} MB  "
          f"(-{(1 - file_mb(destination_user)/file_mb(source_user))*100:.0f}%)  "
          f"({time.time()-inicio:.1f}s)")

    # --- 2. Recorte de campos: business.json -------
    # Se eliminan: "address", "postal_code", "latitude", "longitude" (datos de ubicación exacta, irrelevantes para el análisis)
    print("\n=== [2] business.json — drop address, postal_code, latitude, longitude ===")
    inicio = time.time()
    source_business = os.path.join(RAW_DIR, "yelp_academic_dataset_business.json")
    destination_business = os.path.join(TRIMMED_DIR, "yelp_academic_dataset_business.json")

    total = trim_fields(source_business, destination_business, drop=BUSINESS_DROP)

    print(f"[OK] business: {total:,} registros  "
          f"{file_mb(source_business):.1f} MB -> {file_mb(destination_business):.1f} MB  "
          f"(-{(1 - file_mb(destination_business)/file_mb(source_business))*100:.0f}%)  "
          f"({time.time()-inicio:.1f}s)")

    # Reporte: comparativa RAW vs TRIMMED
    print(f"\n{'=' * 58}")
    print(f"  {'Archivo':<30} {'RAW':>9}  {'TRIMMED':>9}  {'Δ':>6}")
    print(f"  {'-'*30} {'-'*9}  {'-'*9}  {'-'*6}")
    total_mb_origen = total_mb_destino = 0

    # Itera sobre los archivos recortados para comparar su tamaño antes y después del trim,
    # acumulando los totales para mostrar el ahorro global al final de la tabla
    for nombre_archivo in TRIM_FILES:
        mb_origen = file_mb(os.path.join(RAW_DIR, nombre_archivo))
        mb_destino = file_mb(os.path.join(TRIMMED_DIR, nombre_archivo))
        total_mb_origen += mb_origen
        total_mb_destino += mb_destino
        nombre = nombre_archivo.replace("yelp_academic_dataset_", "").replace(".json", "")
        print(f"  {nombre:<30} {mb_origen:>8.1f}  {mb_destino:>8.1f}  "
              f"{-(1 - mb_destino/mb_origen)*100:>5.0f}%")

    print(f"  {'-'*30} {'-'*9}  {'-'*9}  {'-'*6}")
    print(f"  {'TOTAL':<30} {total_mb_origen:>8.1f}  {total_mb_destino:>8.1f}  "
          f"{-(1 - total_mb_destino/total_mb_origen)*100:>5.0f}%")
    print(f"{'=' * 58}")

    # --- 3. Muestreo de usuarios --------------
    # Se toma el archivo ya recortado (raw_trimmed) como fuente, no el raw original,
    # para que los usuarios muestreados ya vengan sin los campos eliminados en el paso 1.
    # Los user_ids retornados se guardan en memoria y se usan en los pasos 4 y 5.
    # USER_SAMPLE_FRACTION indica la cantidad a conservar (40%)
    print(f"\n=== [3] muestreo user {int(USER_SAMPLE_FRACTION*100)}% "
          f"(seed={SAMPLE_SEED}) — fuente: raw_trimmed ===")
    inicio = time.time()
    user_ids = sample_users(
        source=os.path.join(TRIMMED_DIR, "yelp_academic_dataset_user.json"),
        destination=os.path.join(FILTERED_TRIMMED_DIR, "yelp_academic_dataset_user.json"),
        fraction=USER_SAMPLE_FRACTION,
        seed=SAMPLE_SEED,
    )
    print(f"  ({time.time()-inicio:.1f}s)")

    # --- 4. Filtro en cascada: reviews --------------
    # Se lee desde raw (no trimmed) porque reviews no tiene campos que recortar.
    # Se activa progress_every dado que el archivo completo supera los 7M de registros.
    print("\n=== [4] cascade review — fuente: raw ===")
    cascade_filter(
        source=os.path.join(RAW_DIR, "yelp_academic_dataset_review.json"),
        destination=os.path.join(FILTERED_TRIMMED_DIR, "yelp_academic_dataset_review.json"),
        user_ids=user_ids,
        progress_every=1_000_000,
    )

    # --- 5. Filtro en cascada: tips -----------------
    # Mismo criterio que reviews: fuente raw y filtro por los user_ids del paso 3.
    # No requiere progress_every porque tips es significativamente más pequeño.
    print("\n=== [5] cascade tip — fuente: raw ===")
    cascade_filter(
        source=os.path.join(RAW_DIR, "yelp_academic_dataset_tip.json"),
        destination=os.path.join(FILTERED_TRIMMED_DIR, "yelp_academic_dataset_tip.json"),
        user_ids=user_ids,
    )

    # Reporte: post-cascade (antes del filtrado temporal)
    print_file_table(
        paths=[os.path.join(FILTERED_TRIMMED_DIR, f) for f in [
            "yelp_academic_dataset_user.json",
            "yelp_academic_dataset_review.json",
            "yelp_academic_dataset_tip.json",
        ]],
        label="Post-cascade (antes del filtrado temporal)",
    )

    # --- 6. Filtrado temporal: reviews --------------
    print(f"\n=== [6] review — filtrar {YEAR_MIN}–{YEAR_MAX} ===")
    filter_date_inplace(
        path=os.path.join(FILTERED_TRIMMED_DIR, "yelp_academic_dataset_review.json"),
        field="date", year_min=YEAR_MIN, year_max=YEAR_MAX,
    )

    # --- 7. Filtrado temporal: tips -----------------
    print(f"\n=== [7] tip — filtrar {YEAR_MIN}–{YEAR_MAX} ===")
    filter_date_inplace(
        path=os.path.join(FILTERED_TRIMMED_DIR, "yelp_academic_dataset_tip.json"),
        field="date", year_min=YEAR_MIN, year_max=YEAR_MAX,
    )

    # --- 8. Filtrado temporal: checkins -------------
    print(f"\n=== [8] checkin — filtrar {YEAR_MIN}–{YEAR_MAX} desde raw ===")
    filter_checkins(
        source=os.path.join(RAW_DIR, "yelp_academic_dataset_checkin.json"),
        destination=os.path.join(FILTERED_TRIMMED_DIR, "yelp_academic_dataset_checkin.json"),
        year_min=YEAR_MIN, year_max=YEAR_MAX,
    )

    # Reporte: dataset completo antes del muestreo de reviews
    print_file_table(
        paths=[os.path.join(FILTERED_TRIMMED_DIR, f) for f in FINAL_FILES],
        label="filtered_trimmed completo",
    )

    # --- 9. Muestreo de reviews ---------------
    # Aun después del filtrado temporal y la cascada, reviews sigue siendo el archivo
    # más grande. Se aplica un muestreo adicional in-place para llevarlo a un volumen
    # manejable para la carga a la nube y los análisis locales con Spark.
    # REVIEW_SAMPLE_FRACTION indica el porcentaje a conservar (20%)
    print(f"\n=== [9] muestreo review {int(REVIEW_SAMPLE_FRACTION*100)}% "
          f"(seed={SAMPLE_SEED}) — in-place en filtered_trimmed ===")
    sample_inplace(
        path=os.path.join(FILTERED_TRIMMED_DIR, "yelp_academic_dataset_review.json"),
        fraction=REVIEW_SAMPLE_FRACTION,
        seed=SAMPLE_SEED,
    )

    # Reporte: dataset final
    print_file_table(
        paths=[os.path.join(FILTERED_TRIMMED_DIR, f) for f in FINAL_FILES],
        label="filtered_trimmed — dataset final",
    )

    print("\n=== pipeline completo ===")
