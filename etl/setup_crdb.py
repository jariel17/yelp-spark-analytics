"""
Finalización del esquema de CockroachDB — ejecutar una vez después de pipeline.py.

Spark crea las columnas como nullable por defecto y no puede definir PRIMARY KEY
a través de JDBC. Este script aplica las constraints que faltan sobre la tabla
'listings' ya cargada.

ORDEN DE EJECUCIÓN:
  1. uv run etl/pipeline.py     ← carga los datos
  2. uv run etl/setup_crdb.py   ← aplica PRIMARY KEY

IDEMPOTENTE: si la tabla ya tiene PRIMARY KEY, el script lo detecta y sale sin error.
"""
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

CRDB_CONNECTION = os.getenv("CRDB_CONNECTION")


def finalizar_schema() -> None:
    conn = psycopg2.connect(CRDB_CONNECTION)
    conn.autocommit = True  # DDL requiere autocommit en CockroachDB
    cur = conn.cursor()

    # Verificar que la tabla existe antes de continuar
    cur.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = 'listings'
    """)
    if cur.fetchone()[0] == 0:
        print("La tabla 'listings' no existe — ejecutar pipeline.py primero.")
        cur.close()
        conn.close()
        return

    # Verificar si ya tiene PRIMARY KEY — evita error si se corre dos veces
    cur.execute("""
        SELECT COUNT(*)
        FROM information_schema.table_constraints
        WHERE table_name = 'listings'
          AND constraint_type = 'PRIMARY KEY'
    """)
    if cur.fetchone()[0] > 0:
        print("'listings' ya tiene PRIMARY KEY — nada que hacer.")
        cur.close()
        conn.close()
        return

    # Paso 1: cambiar business_id a NOT NULL
    # Funciona sin error porque pipeline.py filtra business_id IS NOT NULL en limpieza
    cur.execute("ALTER TABLE listings ALTER COLUMN business_id SET NOT NULL")
    print("business_id SET NOT NULL — aplicado")

    # Paso 2: agregar PRIMARY KEY
    cur.execute("ALTER TABLE listings ADD PRIMARY KEY (business_id)")
    print("PRIMARY KEY (business_id) — aplicado")

    cur.close()
    conn.close()
    print("\nEsquema finalizado correctamente.")


if __name__ == "__main__":
    finalizar_schema()
