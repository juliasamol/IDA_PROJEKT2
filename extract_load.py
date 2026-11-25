""""
Extract & Load (CDC): Neon (PostgreSQL) -> BigQuery

Uruchamiany przez GitHub Actions (.github/workflows/load.yml).
Ładuje wszystkie tabele bazy źródłowej SEKWENCYJNIE, w kolejności
wynikającej z relacji PK-FK (najpierw tabele nadrzędne, potem podrzędne).

Tryby:
  full_refresh -> pobiera całą tabelę i nadpisuje tabelę w BigQuery (WRITE_TRUNCATE)
  incremental  -> pobiera tylko wiersze z updated_at > MAX(updated_at) w BigQuery
                  i dopisuje je jako kolejne wersje (WRITE_APPEND)

Tryb ustawiany jest przy ręcznym uruchomieniu workflow (zmienna LOAD_MODE).
"""

import os
from datetime import datetime, timezone

import pandas as pd
import psycopg2
from google.cloud import bigquery

# ============================================================
# KONFIGURACJA
# ============================================================

NEON_CONNECTION_STRING = os.environ["NEON_CONNECTION_STRING"]
BQ_PROJECT_ID = os.environ["BQ_PROJECT_ID"]
BQ_DATASET = os.environ["BQ_DATASET"]

# full_refresh / incremental – domyślnie incremental (np. przy uruchomieniu z harmonogramu)
LOAD_MODE = os.environ.get("LOAD_MODE", "incremental").strip().lower()

INCREMENTAL_COLUMN = "updated_at"

# Kolejność ładowania zgodna z relacjami PK-FK:
#   1. regions, categories           – tabele słownikowe, bez kluczy obcych
#   2. stores, products, customers   – zależą od regions / categories
#   3. orders                        – zależy od customers i stores
#   4. order_items                   – zależy od orders i products
TABLES_IN_LOAD_ORDER = [
    "regions",
    "categories",
    "stores",
    "products",
    "customers",
    "orders",
    "order_items",
]

# ============================================================
# LOGIKA
# ============================================================


def get_last_loaded_timestamp(bq_client: bigquery.Client, table_ref: str):
    """Znacznik ostatniego ładowania = MAX(updated_at) w tabeli docelowej.
    Jeśli tabela jeszcze nie istnieje, zwraca None (czyli: załaduj wszystko)."""
    query = f"SELECT MAX({INCREMENTAL_COLUMN}) AS max_ts FROM `{table_ref}`"
    try:
        row = next(iter(bq_client.query(query).result()))
        return row.max_ts
    except Exception:
        return None


def extract_from_neon(conn, table: str, mode: str, last_ts=None) -> pd.DataFrame:
    """Pobiera dane z tabeli źródłowej w Neon."""
    if mode == "incremental" and last_ts is not None:
        query = f"SELECT * FROM {table} WHERE {INCREMENTAL_COLUMN} > %s ORDER BY {INCREMENTAL_COLUMN}"
        return pd.read_sql(query, conn, params=(last_ts,))
    query = f"SELECT * FROM {table}"
    return pd.read_sql(query, conn)


def load_to_bigquery(bq_client: bigquery.Client, df: pd.DataFrame, table_ref: str, mode: str):
    """Ładuje DataFrame do BigQuery."""
    if mode == "full_refresh":
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
    else:
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        )
    job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    return job


def main():
    if LOAD_MODE not in ("full_refresh", "incremental"):
        raise ValueError(f"Nieznany tryb ładowania: {LOAD_MODE}")

    bq_client = bigquery.Client(project=BQ_PROJECT_ID)
    loaded_at = datetime.now(timezone.utc)
    summary = []

    print(f"=== Start ładowania | tryb: {LOAD_MODE} | {loaded_at:%Y-%m-%d %H:%M:%S} UTC ===")

    conn = psycopg2.connect(NEON_CONNECTION_STRING)
    try:
        for step, table in enumerate(TABLES_IN_LOAD_ORDER, start=1):
            table_ref = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{table}"
            print(f"\n[{step}/{len(TABLES_IN_LOAD_ORDER)}] {table} -> {table_ref}")

            last_ts = None
            if LOAD_MODE == "incremental":
                last_ts = get_last_loaded_timestamp(bq_client, table_ref)
                print(f"    Ostatni załadowany timestamp w BigQuery: {last_ts}")

            df = extract_from_neon(conn, table, LOAD_MODE, last_ts)
            print(f"    Wyciągnięto {len(df)} wierszy z Neon")

            if df.empty:
                print("    Brak nowych danych – pomijam tabelę.")
                summary.append((table, 0))
                continue

            # kolumny techniczne warstwy landing
            df["_loaded_at"] = loaded_at
            df["_load_mode"] = LOAD_MODE

            job = load_to_bigquery(bq_client, df, table_ref, LOAD_MODE)
            print(f"    Załadowano {job.output_rows} wierszy (tryb: {LOAD_MODE})")
            summary.append((table, job.output_rows))
    finally:
        conn.close()

    print("\n=== Podsumowanie ===")
    for table, rows in summary:
        print(f"    {table:<12} {rows:>5} wierszy")


if __name__ == "__main__":
    main()
