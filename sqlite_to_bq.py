#!/usr/bin/env python3
"""
sqlite_to_bq.py  –  mirror every table in an SQLite DB to BigQuery.
                              On a per-table failure it logs, appends to a
                              separate failure file, and carries on.

Requires: google-cloud-bigquery, pandas, sqlalchemy, tqdm
"""

import logging
import time
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, inspect
from google.cloud import bigquery
from tqdm import tqdm

# ── CONFIG ──────────────────────────────
DB_PATH  = Path("jse_financial_data.db")
PROJECT  = "jse-datasphere"
DATASET  = "jse_raw_financial_data_dev_elroy"
FAIL_LOG = Path("load_failures.tsv")   # tab-delimited: table_name <tab> error
LOGLEVEL = logging.INFO
# ────────────────────────────────────────

logging.basicConfig(
    level=LOGLEVEL,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)

def record_failure(table: str, err: Exception) -> None:
    """Append the table name + error message to FAIL_LOG."""
    with FAIL_LOG.open("a") as f:
        f.write(f"{table}\t{err}\n")

def main() -> None:
    t0 = time.time()
    logging.info("🔌  SQLite → %s", DB_PATH.resolve())
    engine    = create_engine(f"sqlite:///{DB_PATH}")
    inspector = inspect(engine)

    logging.info("🎯  BigQuery target: %s.%s", PROJECT, DATASET)
    client      = bigquery.Client(project=PROJECT)
    dataset_ref = client.dataset(DATASET)

    FAIL_LOG.unlink(missing_ok=True)  # start fresh each run

    for tbl in tqdm(inspector.get_table_names(), desc="loading", ncols=80):
        step_start = time.time()
        try:
            logging.info("   ↳ %s: pull from SQLite", tbl)
            df = pd.read_sql_table(tbl, con=engine)
            logging.info("   ↳ %s: push %d rows to BQ (overwrite)", tbl, len(df))

            job_cfg = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            job = client.load_table_from_dataframe(df, dataset_ref.table(tbl), job_config=job_cfg)
            job.result()  # wait

            logging.info("   ↳ %s: ✅ done in %.1fs", tbl, time.time() - step_start)

        except Exception as exc:
            logging.error("   ↳ %s: 💥 %s", tbl, exc)
            record_failure(tbl, exc)

    if FAIL_LOG.exists():
        logging.warning("⚠️  Some tables failed – see %s", FAIL_LOG)
    else:
        logging.info("✅  All tables loaded clean.")

    logging.info("🏁  Finished in %.1fs", time.time() - t0)

if __name__ == "__main__":
    main()
