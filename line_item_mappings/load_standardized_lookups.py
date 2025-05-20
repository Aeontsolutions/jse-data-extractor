#!/usr/bin/env python3
"""
build_and_load_lookups.py    derive lookup tables from Line Item Mapping.csv
                              and load them into BigQuery (jse_standardized).

Changes v2:
* Removed operator_sequence concept.
* `lu_calculated_line_items` is now **one row per operand** inside a calculated
  expression and carries explicit `operation` ("+" or "-") **and**
  `operation_order` (1‑based position in the expression).
* Every operand row also repeats the `company_operand_line_item` so joins are
  trivial.

Requires:
    pip install pandas google-cloud-bigquery python-dateutil tqdm
    export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa_key.json
"""

import logging
import re
import sys
from pathlib import Path
from typing import List, Dict

import pandas as pd
from dateutil import parser
from google.cloud import bigquery
from tqdm import tqdm

# ── CONFIG ──────────────────────────────────────────────────────────────────
SRC_CSV         = Path("Line Item Mapping.csv")
OUT_LU_MAP      = Path("lu_line_item_mappings.csv")
OUT_LU_CALC     = Path("lu_calculated_line_items.csv")
OUT_LU_INDS     = Path("lu_industries.csv")
OUT_LU_EXC      = Path("lu_line_item_exceptions.csv")

BQ_PROJECT      = "jse-datasphere"
BQ_DATASET      = "jse_standardized_dev_elroy"
LOG_LEVEL       = logging.INFO
LOG_FILE        = "build_and_load_lookups.log"
# ────────────────────────────────────────────────────────────────────────────


def setup_logging() -> None:
    fmt = "%(asctime)s | %(levelname)-8s | %(message)s"
    datefmt = "%H:%M:%S"
    logging.basicConfig(
        level=LOG_LEVEL,
        format=fmt,
        datefmt=datefmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(LOG_FILE, mode="w"),
        ],
    )


# ════════════════════════════════════════════════════════════════════════════
#  1.  CSV → dataframes
# ════════════════════════════════════════════════════════════════════════════
BRACE_RE = re.compile(r"\{.*?\}")
OP_SPLIT_RE = re.compile(r"\s*([+\-])\s*")  # keeps operators as separate tokens


def strip_braces(text: str) -> str:
    """Remove {...} snippets and trim whitespace."""
    text = str(text or "").strip()
    return BRACE_RE.sub("", text or "").strip()


def iso_date(date_str: str) -> str:
    """'September 30, 2022' → '2022-09-30'; empty on failure."""
    if not date_str:
        return ""
    try:
        return parser.parse(date_str, dayfirst=False).date().isoformat()
    except Exception:
        logging.debug("bad date: %s", date_str)
        return ""


def build_lookups(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    li_rows: List[Dict] = []
    calc_rows: List[Dict] = []
    ind_rows: List[Dict] = []
    exc_rows: List[Dict] = []

    # treat + or - as an operator ONLY when surrounded by whitespace
    OP_SPLIT_RE = re.compile(r'\s+([+-])\s+')

    for _, r in tqdm(df.iterrows(), total=len(df), desc="transform"):
        symbol = r["Symbol"].strip()
        company_item_raw = r["Company Specific Line Item"]
        company_item = strip_braces(company_item_raw)
        std_item = strip_braces(r["Standardized Line Item"])
        is_calc = str(r["Is_Calculated"]).strip() == "1"
        note_raw = str(r["Note"] or "")
        note = note_raw.lower()
        annual_period = iso_date(r["Annual Period"])

        # ── industries (unique later) ────────────────────────────────────────
        ind_rows.append(
            {
                "symbol": symbol,
                "company_name": str(r["Company Name"]).strip(),
                "industry": str(r["Industry"]).strip(),
                "granular_industry": str(r["Sub-Category"] or "").strip(),
                "industry_jse": str(r["JSE Market Index"] or "").strip(),
            }
        )

        # ── non-calculated straightforward ──────────────────────────────────
        if not is_calc:
            li_rows.append(
                {
                    "symbol": symbol,
                    "company_line_item": company_item,
                    "standardized_line_item": std_item or None,
                    "as_of_date": annual_period,
                }
            )
            continue

        # ── calculated line item handling ────────────────────────────────────
        if "calculation provided" not in note:
            exc_rows.append(
                {
                    "symbol": symbol,
                    "company_line_item": company_item,
                    "reason": "calculation missing",
                }
            )
            logging.warning("exception  %s / %s : calculation missing", symbol, company_item)
            continue

        # Take the first line before a brace or newline as the expression
        expression_line = re.split(r"\{", note_raw, maxsplit=1)[0].strip()
        if not expression_line:
            exc_rows.append(
                {
                    "symbol": symbol,
                    "company_line_item": company_item,
                    "reason": "empty expression",
                }
            )
            logging.warning("exception  %s / %s : empty expression", symbol, company_item)
            continue

        # Tokenize into operands & operators while respecting embedded dashes
        parts = OP_SPLIT_RE.split(expression_line)
        operands = parts[::2]                     # even indices
        operators = parts[1::2]                  # odd indices

        # Ensure we can zip safely by treating first operand as implicit '+'
        ops_iter = ["+"] + operators

        for idx, (op_line_item_raw, op_symbol) in enumerate(zip(operands, ops_iter), start=1):
            operand_clean = strip_braces(op_line_item_raw)
            if not operand_clean:
                continue

            # a) linkage row in lu_line_item_mappings (NULL standardized)
            li_rows.append(
                {
                    "symbol": symbol,
                    "company_line_item": operand_clean,
                    "standardized_line_item": None,
                    "as_of_date": annual_period,
                }
            )

            # b) detailed row in lu_calculated_line_items
            calc_rows.append(
                {
                    "symbol": symbol,
                    "standardized_line_item": std_item,
                    "operation_order": idx,
                    "operation": op_symbol,
                    "company_operand_line_item": operand_clean,
                }
            )

    # de-dupe industries
    ind_df = (
        pd.DataFrame(ind_rows)
        .drop_duplicates()
        .reset_index(drop=True)
    )

    return {
        "map": pd.DataFrame(li_rows),
        "calc": pd.DataFrame(calc_rows),
        "inds": ind_df,
        "exc": pd.DataFrame(exc_rows),
    }



# ════════════════════════════════════════════════════════════════════════════
#  2.  write CSVs
# ════════════════════════════════════════════════════════════════════════════

def write_csvs(lu: Dict[str, pd.DataFrame]) -> None:
    lu["map"].to_csv(OUT_LU_MAP, index=False)
    lu["calc"].to_csv(OUT_LU_CALC, index=False)
    lu["inds"].to_csv(OUT_LU_INDS, index=False)
    lu["exc"].to_csv(OUT_LU_EXC, index=False)
    logging.info("wrote CSVs → %s", Path.cwd())


# ════════════════════════════════════════════════════════════════════════════
#  3.  load to BigQuery
# ════════════════════════════════════════════════════════════════════════════

def load_to_bq(lu: Dict[str, pd.DataFrame]) -> None:
    client = bigquery.Client(project=BQ_PROJECT)
    ds_ref = client.dataset(BQ_DATASET)

    jobs = [
        ("lu_line_item_mappings", lu["map"]),
        ("lu_calculated_line_items", lu["calc"]),
        ("lu_industries", lu["inds"]),
        ("lu_line_item_exceptions", lu["exc"]),
    ]

    for tbl_name, df in jobs:
        logging.info("loading %s (%d rows)…", tbl_name, len(df))
        job_cfg = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(df, ds_ref.table(tbl_name), job_config=job_cfg)
        job.result()
        logging.info("↳ %s done", tbl_name)


# ════════════════════════════════════════════════════════════════════════════

def main() -> None:
    setup_logging()
    logging.info("reading %s", SRC_CSV)
    df_src = pd.read_csv(SRC_CSV)

    lookups = build_lookups(df_src)
    for k, v in lookups.items():
        logging.info("%s rows: %d", k, len(v))

    write_csvs(lookups)
    load_to_bq(lookups)
    logging.info("all done!")


if __name__ == "__main__":
    main()
