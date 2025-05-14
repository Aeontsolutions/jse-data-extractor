#!/usr/bin/env python3
"""
build_standardized_tables.py
──────────────────
Builds standardized financial-statement tables for JSE companies.

    python build_standardized_tables.py                 # all symbols
    python build_standardized_tables.py --symbol WISYNCO  # single symbol debug

Output (all inside dataset **jse_standardized**):

1. staging_line_item_mapping      – every (raw → company → standardized) mapping for this run
2. standardization_audit          – rows that were NONE / AMBIG / LLM_ERROR
3. jse_standardized_<SYMBOL>      – clean table per company, with

       raw_line_item
       company_line_item
       standardized_line_item
       snapshot_date
       match_type
       …plus every original raw column
"""

import argparse, asyncio, json, logging, re, sys
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Tuple
import difflib

import pandas as pd
from google.cloud import bigquery, secretmanager
from google import genai
from copy import deepcopy


# ── CONSTANTS ────────────────────────────────────────────────────────────────
PROJECT            = "jse-datasphere"
DATASET            = "jse_standardized_dev_elroy"            # everything lives here
LOOKUP_TABLE       = "lu_line_item_mappings"
RAW_DATASET        = "jse_raw_financial_data_dev_elroy"
RAW_PREFIX         = "jse_raw_"
STD_PREFIX         = "jse_standardized_"
STAGING_TABLE      = "staging_line_item_mapping"
AUDIT_TABLE        = "standardization_audit"
MODEL_NAME         = "gemini-2.0-flash"
CONCURRENCY        = 20
NORMALISE_RE       = re.compile(r"[ \-']", re.ASCII)  # remove space, dash, apostrophe
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)

bq = bigquery.Client(project=PROJECT)

sm = secretmanager.SecretManagerServiceClient()
API_KEY = sm.access_secret_version(
    request={"name": f"projects/{PROJECT}/secrets/GOOGLE_VERTEX_API_KEY/versions/1"}
).payload.data.decode()

genai_client = genai.Client(api_key=API_KEY)

# ── HELPERS ──────────────────────────────────────────────────────────────────
def norm(s: str) -> str:
    return NORMALISE_RE.sub("", s.lower())

def run_id() -> str:
    return datetime.utcnow().strftime("%Y%m%dT%H%M%S")

def choose_snapshot(dates: List[str], report_date: datetime.date) -> str:
    if not dates:
        raise ValueError("no snapshot dates")
    sorted_dates = sorted(pd.to_datetime(dates).date)
    if len(sorted_dates) == 1:
        return sorted_dates[0].isoformat()
    if len(sorted_dates) == 2:
        return sorted_dates[1].isoformat() if report_date >= sorted_dates[1] else sorted_dates[0].isoformat()
    return max(d for d in sorted_dates if d <= report_date).isoformat()

def similarity(a: str, b: str) -> float:
    return difflib.SequenceMatcher(None, a.lower(), b.lower()).ratio()

# ── LLM SET-UP (exactly like in your notebook) ──────────────────────────────
CANON_SCHEMA_TEMPLATE   = {
    "type": "ARRAY",
    "items": {
        "type": "OBJECT",
        "properties": {
            "company_line_item": {
                "type": "STRING",
                "enum": []
            },
            "raw_match": {
                "type": "STRING",
                "enum": []
            },
        },
        "required": ["company_line_item", "raw_match"],
    },
}

async def map_canonical_to_raw(
    canonical_items: List[str],
    raw_headers: List[str],
) -> Dict[str, str]:
    """
    Returns dict {company_line_item -> raw_match | NONE | AMBIG | LLM_ERROR}
    """

    prompt = f"""
    You are a meticulous financial analyst.

    **Goal**
    For each *company-specific line item* below, pick **exactly one** matching raw
    header from this statement slice **only if you are >90% certain**.  

    - If you are uncertain at all, return "NONE".  
    - If more than one raw header is equally plausible, return "AMBIG".

    A good rule of thumb is that if the name isn't a very close match, it probably isn't one. I don't want you making guesses.
    A good match could be mapping "Net Income" to "Net Income (Loss) Attributable to Common Shareholders".
    A bad match would be mapping "Equity" to "Capital and reserves attributable to equity holders of the parent".
    There are upstream processes built to ensure that you are getting data that is *close* so if it's not then it's not a match.

    Again, if you're not sure, return "NONE".

    **Company-specific items** ({len(canonical_items)}):
    {json.dumps(canonical_items, indent=2)}

    **Raw headers in this slice** ({len(raw_headers)}):
    {json.dumps(raw_headers, indent=2)}

    Respond with a JSON array.  Each element **must** follow this schema:

    ```json
    {{
    "company_line_item": "<one of the canonical items verbatim>",
    "raw_match": "<one raw header OR 'NONE' OR 'AMBIG'>"
    }}
    ```
    """
    # build enum
    schema = deepcopy(CANON_SCHEMA_TEMPLATE)
    valid_canonical = [c for c in canonical_items if isinstance(c, str) and c.strip()]
    schema["items"]["properties"]["company_line_item"]["enum"] = valid_canonical
    valid_raws = [r for r in raw_headers if isinstance(r, str) and r.strip()]
    schema["items"]["properties"]["raw_match"]["enum"] = valid_raws + ["NONE", "AMBIG"]

    cfg = {
        "response_mime_type": "application/json",
        "response_schema": schema,
        "temperature": 0,
    }

    def sync_call():
        return genai_client.models.generate_content(
            model=MODEL_NAME,
            contents=[prompt],
            config=cfg,
        )

    resp = await asyncio.to_thread(sync_call)
    try:
        arr = json.loads(resp.text)
        return {d["company_line_item"]: d["raw_match"] for d in arr}
    except Exception as e:
        logging.error("LLM JSON parse error: %s", e)
        return {c: "LLM_ERROR" for c in canonical_items}

# ── LOOKUP LOADER ────────────────────────────────────────────────────────────
def load_lookup(symbol: str):
    """
    Returns (dated_snapshots, timeless_variants)

    dated_snapshots:  {date -> {standardized -> [company_variants]}}
    timeless_variants: {standardized -> [company_variants]}  (as_of_date IS NULL)
    """
    sql = f"""
        SELECT company_line_item, standardized_line_item, as_of_date
        FROM `{PROJECT}.{DATASET}.{LOOKUP_TABLE}`
        WHERE symbol = @sym
    """
    job = bq.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("sym", "STRING", symbol)]
        ),
    )
    timeless = defaultdict(list)
    dated = defaultdict(lambda: defaultdict(list))
    for r in job:
        if r.as_of_date is None:
            timeless[r.standardized_line_item].append(r.company_line_item)
        else:
            dated[str(r.as_of_date)][r.standardized_line_item].append(r.company_line_item)
    return dict(dated), dict(timeless)

# ── STAGING TABLES ───────────────────────────────────────────────────────────
def recreate_staging():
    schema_map = [
    bigquery.SchemaField("run_id", "STRING"),
    bigquery.SchemaField("symbol", "STRING"),
    bigquery.SchemaField("snapshot_date", "DATE"),
    bigquery.SchemaField("report_date", "DATETIME"),          
    bigquery.SchemaField("period", "STRING"),                 
    bigquery.SchemaField("period_type", "STRING"),            
    bigquery.SchemaField("group_or_company_level", "STRING"), 
    bigquery.SchemaField("raw_line_item", "STRING"),
    bigquery.SchemaField("company_line_item", "STRING"),
    bigquery.SchemaField("standardized_line_item", "STRING"),
    bigquery.SchemaField("match_type", "STRING"),
    ]

    schema_audit = [
        bigquery.SchemaField("run_id", "STRING"),
        bigquery.SchemaField("symbol", "STRING"),
        bigquery.SchemaField("csv_path", "STRING"),
        bigquery.SchemaField("report_date", "DATETIME"),
        bigquery.SchemaField("period", "STRING"),
        bigquery.SchemaField("period_type", "STRING"),
        bigquery.SchemaField("group_or_company_level", "STRING"),
        bigquery.SchemaField("snapshot_date", "DATE"),
        bigquery.SchemaField("company_line_item", "STRING"),
        bigquery.SchemaField("standardized_line_item", "STRING"),
        bigquery.SchemaField("status", "STRING"),          # NONE | AMBIG | LLM_ERROR | EXPECTED_MISSING
        bigquery.SchemaField("llm_detail", "STRING"),      # raw header or extra context
    ]

    for tbl, schema in ((STAGING_TABLE, schema_map), (AUDIT_TABLE, schema_audit)):
        bq.delete_table(f"{PROJECT}.{DATASET}.{tbl}", not_found_ok=True)
        bq.create_table(bigquery.Table(f"{PROJECT}.{DATASET}.{tbl}", schema=schema))

# ── PROCESS ONE COMPANY ──────────────────────────────────────────────────────
async def process_company(symbol: str, run: str):
    """
    Build mapping + audit rows for one company and rebuild its standardized table.
    Adds a dedup guard so each (raw_line_item, snapshot_date) pair is emitted only once.
    """

    raw_tbl = f"{PROJECT}.{RAW_DATASET}.{RAW_PREFIX}{symbol}"
    logging.info("▶ %-8s", symbol)

    # ---- load lookup snapshots ------------------------------------------------
    dated_snaps, timeless = load_lookup(symbol)
    if not dated_snaps and not timeless:
        logging.warning("No lookup rows for %s — skipped", symbol)
        return

    # ---- gather raw slice info ------------------------------------------------
    sql = f"""
        SELECT report_date, period, period_type, group_or_company_level,
               csv_path, line_item
        FROM `{raw_tbl}`
        WHERE line_item IS NOT NULL
    """
    slices: Dict[Tuple, Dict] = {}
    for r in bq.query(sql):
        key = (r.report_date, r.period, r.period_type, r.group_or_company_level)
        entry = slices.setdefault(key, {"csv": set(), "headers": set()})
        entry["csv"].add(r.csv_path)
        entry["headers"].add(r.line_item)

    sem = asyncio.Semaphore(CONCURRENCY)
    map_rows, audit_rows = [], []

    # --- NEW: dedup set to avoid duplicate mappings ---
    seen_map_keys: set[tuple] = set()   # (raw_line_item, snapshot_date)

    async def handle_slice(slice_key, slice_data):
        # ── unpack the slice descriptor ──────────────────────────────────────────
        rd, per, ptyp, gcl = slice_key
        snapshot_date = (
            choose_snapshot(list(dated_snaps), rd.date()) if dated_snaps else None
        )

        # ── build lookup structures (variant → (canonical, std)) ────────────────
        variant_map: Dict[str, Tuple[str, str]] = {}
        canonical_items: List[str] = []

        def _add_variant(v: str, std: str):
            """utility: skip nulls and register the normalized key"""
            if v and isinstance(v, str) and v.strip():
                variant_map[norm(v)] = (v, std)

        # snapshot-dated variants ---------------------------------------------------
        if snapshot_date:
            for std, variants in dated_snaps[snapshot_date].items():
                canonical_items.append(std)
                for v in variants:
                    _add_variant(v, std)
                # ensure canonical key itself exists
                _add_variant(std, std)

        # timeless variants ---------------------------------------------------------
        for std, variants in timeless.items():
            if std not in canonical_items:
                canonical_items.append(std)
            for v in variants:
                _add_variant(v, std)
            _add_variant(std, std)

        canonical_items = [std for std in canonical_items if std and isinstance(std, str)]

        raw_headers = list(slice_data["headers"])

        # ── exact matches (string-normalized) ───────────────────────────────────
        mapped_lookup = set()
        for raw in raw_headers:
            n = norm(raw)
            if n in variant_map:
                company_variant, std = variant_map[n]
                mapped_lookup.add(company_variant)

                dedup = (raw, snapshot_date, rd, per, ptyp, gcl)
                if dedup not in seen_map_keys:
                    seen_map_keys.add(dedup)
                    map_rows.append(
                        dict(
                            run_id=run, symbol=symbol, snapshot_date=snapshot_date,
                            report_date=rd, period=per, period_type=ptyp,
                            group_or_company_level=gcl,
                            raw_line_item=raw,
                            company_line_item=company_variant,
                            standardized_line_item=std,
                            match_type="EXACT",
                        )
                    )

        # ── LLM pass: canonical ► raw header (one-to-one) ───────────────────────
        async with sem:
            llm_res = await map_canonical_to_raw(canonical_items, raw_headers)

        for canonical in canonical_items:
            raw_match = llm_res.get(canonical, "LLM_ERROR")

            # ── evaluate similarity ------------------------------------------------
            sim_score = (
                similarity(canonical, raw_match)
                if raw_match not in ("NONE", "AMBIG", "LLM_ERROR")
                else 0.0
            )
            pass_similarity = sim_score >= 0.70 

            if raw_match not in ("NONE", "AMBIG", "LLM_ERROR"):
                mapped_lookup.add(canonical)

                dedup = (raw_match, snapshot_date, rd, per, ptyp, gcl)
                if dedup not in seen_map_keys:
                    seen_map_keys.add(dedup)
                    map_rows.append(
                        dict(
                            run_id=run, symbol=symbol, snapshot_date=snapshot_date,
                            report_date=rd, period=per, period_type=ptyp,
                            group_or_company_level=gcl,
                            raw_line_item=raw_match,
                            company_line_item=canonical,
                            standardized_line_item=variant_map[norm(canonical)][1],
                            match_type="LLM",
                        )
                    )

                # Log low-confidence matches
                if not pass_similarity:
                    audit_rows.append(
                        dict(
                            run_id=run, symbol=symbol,
                            csv_path=next(iter(slice_data["csv"])),
                            report_date=rd, period=per, period_type=ptyp,
                            group_or_company_level=gcl,
                            snapshot_date=snapshot_date,
                            company_line_item=canonical,
                            standardized_line_item=variant_map[norm(canonical)][1],
                            status="LOW_CONFIDENCE",
                            llm_detail=f"{raw_match} (sim={sim_score:.2f})",
                        )
                    )
            else:
                # log LLM ambiguity / failure for a canonical item
                audit_rows.append(
                    dict(
                        run_id=run, symbol=symbol,
                        csv_path=next(iter(slice_data["csv"])),
                        report_date=rd, period=per, period_type=ptyp,
                        group_or_company_level=gcl,
                        snapshot_date=snapshot_date,
                        company_line_item=canonical,
                        standardized_line_item=variant_map[norm(canonical)][1],
                        status=raw_match,           # NONE | AMBIG | LLM_ERROR
                        llm_detail=None,
                    )
                )

        # ── raw headers that never matched any lookup item ─────────────────────────
        raw_headers = list(slice_data["headers"])

        exact_matched = {
            raw for raw in raw_headers
            if norm(raw) in variant_map
        }
        mapped_raws = {
            rm for rm in llm_res.values()
            if rm not in ("NONE", "AMBIG", "LLM_ERROR")
        }

        # 3) everything else is noise
        unmapped_noise = set(raw_headers) - exact_matched - mapped_raws

        for raw in unmapped_noise:
            audit_rows.append(
                dict(
                    run_id=run,
                    symbol=symbol,
                    csv_path=next(iter(slice_data["csv"])),
                    report_date=rd,
                    period=per,
                    period_type=ptyp,
                    group_or_company_level=gcl,
                    snapshot_date=snapshot_date,
                    company_line_item=None,
                    standardized_line_item=None,
                    status="RAW_UNMAPPED",
                    llm_detail=raw,
                )
            )


        # ── lookup items still missing after LLM pass ───────────────────────────
        expected = {v for (_, (v, _)) in variant_map.items()}
        for missing in expected - mapped_lookup:
            std = variant_map[norm(missing)][1]
            audit_rows.append(
                dict(
                    run_id=run, symbol=symbol,
                    csv_path=next(iter(slice_data["csv"])),
                    report_date=rd, period=per, period_type=ptyp,
                    group_or_company_level=gcl,
                    snapshot_date=snapshot_date,
                    company_line_item=missing,
                    standardized_line_item=std,
                    status="EXPECTED_MISSING",
                    llm_detail=None,
                )
            )



    # run every slice concurrently
    await asyncio.gather(*(handle_slice(k, v) for k, v in slices.items()))

    # ---- load mapping --------------------------------------------------------
    if map_rows:
        df_map = pd.DataFrame(map_rows)
        df_map["snapshot_date"] = (
            pd.to_datetime(df_map["snapshot_date"], errors="coerce").dt.date
        )
        bq.load_table_from_dataframe(
            df_map,
            f"{PROJECT}.{DATASET}.{STAGING_TABLE}",
            bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
        ).result()

    # ---- load audit ----------------------------------------------------------
    if audit_rows:
        df_audit = pd.DataFrame(audit_rows)
        df_audit["snapshot_date"] = (
            pd.to_datetime(df_audit["snapshot_date"], errors="coerce").dt.date
        )
        bq.load_table_from_dataframe(
            df_audit,
            f"{PROJECT}.{DATASET}.{AUDIT_TABLE}",
            bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
        ).result()

    # ---- rebuild standardized table -----------------------------------------
    std_tbl = f"{PROJECT}.{DATASET}.{STD_PREFIX}{symbol}"
    sql = f"""
    CREATE OR REPLACE TABLE `{std_tbl}` AS
    SELECT r.*,
        m.raw_line_item,
        m.company_line_item,
        m.standardized_line_item,
        m.snapshot_date,
        m.match_type
    FROM `{raw_tbl}` r
    JOIN `{PROJECT}.{DATASET}.{STAGING_TABLE}` m
    ON r.symbol                 = m.symbol
    AND r.line_item              = m.raw_line_item
    AND r.report_date            = m.report_date
    AND r.period                 = m.period
    AND r.period_type            = m.period_type
    AND r.group_or_company_level = m.group_or_company_level
    AND m.run_id                 = '{run}';
    """
    bq.query(sql).result()
    logging.info("▲ standardized table ready for %s", symbol)

# ── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", help="single company symbol to process")
    args = parser.parse_args()

    recreate_staging()
    rid = run_id()
    logging.info("RUN %s", rid)

    sql = f"""
        SELECT table_name
        FROM `{PROJECT}.{RAW_DATASET}.INFORMATION_SCHEMA.TABLES`
        WHERE table_name LIKE '{RAW_PREFIX}%'
    """
    symbols = [r.table_name.replace(RAW_PREFIX, "") for r in bq.query(sql)]
    if args.symbol:
        if args.symbol not in symbols:
            logging.error("Symbol %s not found", args.symbol)
            sys.exit(1)
        symbols = [args.symbol]

    async def runner():
        await asyncio.gather(*(process_company(sym, rid) for sym in symbols))

    asyncio.run(runner())
    logging.info("DONE.")

if __name__ == "__main__":
    main()
