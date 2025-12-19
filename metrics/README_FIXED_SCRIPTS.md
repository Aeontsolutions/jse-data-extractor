# Fixed Data Standardization Scripts

## 🔴 Problems Found in Original Scripts

### Critical Issues:
1. **Canonical mapping dictionary was never applied** - Line 1311 in `clean_items.ipynb` just did `.str.lower().str.strip()` instead of using the mapping
2. **Wrong margin key names** - Data had `gross_profit_margin`, `net_profit_margin`, `operating_profit_margin` but frontend expects `gross_margin`, `net_margin`, `operating_margin`
3. **ROA/ROE inconsistency** - Mapping existed but wasn't applied, so `return_on_assets` instead of `roa`
4. **Missing validation** - No check that all 15 required keys exist before uploading to BigQuery

## ✅ Fixed Scripts

### 1. `clean_and_standardize_items.py`
**What it does:**
- Loads raw CSV data
- Standardizes item names to snake_case
- Applies proper canonical mapping (the fix!)
- Validates all 15 required keys exist
- Saves cleaned data ready for BigQuery

**Key fixes:**
- Actually applies the canonical mapping dictionary
- Maps margin names correctly: `gross_profit_margin` → `gross_margin`
- Maps returns correctly: `return_on_equity` → `roe`, `return_on_assets` → `roa`
- Validates all required keys before saving

### 2. `migrate_multiyear_batch_fixed.py`
**What it does:**
- Pre-validates data before upload
- Creates BigQuery table with correct schema
- Performs UPSERT (merge) operation
- Verifies required keys exist in BigQuery

**Key fixes:**
- Uses correct column names from cleaned data
- Pre-migration validation step
- Post-migration verification of required keys
- Better error messages

## 📋 Required Frontend Keys (15 total)

```python
# Profitability (4)
'revenue'
'gross_profit'
'operating_profit'
'net_profit'

# Market (1)
'eps'

# Financial Position (6)
'total_assets'
'total_equity'
'debt_to_equity_ratio'
'roa'              # NOT return_on_assets
'roe'              # NOT return_on_equity
'current_ratio'

# Margins (4)
'gross_margin'     # NOT gross_profit_margin
'ebitda_margin'
'operating_margin' # NOT operating_profit_margin
'net_margin'       # NOT net_profit_margin
```

## 🚀 How to Use

### Step 1: Clean and Standardize Data
```bash
python metrics/clean_and_standardize_items.py
```

**Output:**
- `csvs/multiyear_batch/cleaned_standardized_items_fixed.csv` - Main cleaned data
- `csvs/multiyear_batch/canonical_item_mapping.csv` - Mapping reference

**What to check:**
- Console output shows "✅ SUCCESS: All 15 required keys are present!"
- Sample data for DCOVE 2024 shows all required metrics
- No warnings about missing keys

### Step 2: Upload to BigQuery
```bash
python metrics/migrate_multiyear_batch_fixed.py
```

**What it does:**
1. Validates CSV has all required keys
2. Asks for confirmation if any keys missing
3. Creates/verifies BigQuery table
4. Performs UPSERT operation
5. Verifies all required keys in BigQuery

**What to check:**
- Pre-migration validation shows all 15 keys present
- MERGE operation completes successfully
- Post-migration verification shows all keys in BigQuery

## 🔍 Key Differences from Original

### Original (WRONG):
```python
# Line 1311 in clean_items.ipynb
df_cleaned["canonical_item_name"] = df_cleaned["standard_item"].str.lower().str.strip()
# ❌ This just lowercases, doesn't apply mapping!
```

### Fixed (CORRECT):
```python
# clean_and_standardize_items.py
canonical_mapping = create_canonical_mapping()
df['standard_item_lower'] = df['standard_item'].str.lower().str.strip()
df['canonical_item_name'] = df['standard_item_lower'].map(canonical_mapping)
df['canonical_item_name'] = df['canonical_item_name'].fillna(df['standard_item_lower'])
# ✅ Properly applies mapping with fallback
```

## 🎯 Canonical Mapping Examples

The fixed script correctly maps:

```python
# Margins - Critical for frontend!
'gross_profit_margin' → 'gross_margin'
'net_profit_margin' → 'net_margin'
'operating_profit_margin' → 'operating_margin'

# Returns - Must be short form
'return_on_equity' → 'roe'
'return_on_assets' → 'roa'

# EPS - Multiple variations
'earnings_per_share' → 'eps'
'earnings_per_stock_unit' → 'eps'
'ratio_eps' → 'eps'

# Equity - Multiple variations
'shareholders_equity' → 'total_equity'
'stockholders_equity' → 'total_equity'
'total_shareholders_equity' → 'total_equity'
```

## 📊 Validation Output

When you run the cleaning script, you should see:

```
============================================================
VALIDATION: Checking for required frontend keys
============================================================

✓ Present keys (15/15):
  ✓ current_ratio              (50 records)
  ✓ debt_to_equity_ratio        (44 records)
  ✓ ebitda_margin               (66 records)
  ✓ eps                         (93 records)
  ✓ gross_margin                (50 records)
  ✓ gross_profit                (238 records)
  ✓ net_margin                  (23 records)
  ✓ net_profit                  (334 records)
  ✓ operating_margin            (19 records)
  ✓ operating_profit            (290 records)
  ✓ revenue                     (324 records)
  ✓ roa                         (33 records)
  ✓ roe                         (81 records)
  ✓ total_assets                (318 records)
  ✓ total_equity                (130 records)

✅ SUCCESS: All 15 required keys are present!
```

## 🐛 Troubleshooting

### If validation fails:
1. Check the "Missing keys" section in console output
2. Review the mapping in `create_canonical_mapping()` function
3. Verify your source CSV has those metrics
4. Add missing mappings to the canonical_mapping dictionary

### If BigQuery upload fails:
1. Check Google Cloud credentials are set: `GOOGLE_APPLICATION_CREDENTIALS`
2. Verify project ID in `.env` file
3. Check dataset `jse_raw_financial_data_dev_elroy` exists
4. Run with pre-migration validation to catch issues early

## 📝 Notes

- The fixed scripts create new output files with `_fixed` suffix
- Original files are not modified
- Both scripts include extensive logging
- Validation happens at multiple steps
- Safe to run multiple times (UPSERT handles duplicates)

## ✨ Benefits of Fixed Approach

1. **Type safety** - All 15 keys guaranteed to exist
2. **Frontend compatibility** - Keys match exactly what Overview.svelte expects
3. **Validation** - Catches issues before uploading to BigQuery
4. **Audit trail** - Saves mapping reference for review
5. **Idempotent** - Safe to re-run without creating duplicates


