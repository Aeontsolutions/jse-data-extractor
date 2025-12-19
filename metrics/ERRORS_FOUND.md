# Critical Errors Found in Data Standardization

## 🔴 Error #1: Canonical Mapping Never Applied (CRITICAL!)

### Location: `clean_items.ipynb` - Line 1311

**What you wrote:**
```python
df_cleaned["canonical_item_name"] = df_cleaned["standard_item"].str.lower().str.strip()
```

**Why it's wrong:**
- You created a 170-line mapping dictionary (`canonical_item_mapping`, lines 1010-1181)
- But you **never used it**!
- This line just lowercases and strips whitespace
- None of your careful mapping work is applied

**Result:**
- Database has `gross_profit_margin` instead of `gross_margin`
- Database has `return_on_equity` instead of `roe`
- Database has `return_on_assets` instead of `roa`
- Frontend expects exact keys → displays "undefined"

**Correct code:**
```python
canonical_mapping = create_canonical_mapping()
df['standard_item_lower'] = df['standard_item'].str.lower().str.strip()
df['canonical_item_name'] = df['standard_item_lower'].map(canonical_mapping)
df['canonical_item_name'] = df['canonical_item_name'].fillna(df['standard_item_lower'])
```

---

## 🔴 Error #2: Wrong Margin Key Names

### Location: `clean_items.ipynb` - Lines 1059-1070 (mapping dictionary)

**What's in your data:**
```
gross_profit_margin
net_profit_margin
operating_profit_margin
```

**What frontend expects:**
```python
# From Overview.svelte
gross_margin     // NOT gross_profit_margin
net_margin       // NOT net_profit_margin
operating_margin // NOT operating_profit_margin
```

**Your mapping had this:**
```python
'gross_profit_margin': 'gross_margin',  # ✓ Correct
'operating_profit_margin': 'operating_margin',  # ✓ Correct
'net_profit_margin': 'net_margin',  # ✓ Correct
```

**But Error #1 means it was never applied!**

---

## 🔴 Error #3: ROA/ROE Wrong Format

### Location: Same issue - mapping exists but not applied

**What's in your data:**
```
return_on_equity
return_on_assets
```

**What frontend expects:**
```python
roe  // NOT return_on_equity
roa  // NOT return_on_assets
```

**Your mapping had this:**
```python
'return_on_equity': 'roe',  # ✓ Correct
'return_on_assets': 'roa',  # ✓ Correct
```

**But again, never applied!**

---

## 🔴 Error #4: No Validation

### Location: Missing entirely from notebook

**Problem:**
- No check that all 15 required keys exist
- No validation before saving CSV
- No verification after BigQuery upload
- Errors only discovered when frontend loads data

**What should happen:**
```python
required_keys = [
    'revenue', 'gross_profit', 'operating_profit', 'net_profit',
    'eps', 'total_assets', 'total_equity', 'debt_to_equity_ratio',
    'roa', 'roe', 'current_ratio',
    'gross_margin', 'ebitda_margin', 'operating_margin', 'net_margin'
]

available_keys = df['canonical_item_name'].unique()
missing = set(required_keys) - set(available_keys)

if missing:
    print("ERROR: Missing required keys:", missing)
    # Don't proceed!
```

---

## 🔴 Error #5: Migration Script Column Name Mismatch

### Location: `migrate_multiyear_batch_final.py` - Lines 76-80

**What the migration script expects:**
```python
df = df.rename(columns={
    'item_name_standardized': 'item_name',
    'item_type_filled': 'item_type',
    'canonical_item_name': 'standard_item',
    'item_value': 'item',  # ← Renames to 'item'
})
```

**What the notebook saves (line 1670):**
```python
cols_to_keep = [
    # ...
    "item_value",  # ← Keeps as 'item_value'
    # ...
]
```

**But wait, line 58 in migration script:**
```python
# df['item'] = pd.to_numeric(df['item_value'], errors='coerce')
# ↑ This line is COMMENTED OUT!
```

**Result:**
- Column name confusion
- Potential data loss or errors during upload

---

## 📊 Impact Assessment

### What users will see in frontend:

```javascript
// Overview.svelte expects these exact keys:
instrument["fin_details"].gross_margin      // ❌ undefined (has gross_profit_margin)
instrument["fin_details"].net_margin        // ❌ undefined (has net_profit_margin)
instrument["fin_details"].operating_margin  // ❌ undefined (has operating_profit_margin)
instrument["fin_details"].roa               // ❌ undefined (has return_on_assets)
instrument["fin_details"].roe               // ❌ undefined (has return_on_equity)
```

**Result:** Empty or "undefined" values in financial metrics cards!

---

## ✅ How Fixed Scripts Solve This

### 1. `clean_and_standardize_items.py`

**Fixes Error #1:**
```python
def apply_canonical_mapping(df):
    canonical_mapping = create_canonical_mapping()
    df['standard_item_lower'] = df['standard_item'].str.lower().str.strip()
    df['canonical_item_name'] = df['standard_item_lower'].map(canonical_mapping)
    df['canonical_item_name'] = df['canonical_item_name'].fillna(df['standard_item_lower'])
    return df
```
✅ Actually applies the mapping!

**Fixes Error #2 & #3:**
```python
# In create_canonical_mapping():
'gross_profit_margin': 'gross_margin',  # Applied ✓
'return_on_equity': 'roe',              # Applied ✓
```
✅ Margins and returns mapped correctly!

**Fixes Error #4:**
```python
def validate_required_keys(df):
    available_keys = set(df['canonical_item_name'].dropna().unique())
    missing_keys = set(REQUIRED_KEYS) - available_keys
    
    if missing_keys:
        print("⚠️ WARNING: Missing required keys:")
        for key in missing_keys:
            print(f"  ✗ {key}")
        return False
    else:
        print("✅ SUCCESS: All 15 required keys are present!")
        return True
```
✅ Validates before saving!

### 2. `migrate_multiyear_batch_fixed.py`

**Fixes Error #5:**
```python
# Correctly handles column names
df = df.rename(columns={
    'item_name_standardized': 'item_name',
    'item_type_filled': 'item_type',
    'canonical_item_name': 'standard_item',
    # item_value stays as item_value
})
```
✅ Consistent column naming!

**Adds validation:**
```python
def validate_csv_before_upload(csv_path):
    # Pre-upload validation
    # Checks all required keys exist
    # Warns if any missing
    return validation_passed
```
✅ Catches issues before upload!

---

## 🎯 Summary

| Error | Severity | Impact | Fixed |
|-------|----------|--------|-------|
| Mapping not applied | CRITICAL | Wrong keys in DB | ✅ |
| Margin names | CRITICAL | Frontend shows undefined | ✅ |
| ROA/ROE format | CRITICAL | Frontend shows undefined | ✅ |
| No validation | HIGH | Silent failures | ✅ |
| Column mismatch | MEDIUM | Potential data loss | ✅ |

---

## 🚀 Next Steps

1. **Run the fixed cleaning script:**
   ```bash
   python metrics/clean_and_standardize_items.py
   ```

2. **Verify output shows:**
   ```
   ✅ SUCCESS: All 15 required keys are present!
   ```

3. **Run the fixed migration script:**
   ```bash
   python metrics/migrate_multiyear_batch_fixed.py
   ```

4. **Verify BigQuery has correct keys:**
   - Query: `SELECT DISTINCT standard_item FROM multiyear_financial_data`
   - Should see: `gross_margin`, `roe`, `roa`, etc. (not the long versions)

5. **Test frontend:**
   - All 15 metrics should display values
   - No "undefined" in Overview.svelte components


