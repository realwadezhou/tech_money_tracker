"""
Check whether itpas2 and itoth contain the same rows for 24K, 24E, 24A.

If they overlap, using both files would double-count. We need to know
which file to use for which purpose.

Strategy: for shared transaction types (24K, 24E, 24A), compare row counts,
dollar totals, and try to match specific rows on key fields.
"""

from pathlib import Path
import pandas as pd

DATA_ROOT = Path(__file__).resolve().parent.parent / "data"
OUT_DIR = Path(__file__).resolve().parent.parent / "outputs"

ITCONT_COLS = [
    "cmte_id", "amndt_ind", "rpt_tp", "transaction_pgi", "image_num",
    "transaction_tp", "entity_tp", "name", "city", "state",
    "zip_code", "employer", "occupation", "transaction_dt", "transaction_amt",
    "other_id", "tran_id", "file_num", "memo_cd", "memo_text", "sub_id",
]

ITPAS2_COLS = [
    "cmte_id", "amndt_ind", "rpt_tp", "transaction_pgi", "image_num",
    "transaction_tp", "entity_tp", "name", "city", "state",
    "zip_code", "employer", "occupation", "transaction_dt", "transaction_amt",
    "other_id", "cand_id", "tran_id", "file_num", "memo_cd", "memo_text", "sub_id",
]

print("Loading itoth...")
itoth = pd.read_csv(
    DATA_ROOT / "interim" / "fec" / "2024" / "oth24" / "itoth.txt",
    sep="|", header=None, names=ITCONT_COLS, dtype="string", na_filter=False,
)
itoth["transaction_amt"] = pd.to_numeric(itoth["transaction_amt"], errors="coerce").fillna(0.0)

print("Loading itpas2...")
itpas2 = pd.read_csv(
    DATA_ROOT / "interim" / "fec" / "2024" / "pas224" / "itpas2.txt",
    sep="|", header=None, names=ITPAS2_COLS, dtype="string", na_filter=False,
)
itpas2["transaction_amt"] = pd.to_numeric(itpas2["transaction_amt"], errors="coerce").fillna(0.0)

# ── High-level comparison for shared types ───────────────────────────
shared_types = ["24K", "24E", "24A", "24Z", "24C", "24F", "24N"]

print(f"\n{'='*70}")
print("HIGH-LEVEL COMPARISON: itoth vs itpas2")
print(f"{'='*70}")
print(f"{'type':<6} {'itoth_rows':>12} {'itoth_$':>18} {'itpas2_rows':>12} {'itpas2_$':>18}")
print("-" * 70)
for tt in shared_types:
    oth_sub = itoth[itoth["transaction_tp"] == tt]
    pas_sub = itpas2[itpas2["transaction_tp"] == tt]
    # Also count memo X variants
    oth_x = itoth[(itoth["transaction_tp"] == tt) & (itoth["memo_cd"] == "X")]
    pas_x = itpas2[(itpas2["transaction_tp"] == tt) & (itpas2["memo_cd"] == "X")]
    print(f"{tt:<6} {len(oth_sub):>12,} {oth_sub['transaction_amt'].sum():>18,.0f} {len(pas_sub):>12,} {pas_sub['transaction_amt'].sum():>18,.0f}")
    if len(oth_x) > 0 or len(pas_x) > 0:
        print(f"  (X)  {len(oth_x):>12,} {oth_x['transaction_amt'].sum():>18,.0f} {len(pas_x):>12,} {pas_x['transaction_amt'].sum():>18,.0f}")

# ── Try to match rows on sub_id (unique row identifier) ──────────────
print(f"\n{'='*70}")
print("SUB_ID OVERLAP CHECK")
print(f"{'='*70}")
for tt in ["24K", "24E", "24A"]:
    oth_ids = set(itoth.loc[itoth["transaction_tp"] == tt, "sub_id"])
    pas_ids = set(itpas2.loc[itpas2["transaction_tp"] == tt, "sub_id"])
    overlap = oth_ids & pas_ids
    print(f"\n{tt}:")
    print(f"  itoth sub_ids:  {len(oth_ids):,}")
    print(f"  itpas2 sub_ids: {len(pas_ids):,}")
    print(f"  overlap:        {len(overlap):,}")
    print(f"  itoth only:     {len(oth_ids - pas_ids):,}")
    print(f"  itpas2 only:    {len(pas_ids - oth_ids):,}")

# ── Try matching on composite key ────────────────────────────────────
print(f"\n{'='*70}")
print("COMPOSITE KEY OVERLAP (cmte_id + tran_id + transaction_amt + transaction_dt)")
print(f"{'='*70}")
for tt in ["24K", "24E", "24A"]:
    oth_sub = itoth[itoth["transaction_tp"] == tt].copy()
    pas_sub = itpas2[itpas2["transaction_tp"] == tt].copy()

    oth_sub["key"] = oth_sub["cmte_id"] + "|" + oth_sub["tran_id"] + "|" + oth_sub["transaction_amt"].astype(str) + "|" + oth_sub["transaction_dt"]
    pas_sub["key"] = pas_sub["cmte_id"] + "|" + pas_sub["tran_id"] + "|" + pas_sub["transaction_amt"].astype(str) + "|" + pas_sub["transaction_dt"]

    oth_keys = set(oth_sub["key"])
    pas_keys = set(pas_sub["key"])
    overlap = oth_keys & pas_keys

    print(f"\n{tt}:")
    print(f"  itoth keys:   {len(oth_keys):,}")
    print(f"  itpas2 keys:  {len(pas_keys):,}")
    print(f"  overlap:      {len(overlap):,}")
    print(f"  itoth only:   {len(oth_keys - pas_keys):,}")
    print(f"  itpas2 only:  {len(pas_keys - oth_keys):,}")

# ── Spot check: pick a specific committee and compare ────────────────
# Use AMERICA PAC since we know it well
print(f"\n{'='*70}")
print("SPOT CHECK: AMERICA PAC (C00879510) 24E rows")
print(f"{'='*70}")
ap_oth = itoth[(itoth["cmte_id"] == "C00879510") & (itoth["transaction_tp"] == "24E")]
ap_pas = itpas2[(itpas2["cmte_id"] == "C00879510") & (itpas2["transaction_tp"] == "24E")]
print(f"  itoth:  {len(ap_oth):,} rows, ${ap_oth['transaction_amt'].sum():,.2f}")
print(f"  itpas2: {len(ap_pas):,} rows, ${ap_pas['transaction_amt'].sum():,.2f}")

if len(ap_oth) > 0 and len(ap_pas) > 0:
    print(f"\n  itoth sample (first 5):")
    print(ap_oth[["cmte_id", "name", "transaction_amt", "transaction_dt", "tran_id", "memo_cd"]].head().to_string(index=False))
    print(f"\n  itpas2 sample (first 5):")
    print(ap_pas[["cmte_id", "name", "transaction_amt", "transaction_dt", "tran_id", "memo_cd"]].head().to_string(index=False))

# ── Check: does itpas2 have data that itoth doesn't? ─────────────────
# Look at itpas2 cmte_ids not in itoth for 24K
print(f"\n{'='*70}")
print("COMMITTEE COVERAGE CHECK FOR 24K")
print(f"{'='*70}")
oth_24k_cmtes = set(itoth.loc[itoth["transaction_tp"] == "24K", "cmte_id"])
pas_24k_cmtes = set(itpas2.loc[itpas2["transaction_tp"] == "24K", "cmte_id"])
print(f"  Committees with 24K in itoth:  {len(oth_24k_cmtes):,}")
print(f"  Committees with 24K in itpas2: {len(pas_24k_cmtes):,}")
print(f"  In both:                       {len(oth_24k_cmtes & pas_24k_cmtes):,}")
print(f"  itoth only:                    {len(oth_24k_cmtes - pas_24k_cmtes):,}")
print(f"  itpas2 only:                   {len(pas_24k_cmtes - oth_24k_cmtes):,}")

print(f"\nSaved to {OUT_DIR}/")
