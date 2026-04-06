"""
Explore negative dollar values and refund patterns in 2024 FEC data.

Questions:
- What transaction types have negative amounts?
- How big is the refund universe?
- Are negatives just refunds, or something else?
"""

from pathlib import Path
import pandas as pd

DATA_ROOT = Path(__file__).resolve().parent.parent.parent / "spending_tracker" / "data"
OUT_DIR = Path(__file__).resolve().parent.parent / "outputs"

ITCONT_COLS = [
    "cmte_id", "amndt_ind", "rpt_tp", "transaction_pgi", "image_num",
    "transaction_tp", "entity_tp", "name", "city", "state",
    "zip_code", "employer", "occupation", "transaction_dt", "transaction_amt",
    "other_id", "tran_id", "file_num", "memo_cd", "memo_text", "sub_id",
]

# ── Load itcont ──────────────────────────────────────────────────────
print("Loading itcont...")
itcont = pd.read_csv(
    DATA_ROOT / "interim" / "fec" / "2024" / "indiv24" / "itcont.txt",
    sep="|", header=None, names=ITCONT_COLS, dtype="string", na_filter=False,
)
itcont["transaction_amt"] = pd.to_numeric(itcont["transaction_amt"], errors="coerce").fillna(0.0)

# ── Negative amounts in itcont ───────────────────────────────────────
negatives = itcont[itcont["transaction_amt"] < 0].copy()

print(f"\n{'='*70}")
print("NEGATIVE AMOUNTS IN ITCONT")
print(f"{'='*70}")
print(f"Total rows in itcont: {len(itcont):,}")
print(f"Rows with negative amounts: {len(negatives):,}")
print(f"Sum of negative amounts: ${negatives['transaction_amt'].sum():,.2f}")

print(f"\nBy transaction type:")
neg_by_tt = (
    negatives.groupby(["transaction_tp", "memo_cd"])
    .agg(rows=("transaction_amt", "size"), total=("transaction_amt", "sum"))
    .reset_index()
    .sort_values("total")
)
print(neg_by_tt.to_string(index=False))

# ── All refund-type transactions (22Y, 22Z, etc.) ───────────────────
# Check both positive and negative amounts for refund codes
refund_codes = ["22Y", "22Z", "22R", "28L", "17R", "17U", "17Y", "17Z"]
refund_rows = itcont[itcont["transaction_tp"].isin(refund_codes)].copy()

print(f"\n{'='*70}")
print("REFUND-TYPE TRANSACTION CODES IN ITCONT")
print(f"{'='*70}")
print(f"Rows with refund codes: {len(refund_rows):,}")

refund_summary = (
    refund_rows.groupby(["transaction_tp", "memo_cd"])
    .agg(
        rows=("transaction_amt", "size"),
        total=("transaction_amt", "sum"),
        min_amt=("transaction_amt", "min"),
        max_amt=("transaction_amt", "max"),
        mean_amt=("transaction_amt", "mean"),
        n_positive=("transaction_amt", lambda x: (x > 0).sum()),
        n_negative=("transaction_amt", lambda x: (x < 0).sum()),
        n_zero=("transaction_amt", lambda x: (x == 0).sum()),
    )
    .reset_index()
    .sort_values("total", ascending=False)
)
print(refund_summary.to_string(index=False))

# ── Now check: are there negative amounts on NON-refund codes? ───────
non_refund_negatives = negatives[~negatives["transaction_tp"].isin(refund_codes)]

print(f"\n{'='*70}")
print("NEGATIVE AMOUNTS ON NON-REFUND TRANSACTION CODES")
print(f"{'='*70}")
print(f"Rows: {len(non_refund_negatives):,}")
print(f"Sum: ${non_refund_negatives['transaction_amt'].sum():,.2f}")

if len(non_refund_negatives) > 0:
    non_refund_neg_tt = (
        non_refund_negatives.groupby(["transaction_tp", "memo_cd"])
        .agg(rows=("transaction_amt", "size"), total=("transaction_amt", "sum"))
        .reset_index()
        .sort_values("total")
    )
    print(non_refund_neg_tt.to_string(index=False))

    print(f"\nSample rows (largest negative amounts on non-refund codes):")
    sample = non_refund_negatives.nsmallest(20, "transaction_amt")[
        ["name", "employer", "transaction_tp", "memo_cd", "memo_text",
         "transaction_amt", "transaction_dt", "cmte_id"]
    ]
    print(sample.to_string(index=False))

# ── Same analysis for itoth ──────────────────────────────────────────
print(f"\n{'='*70}")
print("Loading itoth...")
itoth = pd.read_csv(
    DATA_ROOT / "interim" / "fec" / "2024" / "oth24" / "itoth.txt",
    sep="|", header=None, names=ITCONT_COLS, dtype="string", na_filter=False,
)
itoth["transaction_amt"] = pd.to_numeric(itoth["transaction_amt"], errors="coerce").fillna(0.0)

negatives_oth = itoth[itoth["transaction_amt"] < 0].copy()

print(f"\n{'='*70}")
print("NEGATIVE AMOUNTS IN ITOTH")
print(f"{'='*70}")
print(f"Total rows: {len(itoth):,}")
print(f"Negative rows: {len(negatives_oth):,}")
print(f"Sum of negatives: ${negatives_oth['transaction_amt'].sum():,.2f}")

print(f"\nBy transaction type:")
neg_oth_tt = (
    negatives_oth.groupby(["transaction_tp", "memo_cd"])
    .agg(rows=("transaction_amt", "size"), total=("transaction_amt", "sum"))
    .reset_index()
    .sort_values("total")
)
print(neg_oth_tt.to_string(index=False))

# ── Scale check: refunds as % of gross ───────────────────────────────
print(f"\n{'='*70}")
print("SCALE CHECK")
print(f"{'='*70}")
gross_positive = itcont[itcont["transaction_amt"] > 0]["transaction_amt"].sum()
gross_refunds_22y = refund_rows[refund_rows["transaction_tp"] == "22Y"]["transaction_amt"].sum()
all_negatives_sum = negatives["transaction_amt"].sum()

print(f"  itcont gross positive:  ${gross_positive:>20,.2f}")
print(f"  itcont 22Y refunds:     ${gross_refunds_22y:>20,.2f}")
print(f"  itcont all negatives:   ${all_negatives_sum:>20,.2f}")
print(f"  22Y as % of gross:      {abs(gross_refunds_22y)/gross_positive*100:.3f}%")
print(f"  All neg as % of gross:  {abs(all_negatives_sum)/gross_positive*100:.3f}%")

# Save
neg_by_tt.to_csv(OUT_DIR / "negatives_itcont_by_tt_2024.csv", index=False)
refund_summary.to_csv(OUT_DIR / "refunds_itcont_summary_2024.csv", index=False)
if len(non_refund_negatives) > 0:
    non_refund_neg_tt.to_csv(OUT_DIR / "negatives_non_refund_itcont_2024.csv", index=False)
neg_oth_tt.to_csv(OUT_DIR / "negatives_itoth_by_tt_2024.csv", index=False)

print(f"\nSaved to {OUT_DIR}/")
