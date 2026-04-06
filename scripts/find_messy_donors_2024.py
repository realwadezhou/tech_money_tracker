"""
Find donors with the most negative amounts, refunds, and corrections
in 2024 itcont data. We want to study the messy cases to build
robust counting rules.

Rewritten to avoid slow lambda aggregations on 58M rows.
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

print("Loading itcont...")
itcont = pd.read_csv(
    DATA_ROOT / "interim" / "fec" / "2024" / "indiv24" / "itcont.txt",
    sep="|", header=None, names=ITCONT_COLS, dtype="string", na_filter=False,
)
itcont["transaction_amt"] = pd.to_numeric(itcont["transaction_amt"], errors="coerce").fillna(0.0)
print(f"  Loaded {len(itcont):,} rows")

# ── Pre-compute flags to avoid lambdas in groupby ────────────────────
itcont["is_negative"] = itcont["transaction_amt"] < 0
itcont["is_22y"] = itcont["transaction_tp"] == "22Y"
itcont["amt_positive"] = itcont["transaction_amt"].clip(lower=0)
itcont["amt_negative"] = itcont["transaction_amt"].clip(upper=0)
itcont["amt_22y"] = itcont["transaction_amt"].where(itcont["is_22y"], 0.0)

print("Computing donor stats (this may take a minute)...")
donor_stats = (
    itcont.groupby("name")
    .agg(
        total_rows=("transaction_amt", "size"),
        gross_sum=("transaction_amt", "sum"),
        positive_sum=("amt_positive", "sum"),
        negative_sum=("amt_negative", "sum"),
        n_negative_rows=("is_negative", "sum"),
        n_22y_rows=("is_22y", "sum"),
        sum_22y=("amt_22y", "sum"),
    )
    .reset_index()
)
print(f"  Unique donor names: {len(donor_stats):,}")

# ── Top donors by absolute negative amount ───────────────────────────
print(f"\n{'='*70}")
print("TOP 30 DONORS BY NEGATIVE DOLLAR VOLUME")
print(f"{'='*70}")
top_neg = donor_stats.nsmallest(30, "negative_sum")
print(top_neg[["name", "total_rows", "positive_sum", "negative_sum",
               "n_negative_rows", "n_22y_rows", "sum_22y", "gross_sum"]].to_string(index=False))

# ── Top donors by 22Y refund volume ─────────────────────────────────
print(f"\n{'='*70}")
print("TOP 30 DONORS BY 22Y VOLUME (note: 22Y amounts are mostly POSITIVE)")
print(f"{'='*70}")
top_22y = donor_stats.nlargest(30, "sum_22y")
print(top_22y[["name", "total_rows", "positive_sum", "negative_sum",
               "n_22y_rows", "sum_22y", "gross_sum"]].to_string(index=False))

# ── Donors where negatives are a large fraction of their positive ────
donor_stats["neg_pct"] = donor_stats["negative_sum"].abs() / donor_stats["positive_sum"].replace(0, float("nan")) * 100
big_donors_messy = donor_stats[
    (donor_stats["positive_sum"] > 100000) &
    (donor_stats["neg_pct"] > 10)
].sort_values("neg_pct", ascending=False)

print(f"\n{'='*70}")
print("DONORS >$100K POSITIVE WHERE NEGATIVES EXCEED 10% OF POSITIVE")
print(f"{'='*70}")
print(f"Found: {len(big_donors_messy)} donors")
print(big_donors_messy.head(40)[
    ["name", "total_rows", "positive_sum", "negative_sum", "neg_pct",
     "n_22y_rows", "sum_22y", "gross_sum"]
].to_string(index=False))

# ── Deep dive the messiest cases ─────────────────────────────────────
# Pick interesting cases from each list
messy_names = list(dict.fromkeys(
    top_neg.head(5)["name"].tolist() +
    big_donors_messy.head(5)["name"].tolist() +
    top_22y.head(3)["name"].tolist()
))

for donor_name in messy_names:
    rows = itcont[itcont["name"] == donor_name].copy()
    rows = rows.sort_values("transaction_dt")

    print(f"\n{'='*70}")
    print(f"DETAIL: {donor_name}")
    print(f"{'='*70}")
    print(f"Rows: {len(rows)},  Gross sum: ${rows['transaction_amt'].sum():,.2f}")

    tt_breakdown = (
        rows.groupby(["transaction_tp", "memo_cd"])
        .agg(rows=("transaction_amt", "size"), total=("transaction_amt", "sum"))
        .reset_index()
        .sort_values("total", ascending=False)
    )
    print(f"\nBy transaction type:")
    print(tt_breakdown.to_string(index=False))

    # Show negative rows
    neg_rows = rows[rows["transaction_amt"] < 0]
    if len(neg_rows) > 0 and len(neg_rows) <= 30:
        print(f"\nNegative rows ({len(neg_rows)}):")
        print(neg_rows[["transaction_tp", "memo_cd", "memo_text", "transaction_amt",
                        "transaction_dt", "cmte_id"]].to_string(index=False))
    elif len(neg_rows) > 30:
        print(f"\nNegative rows: {len(neg_rows)} (showing top 15 by magnitude):")
        print(neg_rows.nsmallest(15, "transaction_amt")[
            ["transaction_tp", "memo_cd", "memo_text", "transaction_amt",
             "transaction_dt", "cmte_id"]
        ].to_string(index=False))

    # Show 22Y rows
    refund_rows = rows[rows["transaction_tp"] == "22Y"]
    if len(refund_rows) > 0 and len(refund_rows) <= 30:
        print(f"\n22Y rows ({len(refund_rows)}):")
        print(refund_rows[["transaction_tp", "memo_cd", "memo_text", "transaction_amt",
                           "transaction_dt", "cmte_id"]].to_string(index=False))
    elif len(refund_rows) > 30:
        print(f"\n22Y rows: {len(refund_rows)} (showing top 15 by amount):")
        print(refund_rows.nlargest(15, "transaction_amt")[
            ["transaction_tp", "memo_cd", "memo_text", "transaction_amt",
             "transaction_dt", "cmte_id"]
        ].to_string(index=False))

# Save
top_neg.to_csv(OUT_DIR / "top_negative_donors_2024.csv", index=False)
top_22y.to_csv(OUT_DIR / "top_22y_donors_2024.csv", index=False)
big_donors_messy.to_csv(OUT_DIR / "messy_donors_high_neg_pct_2024.csv", index=False)
print(f"\nSaved to {OUT_DIR}/")
