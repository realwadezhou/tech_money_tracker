"""
Deep dive on 22Y sign conventions in 2024 itcont.

Core question: when a 22Y row has a positive amount vs negative amount,
what does each mean? Is the sign consistent or does it depend on the filer?

We'll look at:
- Distribution of positive vs negative 22Y amounts
- Whether the same donor appears with both signs
- Whether we can match 22Y rows to original contribution rows
- Memo text patterns on positive vs negative 22Y
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

CM_COLS = [
    "cmte_id", "cmte_nm", "tres_nm", "cmte_st1", "cmte_st2",
    "cmte_city", "cmte_st", "cmte_zip", "cmte_dsgn", "cmte_tp",
    "cmte_pty_affiliation", "cmte_filing_freq", "org_tp", "connected_org_nm",
    "cand_id",
]

print("Loading itcont...")
itcont = pd.read_csv(
    DATA_ROOT / "interim" / "fec" / "2024" / "indiv24" / "itcont.txt",
    sep="|", header=None, names=ITCONT_COLS, dtype="string", na_filter=False,
)
itcont["transaction_amt"] = pd.to_numeric(itcont["transaction_amt"], errors="coerce").fillna(0.0)

print("Loading committee master...")
cm = pd.read_csv(
    DATA_ROOT / "interim" / "fec" / "2024" / "cm24" / "cm.txt",
    sep="|", header=None, names=CM_COLS, dtype="string", na_filter=False,
)

# ── Isolate all 22Y rows ──────���─────────────────────────────────────
rows_22y = itcont[itcont["transaction_tp"] == "22Y"].copy()

print(f"\n{'='*70}")
print("22Y OVERVIEW")
print(f"{'='*70}")
print(f"Total 22Y rows: {len(rows_22y):,}")
print(f"  Positive amounts: {(rows_22y['transaction_amt'] > 0).sum():,}  sum: ${rows_22y.loc[rows_22y['transaction_amt'] > 0, 'transaction_amt'].sum():,.2f}")
print(f"  Negative amounts: {(rows_22y['transaction_amt'] < 0).sum():,}  sum: ${rows_22y.loc[rows_22y['transaction_amt'] < 0, 'transaction_amt'].sum():,.2f}")
print(f"  Zero amounts:     {(rows_22y['transaction_amt'] == 0).sum():,}")
print(f"  Net sum:          ${rows_22y['transaction_amt'].sum():,.2f}")

# ── Distribution of amounts ──────────────────────────────────────────
print(f"\n{'='*70}")
print("22Y AMOUNT DISTRIBUTION")
print(f"{'='*70}")
print(rows_22y["transaction_amt"].describe().to_string())

print(f"\nHistogram of positive 22Y amounts:")
pos_22y = rows_22y[rows_22y["transaction_amt"] > 0]["transaction_amt"]
bins = [0, 100, 500, 1000, 3300, 6600, 10000, 50000, 100000, 500000, float("inf")]
labels = ["0-100", "100-500", "500-1K", "1K-3.3K", "3.3K-6.6K", "6.6K-10K", "10K-50K", "50K-100K", "100K-500K", "500K+"]
print(pd.cut(pos_22y, bins=bins, labels=labels).value_counts().sort_index().to_string())

print(f"\nHistogram of negative 22Y amounts:")
neg_22y = rows_22y[rows_22y["transaction_amt"] < 0]["transaction_amt"].abs()
print(pd.cut(neg_22y, bins=bins, labels=labels).value_counts().sort_index().to_string())

# ── Memo text on 22Y rows ──────���────────────────────────────────────
print(f"\n{'='*70}")
print("22Y MEMO TEXT PATTERNS")
print(f"{'='*70}")

# Positive 22Y memo texts
print("\nMost common memo_text on POSITIVE 22Y (top 20):")
pos_memos = rows_22y[rows_22y["transaction_amt"] > 0]["memo_text"].value_counts().head(20)
print(pos_memos.to_string())

print("\nMost common memo_text on NEGATIVE 22Y (top 20):")
neg_memos = rows_22y[rows_22y["transaction_amt"] < 0]["memo_text"].value_counts().head(20)
print(neg_memos.to_string())

# ── Memo code on 22Y ────���───────────────────────────────────────────
print(f"\n{'='*70}")
print("22Y MEMO CODE (X vs blank)")
print(f"{'='*70}")
mc = rows_22y.groupby("memo_cd").agg(
    rows=("transaction_amt", "size"),
    total=("transaction_amt", "sum"),
    n_pos=("transaction_amt", lambda x: (x > 0).sum()),
    n_neg=("transaction_amt", lambda x: (x < 0).sum()),
).reset_index()
print(mc.to_string(index=False))

# ── Do positive and negative 22Y come from the same committees? ──────
print(f"\n{'='*70}")
print("22Y BY COMMITTEE TYPE (positive vs negative)")
print(f"{'='*70}")
rows_22y = rows_22y.merge(
    cm[["cmte_id", "cmte_nm", "cmte_tp"]],
    on="cmte_id", how="left",
)
rows_22y["sign"] = "positive"
rows_22y.loc[rows_22y["transaction_amt"] < 0, "sign"] = "negative"
rows_22y.loc[rows_22y["transaction_amt"] == 0, "sign"] = "zero"

ct_sign = (
    rows_22y.groupby(["cmte_tp", "sign"])
    .agg(rows=("transaction_amt", "size"), total=("transaction_amt", "sum"))
    .reset_index()
    .sort_values(["cmte_tp", "sign"])
)
print(ct_sign.to_string(index=False))

# ── Case study: donors who have BOTH positive and negative 22Y ──────
print(f"\n{'='*70}")
print("DONORS WITH BOTH POSITIVE AND NEGATIVE 22Y")
print(f"{'='*70}")
donor_22y = (
    rows_22y.groupby("name")
    .agg(
        n_pos=("sign", lambda x: (x == "positive").sum()),
        n_neg=("sign", lambda x: (x == "negative").sum()),
        sum_pos=("transaction_amt", lambda x: x[x > 0].sum()),
        sum_neg=("transaction_amt", lambda x: x[x < 0].sum()),
    )
    .reset_index()
)
both = donor_22y[(donor_22y["n_pos"] > 0) & (donor_22y["n_neg"] > 0)]
print(f"Donors with both positive and negative 22Y: {len(both):,}")
print(f"\nTop 20 by combined volume:")
both["total_vol"] = both["sum_pos"] + both["sum_neg"].abs()
print(both.nlargest(20, "total_vol")[["name", "n_pos", "sum_pos", "n_neg", "sum_neg"]].to_string(index=False))

# ── Case study: pick a few big positive 22Y and see if we can match
# them to original contributions from the same donor to same committee ─
print(f"\n{'='*70}")
print("MATCHING POSITIVE 22Y TO ORIGINAL CONTRIBUTIONS")
print(f"{'='*70}")

big_pos_22y = rows_22y[
    (rows_22y["transaction_amt"] > 50000) &
    (rows_22y["sign"] == "positive")
].nlargest(10, "transaction_amt")

for _, row in big_pos_22y.iterrows():
    donor = row["name"]
    cmte = row["cmte_id"]
    amt = row["transaction_amt"]

    # Find all rows from this donor to this committee
    matching = itcont[
        (itcont["name"] == donor) &
        (itcont["cmte_id"] == cmte)
    ].sort_values("transaction_dt")

    print(f"\n--- {donor} -> {row['cmte_nm']} ({cmte}), 22Y amount: ${amt:,.2f} ---")
    print(matching[["transaction_tp", "memo_cd", "memo_text", "transaction_amt",
                    "transaction_dt"]].to_string(index=False))

# Save
rows_22y_summary = rows_22y.groupby(["sign", "cmte_tp", "memo_cd"]).agg(
    rows=("transaction_amt", "size"),
    total=("transaction_amt", "sum"),
).reset_index()
rows_22y_summary.to_csv(OUT_DIR / "22y_sign_analysis_2024.csv", index=False)
print(f"\nSaved to {OUT_DIR}/")
