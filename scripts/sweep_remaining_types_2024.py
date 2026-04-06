"""
Comprehensive sweep of every remaining transaction type we haven't
fully explored. Goals:

1. Enumerate EVERY unique transaction type across all 3 files
2. Flag any we haven't discussed at all
3. Deep dive the MEDIUM confidence ones with actual examples
4. Check for any transaction types that only appear with memo X
   (could be hiding real money behind memo flags)
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

CM_COLS = [
    "cmte_id", "cmte_nm", "tres_nm", "cmte_st1", "cmte_st2",
    "cmte_city", "cmte_st", "cmte_zip", "cmte_dsgn", "cmte_tp",
    "cmte_pty_affiliation", "cmte_filing_freq", "org_tp", "connected_org_nm",
    "cand_id",
]

print("Loading all files...")
cm = pd.read_csv(
    DATA_ROOT / "interim" / "fec" / "2024" / "cm24" / "cm.txt",
    sep="|", header=None, names=CM_COLS, dtype="string", na_filter=False,
)
itcont = pd.read_csv(
    DATA_ROOT / "interim" / "fec" / "2024" / "indiv24" / "itcont.txt",
    sep="|", header=None, names=ITCONT_COLS, dtype="string", na_filter=False,
)
itcont["transaction_amt"] = pd.to_numeric(itcont["transaction_amt"], errors="coerce").fillna(0.0)

itoth = pd.read_csv(
    DATA_ROOT / "interim" / "fec" / "2024" / "oth24" / "itoth.txt",
    sep="|", header=None, names=ITCONT_COLS, dtype="string", na_filter=False,
)
itoth["transaction_amt"] = pd.to_numeric(itoth["transaction_amt"], errors="coerce").fillna(0.0)

itpas2 = pd.read_csv(
    DATA_ROOT / "interim" / "fec" / "2024" / "pas224" / "itpas2.txt",
    sep="|", header=None, names=ITPAS2_COLS, dtype="string", na_filter=False,
)
itpas2["transaction_amt"] = pd.to_numeric(itpas2["transaction_amt"], errors="coerce").fillna(0.0)


def summarize_type(df, tt, file_label):
    """Full summary of a single transaction type."""
    sub = df[df["transaction_tp"] == tt].copy()
    if len(sub) == 0:
        return
    sub_memo = sub[sub["memo_cd"] == "X"]
    sub_nomemo = sub[sub["memo_cd"] != "X"]

    print(f"\n--- {tt} in {file_label} ---")
    print(f"  Total: {len(sub):,} rows, ${sub['transaction_amt'].sum():,.0f}")
    print(f"  Non-memo: {len(sub_nomemo):,} rows, ${sub_nomemo['transaction_amt'].sum():,.0f}")
    print(f"  Memo X: {len(sub_memo):,} rows, ${sub_memo['transaction_amt'].sum():,.0f}")
    pos = sub[sub["transaction_amt"] > 0]
    neg = sub[sub["transaction_amt"] < 0]
    if len(neg) > 0:
        print(f"  Positive: {len(pos):,} rows, ${pos['transaction_amt'].sum():,.0f}")
        print(f"  Negative: {len(neg):,} rows, ${neg['transaction_amt'].sum():,.0f}")

    # Entity types
    print(f"  Entity types: {sub['entity_tp'].value_counts().head(5).to_dict()}")

    # Recipient committee types
    sub_cm = sub.merge(cm[["cmte_id", "cmte_nm", "cmte_tp"]], on="cmte_id", how="left")
    print(f"  Filer/recipient cmte types: {sub_cm['cmte_tp'].value_counts().head(5).to_dict()}")

    # Top memo texts
    memos = sub["memo_text"].value_counts().head(5)
    if len(memos) > 0:
        print(f"  Top memo texts: {memos.to_dict()}")

    # Sample largest rows
    biggest = sub.nlargest(3, "transaction_amt")
    for _, row in biggest.iterrows():
        cmte_nm = cm.loc[cm["cmte_id"] == row["cmte_id"], "cmte_nm"].values
        cmte_nm = cmte_nm[0] if len(cmte_nm) > 0 else "?"
        print(f"    ${row['transaction_amt']:>12,.0f}  {row['name'][:40]:<40}  -> {cmte_nm[:40]}  memo:{row['memo_cd']}  {row['memo_text'][:50]}")


# ══════════════════════════════════════════════════════════════════════
# 1. Complete type inventory across all files
# ══════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("COMPLETE TRANSACTION TYPE INVENTORY")
print(f"{'='*70}")

for file_label, df in [("itcont", itcont), ("itoth", itoth), ("itpas2", itpas2)]:
    types = (
        df.groupby("transaction_tp")
        .agg(rows=("transaction_amt", "size"), total=("transaction_amt", "sum"))
        .reset_index()
        .sort_values("total", ascending=False)
    )
    print(f"\n{file_label} — {len(types)} unique transaction types:")
    print(types.to_string(index=False))

# ══════════════════════════════════════════════════════════════════════
# 2. Deep dive remaining MEDIUM types
# ══════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("DEEP DIVE: MEDIUM CONFIDENCE TYPES")
print(f"{'='*70}")

# 24I — earmark check pass-through
summarize_type(itcont, "24I", "itcont")
# What does the non-memo X portion look like vs memo X?
t24i = itcont[itcont["transaction_tp"] == "24I"]
t24i_nomemo = t24i[t24i["memo_cd"] != "X"]
t24i_memo = t24i[t24i["memo_cd"] == "X"]
print(f"\n  24I non-memo: who are the donors?")
t24i_donors = t24i_nomemo.groupby("name").agg(rows=("transaction_amt","size"), total=("transaction_amt","sum")).reset_index().nlargest(10,"total")
print(f"    {t24i_donors.to_string(index=False)}")
print(f"\n  24I memo X: who are the donors?")
t24i_m_donors = t24i_memo.groupby("name").agg(rows=("transaction_amt","size"), total=("transaction_amt","sum")).reset_index().nlargest(10,"total")
print(f"    {t24i_m_donors.to_string(index=False)}")

# Is there a matching 15I for 24I?
t15i = itcont[itcont["transaction_tp"] == "15I"]
print(f"\n  15I in itcont: {len(t15i)} rows (15I is the donor side of 24I)")

# 11 — tribal/organizational
summarize_type(itcont, "11", "itcont")

# 20Y — nonfederal
summarize_type(itcont, "20Y", "itcont")
# Check: is the 20Y in itcont actually a disbursement OUT of the committee?
# Or a receipt? The column mapping says cmte_id = receiving committee for itcont.
# But 20Y is a disbursement code. Something is off.
t20y = itcont[itcont["transaction_tp"] == "20Y"]
t20y_cm = t20y.merge(cm[["cmte_id","cmte_nm","cmte_tp"]], on="cmte_id", how="left")
print(f"\n  20Y: is cmte_id the giver or receiver? Check entity_tp:")
print(f"  {t20y['entity_tp'].value_counts().head(5).to_dict()}")
print(f"  20Y: other_id populated: {(t20y['other_id'] != '').sum()} of {len(t20y)}")
# Sample rows to understand direction
print(f"\n  20Y sample rows:")
sample = t20y.merge(cm[["cmte_id","cmte_nm","cmte_tp"]], on="cmte_id", how="left")
for _, row in sample.nlargest(5, "transaction_amt").iterrows():
    print(f"    ${row['transaction_amt']:>12,.0f}  name:{row['name'][:30]:<30}  cmte:{row['cmte_nm'][:35]}({row['cmte_tp']})  entity_tp:{row['entity_tp']}  other_id:{row['other_id']}")

# 30, 31, 32 in itcont — special account contributions
for tt in ["30", "31", "32"]:
    summarize_type(itcont, tt, "itcont")

# 31E, 32E — earmarked variants for special accounts
for tt in ["31E", "32E"]:
    summarize_type(itcont, tt, "itcont")

# 18J in itoth
summarize_type(itoth, "18J", "itoth")

# 24F, 24N
summarize_type(itoth, "24F", "itoth")
summarize_type(itoth, "24N", "itoth")

# ══════════════════════════════════════════════════════════════════════
# 3. Any types that ONLY appear as memo X? (hidden money)
# ══════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("TYPES THAT ONLY APPEAR AS MEMO X")
print(f"{'='*70}")
for file_label, df in [("itcont", itcont), ("itoth", itoth)]:
    types_all = set(df["transaction_tp"].unique())
    types_nomemo = set(df.loc[df["memo_cd"] != "X", "transaction_tp"].unique())
    memo_only = types_all - types_nomemo
    if memo_only:
        print(f"\n{file_label} — types with ONLY memo X rows: {memo_only}")
        for tt in memo_only:
            sub = df[df["transaction_tp"] == tt]
            print(f"  {tt}: {len(sub):,} rows, ${sub['transaction_amt'].sum():,.0f}")
    else:
        print(f"\n{file_label} — no types are memo-X-only")

# ══════════════════════════════════════════════════════════════════════
# 4. The Y suffix types in itcont (15 with Y memo_cd)
# ══════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("Y MEMO CODE IN ITCONT (not X, not blank)")
print(f"{'='*70}")
y_rows = itcont[itcont["memo_cd"] == "Y"]
print(f"Total Y rows: {len(y_rows):,}, ${y_rows['transaction_amt'].sum():,.0f}")
y_by_tt = y_rows.groupby("transaction_tp").agg(
    rows=("transaction_amt","size"), total=("transaction_amt","sum")
).reset_index().sort_values("total", ascending=False)
print(y_by_tt.to_string(index=False))
print(f"\nSample Y rows:")
for _, row in y_rows.nlargest(5, "transaction_amt").iterrows():
    print(f"  ${row['transaction_amt']:>10,.0f}  tt:{row['transaction_tp']}  {row['name'][:35]}  memo:{row['memo_text'][:50]}")

# ══════════════════════════════════════════════════════════════════════
# 5. Check for any other_id patterns we're missing
# Specifically: are there itcont rows where other_id is populated
# on types other than 24T and 24I?
# ══════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("ITCONT ROWS WITH OTHER_ID POPULATED (non-24T, non-24I)")
print(f"{'='*70}")
has_other = itcont[(itcont["other_id"] != "") & (~itcont["transaction_tp"].isin(["24T", "24I"]))]
print(f"Total: {len(has_other):,} rows, ${has_other['transaction_amt'].sum():,.0f}")
other_by_tt = has_other.groupby("transaction_tp").agg(
    rows=("transaction_amt","size"), total=("transaction_amt","sum")
).reset_index().sort_values("total", ascending=False)
print(other_by_tt.to_string(index=False))

# ══════════════════════════════════════════════════════════════════════
# 6. 22H in itoth — what is this?
# ══════════════════════════════════════════════════════════════════════
summarize_type(itoth, "22H", "itoth")

# 10J in itoth
summarize_type(itoth, "10J", "itoth")

# 29 in itoth — electioneering communication
summarize_type(itoth, "29", "itoth")

# 24R — election recount
summarize_type(itoth, "24R", "itoth")

# 42Z in itoth
summarize_type(itoth, "42Z", "itoth")

print(f"\n{'='*70}")
print("SWEEP COMPLETE")
print(f"{'='*70}")
