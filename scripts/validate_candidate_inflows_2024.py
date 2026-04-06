"""
Explore how money arrives at candidate committees in 2024 FEC data.

Instead of starting from a donor, start from a RECIPIENT candidate committee
and find every row across itcont, itoth, and itpas2 where money flows in.

This answers: what files and transaction types do we need to capture all the
ways money reaches a candidate?

We'll test with a few committees:
- A big presidential (HARRIS FOR PRESIDENT C00703975)
- A Senate race (pick a competitive one)
- A House race

itpas2.txt schema (22 columns, pipe-delimited, no header):
    0  CMTE_ID             - Giving committee ID (the PAC/committee making the contribution)
    1  AMNDT_IND
    2  RPT_TP
    3  TRANSACTION_PGI
    4  IMAGE_NUM
    5  TRANSACTION_TP
    6  ENTITY_TP
    7  NAME                - Recipient name (candidate committee name)
    8  CITY
    9  STATE
    10 ZIP_CODE
    11 EMPLOYER
    12 OCCUPATION
    13 TRANSACTION_DT
    14 TRANSACTION_AMT
    15 OTHER_ID            - Recipient committee ID
    16 CAND_ID             - Candidate ID
    17 TRAN_ID
    18 FILE_NUM
    19 MEMO_CD
    20 MEMO_TEXT
    21 SUB_ID
"""

from pathlib import Path
import pandas as pd

DATA_ROOT = Path(__file__).resolve().parent.parent / "data"
OUT_DIR = Path(__file__).resolve().parent.parent / "outputs"
OUT_DIR.mkdir(parents=True, exist_ok=True)

ITCONT_COLS = [
    "cmte_id", "amndt_ind", "rpt_tp", "transaction_pgi", "image_num",
    "transaction_tp", "entity_tp", "name", "city", "state",
    "zip_code", "employer", "occupation", "transaction_dt", "transaction_amt",
    "other_id", "tran_id", "file_num", "memo_cd", "memo_text", "sub_id",
]

# itpas2 has same layout but with cand_id inserted after other_id
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

# ── Target committees ────────────────────────────────────────────────
# We look at inflows from all three files for each target
TARGETS = {
    "HARRIS FOR PRESIDENT": "C00703975",
    # We'll also pick a competitive Senate race — let's find one below
}

# ── Load committee master ────────────────────────────────────────────
print("Loading committee master...")
cm = pd.read_csv(
    DATA_ROOT / "interim" / "fec" / "2024" / "cm24" / "cm.txt",
    sep="|", header=None, names=CM_COLS, dtype="string", na_filter=False,
)

# ── Load itpas2 (small file, ~118MB) ─────────────────────────────────
print("Loading itpas2...")
itpas2 = pd.read_csv(
    DATA_ROOT / "interim" / "fec" / "2024" / "pas224" / "itpas2.txt",
    sep="|", header=None, names=ITPAS2_COLS, dtype="string", na_filter=False,
)
itpas2["transaction_amt"] = pd.to_numeric(itpas2["transaction_amt"], errors="coerce").fillna(0.0)
print(f"  itpas2 rows: {len(itpas2):,}")

# First, let's understand itpas2 globally before filtering
print(f"\n{'='*70}")
print("ITPAS2 GLOBAL OVERVIEW")
print(f"{'='*70}")
print("\nTransaction types:")
print(itpas2["transaction_tp"].value_counts().head(20).to_string())
print("\nMemo code distribution:")
print(itpas2["memo_cd"].value_counts(dropna=False).to_string())
print(f"\nTotal dollar amount: ${itpas2['transaction_amt'].sum():,.2f}")
print(f"\nEntity types (who is giving):")
print(itpas2["entity_tp"].value_counts().head(10).to_string())

# ── Now look at who gives to Harris via itpas2 ──────────────────────
# In itpas2, cmte_id = giving committee, other_id = receiving committee
harris_id = "C00703975"

harris_pas2 = itpas2[itpas2["other_id"] == harris_id].copy()
print(f"\n{'='*70}")
print(f"ITPAS2 ROWS INTO HARRIS FOR PRESIDENT ({harris_id})")
print(f"{'='*70}")
print(f"Rows: {len(harris_pas2):,}")
print(f"Total: ${harris_pas2['transaction_amt'].sum():,.2f}")
print(f"\nBy transaction type and memo code:")
harris_pas2_tt = (
    harris_pas2.groupby(["transaction_tp", "memo_cd"])
    .agg(rows=("transaction_amt", "size"), total=("transaction_amt", "sum"))
    .reset_index()
    .sort_values("total", ascending=False)
)
print(harris_pas2_tt.to_string(index=False))

# Add giving committee names
harris_pas2 = harris_pas2.merge(
    cm[["cmte_id", "cmte_nm", "cmte_tp", "connected_org_nm"]].rename(
        columns={"cmte_nm": "giving_cmte_nm", "cmte_tp": "giving_cmte_tp",
                 "connected_org_nm": "giving_connected_org"}
    ),
    on="cmte_id", how="left",
)
print(f"\nTop giving committees:")
harris_pas2_givers = (
    harris_pas2.groupby(["cmte_id", "giving_cmte_nm", "giving_cmte_tp", "transaction_tp", "memo_cd"])
    .agg(rows=("transaction_amt", "size"), total=("transaction_amt", "sum"))
    .reset_index()
    .sort_values("total", ascending=False)
    .head(30)
)
print(harris_pas2_givers.to_string(index=False))

# ── Load itoth and check inflows to Harris ───────────────────────────
print(f"\n{'='*70}")
print("Loading itoth...")
itoth = pd.read_csv(
    DATA_ROOT / "interim" / "fec" / "2024" / "oth24" / "itoth.txt",
    sep="|", header=None, names=ITCONT_COLS, dtype="string", na_filter=False,
)
itoth["transaction_amt"] = pd.to_numeric(itoth["transaction_amt"], errors="coerce").fillna(0.0)
print(f"  itoth rows: {len(itoth):,}")

# In itoth, cmte_id = the committee that FILED the report (the filer).
# For inflows to Harris, we need rows where Harris is the RECIPIENT.
# But itoth is filed by the GIVING committee — so cmte_id is the giver,
# and the recipient could be in other_id OR cmte_id depending on perspective.
#
# Actually, let's think about this more carefully. itoth is "other committee
# transactions" — it can be filed by EITHER side. Let's check both:
# 1. cmte_id == harris_id (Harris filed, showing inflows)
# 2. other_id == harris_id (another committee filed, showing outflow to Harris)

harris_itoth_filed = itoth[itoth["cmte_id"] == harris_id].copy()
harris_itoth_received = itoth[itoth["other_id"] == harris_id].copy()

print(f"\n{'='*70}")
print(f"ITOTH ROWS WHERE HARRIS IS FILER (cmte_id = {harris_id})")
print(f"{'='*70}")
print(f"Rows: {len(harris_itoth_filed):,}")
print(f"Total: ${harris_itoth_filed['transaction_amt'].sum():,.2f}")
print(f"\nBy transaction type and memo code:")
tt = (
    harris_itoth_filed.groupby(["transaction_tp", "memo_cd"])
    .agg(rows=("transaction_amt", "size"), total=("transaction_amt", "sum"))
    .reset_index()
    .sort_values("total", ascending=False)
)
print(tt.to_string(index=False))

print(f"\n{'='*70}")
print(f"ITOTH ROWS WHERE HARRIS IS OTHER_ID (other_id = {harris_id})")
print(f"{'='*70}")
print(f"Rows: {len(harris_itoth_received):,}")
print(f"Total: ${harris_itoth_received['transaction_amt'].sum():,.2f}")
print(f"\nBy transaction type and memo code:")
tt2 = (
    harris_itoth_received.groupby(["transaction_tp", "memo_cd"])
    .agg(rows=("transaction_amt", "size"), total=("transaction_amt", "sum"))
    .reset_index()
    .sort_values("total", ascending=False)
)
print(tt2.to_string(index=False))

# ── Load itcont and check inflows to Harris ──────────────────────────
print(f"\n{'='*70}")
print("Loading itcont...")
itcont = pd.read_csv(
    DATA_ROOT / "interim" / "fec" / "2024" / "indiv24" / "itcont.txt",
    sep="|", header=None, names=ITCONT_COLS, dtype="string", na_filter=False,
)
itcont["transaction_amt"] = pd.to_numeric(itcont["transaction_amt"], errors="coerce").fillna(0.0)
print(f"  itcont rows: {len(itcont):,}")

# In itcont, cmte_id = the committee receiving the individual contribution
harris_itcont = itcont[itcont["cmte_id"] == harris_id].copy()

print(f"\n{'='*70}")
print(f"ITCONT ROWS INTO HARRIS FOR PRESIDENT ({harris_id})")
print(f"{'='*70}")
print(f"Rows: {len(harris_itcont):,}")
print(f"Total: ${harris_itcont['transaction_amt'].sum():,.2f}")
print(f"\nBy transaction type and memo code:")
tt3 = (
    harris_itcont.groupby(["transaction_tp", "memo_cd"])
    .agg(rows=("transaction_amt", "size"), total=("transaction_amt", "sum"))
    .reset_index()
    .sort_values("total", ascending=False)
)
print(tt3.to_string(index=False))

# ── Also check itcont rows where other_id points to Harris ───────────
# (these would be earmarked contributions routed TO Harris)
harris_itcont_other = itcont[itcont["other_id"] == harris_id].copy()

print(f"\n{'='*70}")
print(f"ITCONT ROWS WHERE OTHER_ID = {harris_id} (earmarks routed to Harris)")
print(f"{'='*70}")
print(f"Rows: {len(harris_itcont_other):,}")
print(f"Total: ${harris_itcont_other['transaction_amt'].sum():,.2f}")
print(f"\nBy transaction type and memo code:")
tt4 = (
    harris_itcont_other.groupby(["transaction_tp", "memo_cd"])
    .agg(rows=("transaction_amt", "size"), total=("transaction_amt", "sum"))
    .reset_index()
    .sort_values("total", ascending=False)
)
print(tt4.to_string(index=False))

# ── Summary ──────────────────────────────────────────────────────────
print(f"\n{'='*70}")
print("SUMMARY: ALL INFLOW PATHS TO HARRIS FOR PRESIDENT")
print(f"{'='*70}")
print(f"  itcont (cmte_id = Harris):     ${harris_itcont['transaction_amt'].sum():>20,.2f}  ({len(harris_itcont):>10,} rows)")
print(f"  itcont (other_id = Harris):     ${harris_itcont_other['transaction_amt'].sum():>20,.2f}  ({len(harris_itcont_other):>10,} rows)")
print(f"  itoth  (cmte_id = Harris):      ${harris_itoth_filed['transaction_amt'].sum():>20,.2f}  ({len(harris_itoth_filed):>10,} rows)")
print(f"  itoth  (other_id = Harris):     ${harris_itoth_received['transaction_amt'].sum():>20,.2f}  ({len(harris_itoth_received):>10,} rows)")
print(f"  itpas2 (other_id = Harris):     ${harris_pas2['transaction_amt'].sum():>20,.2f}  ({len(harris_pas2):>10,} rows)")
print(f"\n  FEC official benchmark (individual contributions): ~$613.6M")
print(f"  FEC official benchmark (total receipts):           ~$1.176B")

# Save all summaries
for name, df in [
    ("harris_itcont_inflows", tt3),
    ("harris_itcont_earmarks_to", tt4),
    ("harris_itoth_filed", tt),
    ("harris_itoth_received", tt2),
    ("harris_itpas2_inflows", harris_pas2_tt),
    ("harris_itpas2_top_givers", harris_pas2_givers),
]:
    df.to_csv(OUT_DIR / f"{name}_2024.csv", index=False)

print(f"\nSaved summaries to {OUT_DIR}/")
