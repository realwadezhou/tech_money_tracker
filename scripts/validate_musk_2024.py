"""
Validate FEC counting rules against Elon Musk's 2024 contributions.

Approach: load ALL rows mentioning Musk with ZERO filters on transaction
type, memo code, or committee type. Then inspect what's there.

Public benchmarks:
- OpenSecrets "2024 Top Donors to Outside Spending Groups": $290,419,587
- Earlier (untrusted) local work found ~$277M in itcont alone

FEC bulk file column mappings per fec.gov data dictionary:
  itcont.txt / itoth.txt (21 columns, pipe-delimited, no header):
    0  CMTE_ID             - Filer committee ID
    1  AMNDT_IND           - Amendment indicator
    2  RPT_TP              - Report type
    3  TRANSACTION_PGI     - Primary/general indicator
    4  IMAGE_NUM           - Image number
    5  TRANSACTION_TP      - Transaction type
    6  ENTITY_TP           - Entity type
    7  NAME                - Contributor name
    8  CITY                - City
    9  STATE               - State
    10 ZIP_CODE            - Zip
    11 EMPLOYER            - Employer
    12 OCCUPATION          - Occupation
    13 TRANSACTION_DT      - Transaction date (MMDDYYYY)
    14 TRANSACTION_AMT     - Amount
    15 OTHER_ID            - Other committee ID (for transfers)
    16 TRAN_ID             - Transaction ID
    17 FILE_NUM            - Filing number
    18 MEMO_CD             - Memo code (X = memo entry)
    19 MEMO_TEXT            - Memo text
    20 SUB_ID              - Submission ID

  cm.txt (15 columns, pipe-delimited, no header):
    0  CMTE_ID
    1  CMTE_NM             - Committee name
    2  TRES_NM             - Treasurer name
    3  CMTE_ST1            - Street 1
    4  CMTE_ST2            - Street 2
    5  CMTE_CITY
    6  CMTE_ST             - State
    7  CMTE_ZIP
    8  CMTE_DSGN           - Designation
    9  CMTE_TP             - Committee type
    10 CMTE_PTY_AFFILIATION
    11 CMTE_FILING_FREQ
    12 ORG_TP              - Interest group category
    13 CONNECTED_ORG_NM
    14 CAND_ID
"""

from pathlib import Path
import pandas as pd

DATA_ROOT = Path(__file__).resolve().parent.parent.parent / "spending_tracker" / "data"
OUT_DIR = Path(__file__).resolve().parent.parent / "outputs"
OUT_DIR.mkdir(parents=True, exist_ok=True)

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

# ── Load committee master ────────────────────────────────────────────
print("Loading committee master...")
cm = pd.read_csv(
    DATA_ROOT / "interim" / "fec" / "2024" / "cm24" / "cm.txt",
    sep="|", header=None, names=CM_COLS, dtype="string", na_filter=False,
)

# ── Load itcont ──────────────────────────────────────────────────────
print("Loading itcont (this is ~11GB, will take a minute)...")
itcont = pd.read_csv(
    DATA_ROOT / "interim" / "fec" / "2024" / "indiv24" / "itcont.txt",
    sep="|", header=None, names=ITCONT_COLS, dtype="string", na_filter=False,
)
itcont["transaction_amt"] = pd.to_numeric(itcont["transaction_amt"], errors="coerce").fillna(0.0)
print(f"  itcont rows: {len(itcont):,}")

# ── Load itoth ───────────────────────────────────────────────────────
print("Loading itoth...")
itoth = pd.read_csv(
    DATA_ROOT / "interim" / "fec" / "2024" / "oth24" / "itoth.txt",
    sep="|", header=None, names=ITCONT_COLS, dtype="string", na_filter=False,
)
itoth["transaction_amt"] = pd.to_numeric(itoth["transaction_amt"], errors="coerce").fillna(0.0)
print(f"  itoth rows: {len(itoth):,}")

# ── Find every Musk row across both files ────────────────────────────
# Use broad name match — we want to catch everything, including trusts
musk_itcont = itcont[itcont["name"].str.contains("MUSK, ELON|ELON MUSK", case=False, na=False)].copy()
musk_itoth = itoth[itoth["name"].str.contains("MUSK, ELON|ELON MUSK", case=False, na=False)].copy()

musk_itcont["source_file"] = "itcont"
musk_itoth["source_file"] = "itoth"

musk_all = pd.concat([musk_itcont, musk_itoth], ignore_index=True)

# Join committee info for the recipient (cmte_id = filer/recipient committee)
musk_all = musk_all.merge(
    cm[["cmte_id", "cmte_nm", "cmte_tp", "cmte_dsgn", "connected_org_nm", "cand_id"]],
    on="cmte_id", how="left",
)

print(f"\n{'='*70}")
print(f"MUSK ROWS FOUND (zero filters)")
print(f"{'='*70}")
print(f"  itcont: {len(musk_itcont):,} rows, ${musk_itcont['transaction_amt'].sum():,.2f}")
print(f"  itoth:  {len(musk_itoth):,} rows, ${musk_itoth['transaction_amt'].sum():,.2f}")
print(f"  total:  {len(musk_all):,} rows, ${musk_all['transaction_amt'].sum():,.2f}")

# ── Break down by transaction type ───────────────────────────────────
print(f"\n{'='*70}")
print("BREAKDOWN BY TRANSACTION TYPE")
print(f"{'='*70}")
tt_summary = (
    musk_all.groupby(["source_file", "transaction_tp", "memo_cd"])
    .agg(
        rows=("transaction_amt", "size"),
        total=("transaction_amt", "sum"),
    )
    .reset_index()
    .sort_values("total", ascending=False)
)
print(tt_summary.to_string(index=False))

# ── Break down by recipient committee ────────────────────────────────
print(f"\n{'='*70}")
print("BREAKDOWN BY RECIPIENT COMMITTEE")
print(f"{'='*70}")
recip_summary = (
    musk_all.groupby(["source_file", "cmte_id", "cmte_nm", "cmte_tp", "transaction_tp", "memo_cd"])
    .agg(
        rows=("transaction_amt", "size"),
        total=("transaction_amt", "sum"),
    )
    .reset_index()
    .sort_values("total", ascending=False)
)
print(recip_summary.to_string(index=False))

# ── Save full detail for inspection ──────────────────────────────────
detail_cols = [
    "source_file", "name", "employer", "transaction_tp", "entity_tp",
    "memo_cd", "memo_text", "transaction_amt", "transaction_dt",
    "cmte_id", "cmte_nm", "cmte_tp", "cmte_dsgn", "other_id",
    "cand_id", "tran_id", "file_num",
]
musk_all[detail_cols].sort_values("transaction_amt", ascending=False).to_csv(
    OUT_DIR / "musk_2024_all_rows.csv", index=False,
)

tt_summary.to_csv(OUT_DIR / "musk_2024_by_transaction_type.csv", index=False)
recip_summary.to_csv(OUT_DIR / "musk_2024_by_recipient.csv", index=False)

print(f"\nSaved detail to {OUT_DIR / 'musk_2024_all_rows.csv'}")
print(f"Saved transaction type summary to {OUT_DIR / 'musk_2024_by_transaction_type.csv'}")
print(f"Saved recipient summary to {OUT_DIR / 'musk_2024_by_recipient.csv'}")
