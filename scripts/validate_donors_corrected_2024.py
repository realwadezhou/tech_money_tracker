"""
Re-validate donor totals with corrected 22Y refund logic.

Rule:
  net_donated = sum(non-22Y rows as-is) - sum(22Y amounts)

Since positive 22Y = refund issued (subtract from donor),
and negative 22Y = refund reversal (add back to donor),
we subtract the raw 22Y sum in both cases.

Test donors:
  - MUSK, ELON (benchmark: OpenSecrets ~$290M outside spending)
  - ANDREESSEN, MARC (should reconcile per earlier testing)
  - HOFFMAN, REID (has known 22Y and negative-15 activity)
  - SHANAHAN, NICOLE (has large 22Y, was RFK VP pick)
  - GRIFFIN, KENNETH (should reconcile per earlier testing)
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

CM_COLS = [
    "cmte_id", "cmte_nm", "tres_nm", "cmte_st1", "cmte_st2",
    "cmte_city", "cmte_st", "cmte_zip", "cmte_dsgn", "cmte_tp",
    "cmte_pty_affiliation", "cmte_filing_freq", "org_tp", "connected_org_nm",
    "cand_id",
]

# Donor search patterns — broad to catch trusts, entities, name variants
DONORS = {
    "Elon Musk": ["MUSK, ELON", "ELON MUSK"],
    "Marc Andreessen": ["ANDREESSEN, MARC"],
    "Reid Hoffman": ["HOFFMAN, REID"],
    "Nicole Shanahan": ["SHANAHAN, NICOLE"],
    "Ken Griffin": ["GRIFFIN, KENNETH", "GRIFFIN, KEN"],
    "Timothy Mellon": ["MELLON, TIMOTHY"],
}

print("Loading data...")
cm = pd.read_csv(
    DATA_ROOT / "interim" / "fec" / "2024" / "cm24" / "cm.txt",
    sep="|", header=None, names=CM_COLS, dtype="string", na_filter=False,
)

itcont = pd.read_csv(
    DATA_ROOT / "interim" / "fec" / "2024" / "indiv24" / "itcont.txt",
    sep="|", header=None, names=ITCONT_COLS, dtype="string", na_filter=False,
)
itcont["transaction_amt"] = pd.to_numeric(itcont["transaction_amt"], errors="coerce").fillna(0.0)
print(f"  itcont: {len(itcont):,} rows")

# Build name lookup for committee types
cm_lookup = cm.set_index("cmte_id")[["cmte_nm", "cmte_tp"]].to_dict("index")

def get_cmte_info(cmte_id):
    info = cm_lookup.get(cmte_id, {})
    return info.get("cmte_nm", ""), info.get("cmte_tp", "")

# Outside-spending committee types
OUTSIDE_TYPES = {"O", "U", "V", "W"}

for donor_label, name_patterns in DONORS.items():
    # Build regex pattern
    pattern = "|".join(name_patterns)
    rows = itcont[itcont["name"].str.contains(pattern, case=False, na=False)].copy()

    if len(rows) == 0:
        print(f"\n{'='*70}")
        print(f"{donor_label}: NO ROWS FOUND")
        continue

    # Add committee info
    rows["cmte_nm"] = rows["cmte_id"].map(lambda x: get_cmte_info(x)[0])
    rows["cmte_tp"] = rows["cmte_id"].map(lambda x: get_cmte_info(x)[1])
    rows["is_outside"] = rows["cmte_tp"].isin(OUTSIDE_TYPES)

    # Split 22Y from contributions
    is_22y = rows["transaction_tp"] == "22Y"
    contrib_rows = rows[~is_22y]
    refund_rows = rows[is_22y]

    # Compute totals
    gross_contributions = contrib_rows["transaction_amt"].sum()
    refund_total = refund_rows["transaction_amt"].sum()  # positive = money returned
    net_total = gross_contributions - refund_total

    # Outside-spending subset
    outside_contrib = contrib_rows[contrib_rows["is_outside"]]["transaction_amt"].sum()
    outside_refund = refund_rows[refund_rows["is_outside"]]["transaction_amt"].sum()
    outside_net = outside_contrib - outside_refund

    # Non-outside subset
    non_outside_contrib = contrib_rows[~contrib_rows["is_outside"]]["transaction_amt"].sum()
    non_outside_refund = refund_rows[~refund_rows["is_outside"]]["transaction_amt"].sum()
    non_outside_net = non_outside_contrib - non_outside_refund

    print(f"\n{'='*70}")
    print(f"{donor_label}")
    print(f"{'='*70}")
    print(f"  Names matched: {rows['name'].unique().tolist()}")
    print(f"  Total rows: {len(rows):,}")
    print(f"")
    print(f"  ALL COMMITTEES:")
    print(f"    Gross contributions (non-22Y): ${gross_contributions:>18,.2f}")
    print(f"    22Y refund amounts:            ${refund_total:>18,.2f}")
    print(f"    Net total:                     ${net_total:>18,.2f}")
    print(f"")
    print(f"  OUTSIDE SPENDING ONLY (cmte_tp in O,U,V,W):")
    print(f"    Gross contributions (non-22Y): ${outside_contrib:>18,.2f}")
    print(f"    22Y refund amounts:            ${outside_refund:>18,.2f}")
    print(f"    Net outside:                   ${outside_net:>18,.2f}")
    print(f"")
    print(f"  NON-OUTSIDE:")
    print(f"    Gross contributions (non-22Y): ${non_outside_contrib:>18,.2f}")
    print(f"    22Y refund amounts:            ${non_outside_refund:>18,.2f}")
    print(f"    Net non-outside:               ${non_outside_net:>18,.2f}")

    # Breakdown by transaction type
    print(f"\n  By transaction type:")
    tt = (
        rows.groupby(["transaction_tp", "memo_cd", "is_outside"])
        .agg(rows=("transaction_amt", "size"), total=("transaction_amt", "sum"))
        .reset_index()
        .sort_values("total", ascending=False)
    )
    print("  " + tt.to_string(index=False).replace("\n", "\n  "))

    # Breakdown by recipient
    print(f"\n  Top recipients:")
    recip = (
        rows.groupby(["cmte_id", "cmte_nm", "cmte_tp", "is_outside"])
        .agg(rows=("transaction_amt", "size"), total=("transaction_amt", "sum"))
        .reset_index()
        .sort_values("total", ascending=False)
        .head(15)
    )
    print("  " + recip.to_string(index=False).replace("\n", "\n  "))

    # Save detail
    safe_name = donor_label.lower().replace(" ", "_")
    rows.to_csv(OUT_DIR / f"donor_detail_{safe_name}_2024.csv", index=False)

print(f"\nDone. Detail files saved to {OUT_DIR}/")
