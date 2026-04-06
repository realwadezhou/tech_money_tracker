"""
Deep investigation of remaining MEDIUM confidence transaction types.
Goal: get each to HIGH confidence with a clear include/exclude decision.

Remaining mediums:
  itcont: 20Y ($38M), 24I ($19M)
  itoth:  22Z ($14.5M), 18U ($22M), 20F ($41M), 20C ($96M),
          16G ($2.2M), 42 ($134M), 41 ($27M)
  itpas2: 24C ($90M — also in itoth)
  cross:  24Z ($5.4M itoth, $1.7M itpas2), 15Z ($2.8M itoth)
"""

from pathlib import Path
import pandas as pd

DATA_ROOT = Path(__file__).resolve().parent.parent.parent / "spending_tracker" / "data"

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


def deep_dive(df, tt, file_label, cm_df):
    """Exhaustive analysis of a transaction type."""
    sub = df[df["transaction_tp"] == tt].copy()
    if len(sub) == 0:
        print(f"\n{'='*60}")
        print(f"  {tt} in {file_label}: NO ROWS")
        return

    sub_cm = sub.merge(cm_df[["cmte_id", "cmte_nm", "cmte_tp", "cmte_dsgn"]], on="cmte_id", how="left")

    # Also look up other_id as a committee
    sub_cm = sub_cm.merge(
        cm_df[["cmte_id", "cmte_nm", "cmte_tp"]].rename(
            columns={"cmte_id": "other_id", "cmte_nm": "other_nm", "cmte_tp": "other_tp"}
        ),
        on="other_id", how="left"
    )

    sub_memo = sub[sub["memo_cd"] == "X"]
    sub_nomemo = sub[sub["memo_cd"] != "X"]
    pos = sub[sub["transaction_amt"] > 0]
    neg = sub[sub["transaction_amt"] < 0]

    print(f"\n{'='*60}")
    print(f"  {tt} in {file_label}")
    print(f"{'='*60}")
    print(f"  Total: {len(sub):,} rows, ${sub['transaction_amt'].sum():,.0f}")
    print(f"  Non-memo: {len(sub_nomemo):,} rows, ${sub_nomemo['transaction_amt'].sum():,.0f}")
    print(f"  Memo X:   {len(sub_memo):,} rows, ${sub_memo['transaction_amt'].sum():,.0f}")
    if len(neg) > 0:
        print(f"  Positive: {len(pos):,} rows, ${pos['transaction_amt'].sum():,.0f}")
        print(f"  Negative: {len(neg):,} rows, ${neg['transaction_amt'].sum():,.0f}")

    print(f"\n  Entity types: {sub['entity_tp'].value_counts().to_dict()}")
    print(f"\n  Filer cmte_tp: {sub_cm['cmte_tp'].value_counts().head(8).to_dict()}")
    print(f"  Filer cmte_dsgn: {sub_cm['cmte_dsgn'].value_counts().head(8).to_dict()}")

    # other_id analysis
    has_other = sub_cm[sub_cm["other_id"] != ""]
    no_other = sub_cm[sub_cm["other_id"] == ""]
    print(f"\n  other_id populated: {len(has_other):,} of {len(sub):,}")
    if len(has_other) > 0:
        print(f"  other_id cmte_tp: {has_other['other_tp'].value_counts().head(5).to_dict()}")

    # Memo text patterns
    print(f"\n  Top memo texts:")
    for memo, cnt in sub["memo_text"].value_counts().head(8).items():
        print(f"    {cnt:>6,}  {memo[:80]}")

    # Largest transactions
    print(f"\n  Largest transactions:")
    for _, row in sub_cm.nlargest(8, "transaction_amt").iterrows():
        other_nm = row.get("other_nm", "")
        if pd.isna(other_nm): other_nm = ""
        print(f"    ${row['transaction_amt']:>12,.0f}  {row['name'][:35]:<35}  -> {str(row.get('cmte_nm',''))[:35]:<35}  other:{other_nm[:30]}  memo:{row['memo_cd']}  {row['memo_text'][:40]}")

    # Smallest (most negative)
    if len(neg) > 0:
        print(f"\n  Most negative transactions:")
        for _, row in sub_cm.nsmallest(5, "transaction_amt").iterrows():
            other_nm = row.get("other_nm", "")
            if pd.isna(other_nm): other_nm = ""
            print(f"    ${row['transaction_amt']:>12,.0f}  {row['name'][:35]:<35}  -> {str(row.get('cmte_nm',''))[:35]:<35}  memo:{row['memo_cd']}  {row['memo_text'][:40]}")

    return sub_cm


# ══════════════════════════════════════════════════════════════════════
# 20Y — Nonfederal account receipts in itcont
# Key question: is this double-counting with itoth transfers?
# ══════════════════════════════════════════════════════════════════════
print("\n" + "#"*60)
print("20Y INVESTIGATION")
print("#"*60)
sub = deep_dive(itcont, "20Y", "itcont", cm)

# Check if 20Y donors appear in other types too
t20y = itcont[itcont["transaction_tp"] == "20Y"]
t20y_big = t20y.nlargest(20, "transaction_amt")
for _, row in t20y_big.head(10).iterrows():
    donor = row["name"]
    cmte = row["cmte_id"]
    # Does this donor+cmte combo appear in other transaction types?
    same_donor_cmte = itcont[(itcont["name"] == donor) & (itcont["cmte_id"] == cmte) & (itcont["transaction_tp"] != "20Y")]
    if len(same_donor_cmte) > 0:
        types = same_donor_cmte["transaction_tp"].value_counts().to_dict()
        tot = same_donor_cmte["transaction_amt"].sum()
        print(f"  20Y donor '{donor}' also in cmte {cmte} as: {types} (${tot:,.0f})")

# Also check: do the 20Y cmte_ids appear in itoth as senders?
t20y_cmtes = t20y["cmte_id"].unique()
itoth_from_20y_cmtes = itoth[itoth["cmte_id"].isin(t20y_cmtes)]
print(f"\n  20Y receiving committees also file itoth: {len(itoth_from_20y_cmtes):,} rows")
print(f"  Their itoth transaction types: {itoth_from_20y_cmtes['transaction_tp'].value_counts().head(10).to_dict()}")


# ══════════════════════════════════════════════════════════════════════
# 24I — Earmark check pass-through
# Key question: is the 24I money already counted under 15E?
# ══════════════════════════════════════════════════════════════════════
print("\n" + "#"*60)
print("24I INVESTIGATION — overlap with 15E")
print("#"*60)

# 24I: committee passes through an earmarked check
# The FEC says: 24I is reported BY the conduit (intermediary)
# 15E is reported BY the recipient committee
# So: donor gives to conduit, conduit files 24I, recipient files 15E
# They should be the same money.

# Let's test: take a 24I row and see if there's a matching 15E
t24i = itcont[itcont["transaction_tp"] == "24I"]
t24i_nomemo = t24i[t24i["memo_cd"] != "X"]

# Get the top non-memo 24I donations (these are the "real" ones)
print("\nTop 24I non-memo rows with other_id:")
t24i_top = t24i_nomemo.nlargest(20, "transaction_amt")
for _, row in t24i_top.iterrows():
    cmte_nm = cm.loc[cm["cmte_id"] == row["cmte_id"], "cmte_nm"].values
    cmte_nm = cmte_nm[0] if len(cmte_nm) > 0 else "?"
    print(f"  ${row['transaction_amt']:>10,.0f}  {row['name'][:35]:<35}  -> {cmte_nm[:35]}  other_id:{row['other_id']}  memo:{row['memo_text'][:40]}")

# Check if 24I non-memo amounts by name match 15E amounts
t15e = itcont[itcont["transaction_tp"] == "15E"]
print(f"\nOverlap test: 24I non-memo donors who also appear in 15E")
t24i_donors = set(t24i_nomemo["name"].unique())
t15e_donors = set(t15e["name"].unique())
overlap = t24i_donors & t15e_donors
print(f"  24I non-memo unique donors: {len(t24i_donors):,}")
print(f"  15E unique donors: {len(t15e_donors):,}")
print(f"  Overlap: {len(overlap):,}")


# ══════════════════════════════════════════════════════════════════════
# 22Z — Committee refund
# ══════════════════════════════════════════════════════════════════════
print("\n" + "#"*60)
print("22Z INVESTIGATION")
print("#"*60)
deep_dive(itoth, "22Z", "itoth", cm)


# ══════════════════════════════════════════════════════════════════════
# 18U — Transfer from non-federal account
# ══════════════════════════════════════════════════════════════════════
print("\n" + "#"*60)
print("18U INVESTIGATION")
print("#"*60)
deep_dive(itoth, "18U", "itoth", cm)


# ══════════════════════════════════════════════════════════════════════
# 20F — Nonfederal fund refund
# ══════════════════════════════════════════════════════════════════════
print("\n" + "#"*60)
print("20F INVESTIGATION")
print("#"*60)
deep_dive(itoth, "20F", "itoth", cm)


# ══════════════════════════════════════════════════════════════════════
# 20C — Other disbursement
# ══════════════════════════════════════════════════════════════════════
print("\n" + "#"*60)
print("20C INVESTIGATION")
print("#"*60)
deep_dive(itoth, "20C", "itoth", cm)


# ══════════════════════════════════════════════════════════════════════
# 16G — Transfer from nonfederal (Levin)
# ══════════════════════════════════════════════════════════════════════
print("\n" + "#"*60)
print("16G INVESTIGATION")
print("#"*60)
deep_dive(itoth, "16G", "itoth", cm)


# ══════════════════════════════════════════════════════════════════════
# 42 — Convention account receipt
# ══════════════════════════════════════════════════════════════════════
print("\n" + "#"*60)
print("42 INVESTIGATION")
print("#"*60)
sub42 = deep_dive(itoth, "42", "itoth", cm)

# Check overlap: does 42 in itoth overlap with type 30 in itcont?
# (type 30 = convention account individual contribution)
print("\n  Cross-check: type 30 donors in itcont vs type 42 senders in itoth")
t30 = itcont[itcont["transaction_tp"] == "30"]
t30_donors = set(t30["name"].unique())
t42 = itoth[itoth["transaction_tp"] == "42"]
t42_donors = set(t42["name"].unique())
overlap = t30_donors & t42_donors
print(f"  Type 30 (itcont) unique donors: {len(t30_donors)}")
print(f"  Type 42 (itoth) unique donors: {len(t42_donors)}")
print(f"  Overlap: {len(overlap)}")
if overlap:
    for d in list(overlap)[:5]:
        t30_amt = t30[t30["name"] == d]["transaction_amt"].sum()
        t42_amt = t42[t42["name"] == d]["transaction_amt"].sum()
        print(f"    {d[:40]}: type30=${t30_amt:,.0f}, type42=${t42_amt:,.0f}")


# ══════════════════════════════════════════════════════════════════════
# 41 — Convention account disbursement
# ══════════════════════════════════════════════════════════════════════
print("\n" + "#"*60)
print("41 INVESTIGATION")
print("#"*60)
deep_dive(itoth, "41", "itoth", cm)


# ══════════════════════════════════════════════════════════════════════
# 24C — Coordinated party expenditure
# ══════════════════════════════════════════════════════════════════════
print("\n" + "#"*60)
print("24C INVESTIGATION")
print("#"*60)
deep_dive(itoth, "24C", "itoth", cm)

# Also check itpas2
print("\n--- 24C in itpas2 ---")
t24c_p = itpas2[itpas2["transaction_tp"] == "24C"]
print(f"  Total: {len(t24c_p):,} rows, ${t24c_p['transaction_amt'].sum():,.0f}")
# Do the rows match between itoth and itpas2?
t24c_o = itoth[itoth["transaction_tp"] == "24C"]
print(f"\n  itoth 24C cmte_ids: {t24c_o['cmte_id'].nunique()} unique")
print(f"  itpas2 24C cmte_ids: {t24c_p['cmte_id'].nunique()} unique")
print(f"  itoth 24C total: ${t24c_o['transaction_amt'].sum():,.0f}")
print(f"  itpas2 24C total: ${t24c_p['transaction_amt'].sum():,.0f}")

# Who receives 24C?
if "cand_id" in t24c_p.columns:
    print(f"\n  itpas2 24C: top candidate recipients:")
    top_cand = t24c_p.groupby("cand_id").agg(
        rows=("transaction_amt", "size"), total=("transaction_amt", "sum")
    ).reset_index().nlargest(10, "total")
    for _, row in top_cand.iterrows():
        print(f"    {row['cand_id']}: {row['rows']} rows, ${row['total']:,.0f}")


# ══════════════════════════════════════════════════════════════════════
# 24Z / 15Z — In-kind contributions
# ══════════════════════════════════════════════════════════════════════
print("\n" + "#"*60)
print("IN-KIND INVESTIGATION (24Z, 15Z)")
print("#"*60)
deep_dive(itoth, "24Z", "itoth", cm)
deep_dive(itoth, "15Z", "itoth", cm)

# Check itpas2 for 24Z
print("\n--- 24Z in itpas2 ---")
t24z_p = itpas2[itpas2["transaction_tp"] == "24Z"]
print(f"  Total: {len(t24z_p):,} rows, ${t24z_p['transaction_amt'].sum():,.0f}")
print(f"  Largest:")
t24z_p_cm = t24z_p.merge(cm[["cmte_id","cmte_nm","cmte_tp"]], on="cmte_id", how="left")
for _, row in t24z_p_cm.nlargest(5, "transaction_amt").iterrows():
    print(f"    ${row['transaction_amt']:>10,.0f}  {row['name'][:35]}  -> {row['cmte_nm'][:35]}")


# ══════════════════════════════════════════════════════════════════════
# Final: Types in itoth we haven't looked at at all
# ══════════════════════════════════════════════════════════════════════
print("\n" + "#"*60)
print("REMAINING ITOTH TYPES NOT YET INVESTIGATED")
print("#"*60)

investigated = {"24G", "18G", "24A", "15J", "24E", "24K", "18K", "16C",
                "18J", "31J", "42", "32J", "32G", "20C", "24C", "31G",
                "30J", "20F", "31K", "32K", "30G", "41", "18U", "22Z",
                "31F", "24F", "11J", "24Z", "30K", "32F", "15Z", "16G",
                "30F", "20G", "16F", "20", "10J", "29", "42Z", "24N",
                "24R", "20R", "22H"}

all_itoth_types = set(itoth["transaction_tp"].unique())
remaining = all_itoth_types - investigated
if remaining:
    print(f"  Still uninvestigated: {remaining}")
    for tt in remaining:
        sub = itoth[itoth["transaction_tp"] == tt]
        print(f"    {tt}: {len(sub):,} rows, ${sub['transaction_amt'].sum():,.0f}")
else:
    print("  All itoth types have been investigated!")

print("\n" + "#"*60)
print("INVESTIGATION COMPLETE")
print("#"*60)
