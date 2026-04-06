"""
Stress-test the transaction types we haven't fully validated yet.

We've validated:
  - 10 (super PAC contributions) — HIGH confidence
  - 15 (standard contributions) — seen but not deeply tested
  - 15E (earmarked) — understood relationship with 24T
  - 22Y (refunds) — sign convention confirmed

Still need to understand better:
  - 15C (candidate self-funding)
  - 15I, 15T (earmark variants)
  - 11 (tribal/org contributions)
  - 24T (conduit forwards) — when it appears WITHOUT a matching 15E
  - 24I (earmark check pass-through)
  - 30, 31, 32 and variants (special accounts)
  - 20Y (nonfederal)
  - 21Y (tribal refund)
  - Negative amounts on non-refund types — are they always inline corrections?

Also: what about memo_cd = "X" across all types? When is it safe to include?
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

print("Loading cm...")
cm = pd.read_csv(
    DATA_ROOT / "interim" / "fec" / "2024" / "cm24" / "cm.txt",
    sep="|", header=None, names=CM_COLS, dtype="string", na_filter=False,
)

# ══════════════════════════════════════════════════════════════════════
# 1. TYPE 15C — Candidate self-funding
# ══════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("TYPE 15C — CANDIDATE SELF-FUNDING")
print(f"{'='*70}")
t15c = itcont[itcont["transaction_tp"].isin(["15C"])].copy()
print(f"Rows: {len(t15c):,}, Total: ${t15c['transaction_amt'].sum():,.2f}")
print(f"Positive: {(t15c['transaction_amt'] > 0).sum():,} rows, ${t15c.loc[t15c['transaction_amt'] > 0, 'transaction_amt'].sum():,.2f}")
print(f"Negative: {(t15c['transaction_amt'] < 0).sum():,} rows, ${t15c.loc[t15c['transaction_amt'] < 0, 'transaction_amt'].sum():,.2f}")
print(f"\nMemo code breakdown:")
print(t15c.groupby("memo_cd").agg(rows=("transaction_amt","size"), total=("transaction_amt","sum")).to_string())
print(f"\nTop 15 by amount:")
t15c_top = t15c.nlargest(15, "transaction_amt")[["name", "transaction_amt", "memo_cd", "memo_text", "cmte_id"]]
print(t15c_top.to_string(index=False))
print(f"\nMost negative:")
t15c_neg = t15c.nsmallest(10, "transaction_amt")[["name", "transaction_amt", "memo_cd", "memo_text", "cmte_id"]]
print(t15c_neg.to_string(index=False))

# ══════════════════════════════════════════════════════════════════════
# 2. TYPE 11 — Tribal/organizational
# ══════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("TYPE 11 — TRIBAL/ORGANIZATIONAL CONTRIBUTIONS")
print(f"{'='*70}")
t11 = itcont[itcont["transaction_tp"].isin(["11"])].copy()
t11_x = itcont[(itcont["transaction_tp"] == "11") & (itcont["memo_cd"] == "X")]
print(f"Rows: {len(t11):,}, Total: ${t11['transaction_amt'].sum():,.2f}")
print(f"Memo X: {len(t11_x):,} rows, ${t11_x['transaction_amt'].sum():,.2f}")
print(f"\nEntity types:")
print(t11["entity_tp"].value_counts().to_string())
print(f"\nTop 15 by amount:")
t11_top = t11.nlargest(15, "transaction_amt")[["name", "entity_tp", "employer", "transaction_amt", "memo_cd", "cmte_id"]]
print(t11_top.to_string(index=False))

# ══════════════════════════════════════════════════════════════════════
# 3. TYPE 24T — Conduit forwards
# Are there 24T rows without a matching 15E?
# ══════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("TYPE 24T — CONDUIT FORWARDS")
print(f"{'='*70}")
t24t = itcont[itcont["transaction_tp"] == "24T"].copy()
t15e = itcont[itcont["transaction_tp"] == "15E"].copy()
print(f"24T rows: {len(t24t):,}, Total: ${t24t['transaction_amt'].sum():,.2f}")
print(f"15E rows: {len(t15e):,}, Total: ${t15e['transaction_amt'].sum():,.2f}")
print(f"\n24T by memo code:")
print(t24t.groupby("memo_cd").agg(rows=("transaction_amt","size"), total=("transaction_amt","sum")).to_string())
print(f"\n24T entity types (who is the 'donor' on a 24T row?):")
print(t24t["entity_tp"].value_counts().to_string())
print(f"\n24T: who files these? Top committees:")
t24t_filers = t24t.groupby("cmte_id").agg(rows=("transaction_amt","size"), total=("transaction_amt","sum")).reset_index()
t24t_filers = t24t_filers.merge(cm[["cmte_id","cmte_nm","cmte_tp"]], on="cmte_id", how="left")
print(t24t_filers.nlargest(10, "total")[["cmte_id","cmte_nm","cmte_tp","rows","total"]].to_string(index=False))

# ══════════════════════════════════════════════════════════════════════
# 4. TYPE 24I — Earmark check pass-through
# ══════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("TYPE 24I — EARMARK CHECK PASS-THROUGH")
print(f"{'='*70}")
t24i = itcont[itcont["transaction_tp"] == "24I"].copy()
t24i_x = itcont[(itcont["transaction_tp"] == "24I") & (itcont["memo_cd"] == "X")]
print(f"Non-memo: {len(t24i) - len(t24i_x):,} rows, ${(t24i['transaction_amt'].sum() - t24i_x['transaction_amt'].sum()):,.2f}")
print(f"Memo X: {len(t24i_x):,} rows, ${t24i_x['transaction_amt'].sum():,.2f}")
print(f"\nTop filers:")
t24i_filers = t24i.groupby("cmte_id").agg(rows=("transaction_amt","size"), total=("transaction_amt","sum")).reset_index()
t24i_filers = t24i_filers.merge(cm[["cmte_id","cmte_nm","cmte_tp"]], on="cmte_id", how="left")
print(t24i_filers.nlargest(10, "total")[["cmte_id","cmte_nm","cmte_tp","rows","total"]].to_string(index=False))

# ══════════════════════════════════════════════════════════════════════
# 5. TYPES 30, 31, 32 and variants — Special accounts
# ══════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("TYPES 30/31/32 AND VARIANTS — SPECIAL ACCOUNTS")
print(f"{'='*70}")
special_codes = [c for c in itcont["transaction_tp"].unique() if c.startswith(("30","31","32"))]
special = itcont[itcont["transaction_tp"].isin(special_codes)]
sp_summary = (
    special.groupby(["transaction_tp", "memo_cd"])
    .agg(rows=("transaction_amt","size"), total=("transaction_amt","sum"))
    .reset_index()
    .sort_values("total", ascending=False)
)
print(sp_summary.to_string(index=False))

# What committees receive these?
print(f"\nTop recipient committee types for 31 (headquarters):")
t31 = itcont[itcont["transaction_tp"] == "31"].merge(cm[["cmte_id","cmte_nm","cmte_tp"]], on="cmte_id", how="left")
print(t31["cmte_tp"].value_counts().to_string())
print(f"\nTop recipients:")
t31_recip = t31.groupby(["cmte_id","cmte_nm","cmte_tp"]).agg(rows=("transaction_amt","size"),total=("transaction_amt","sum")).reset_index().nlargest(10,"total")
print(t31_recip.to_string(index=False))

# ══════════════════════════════════════════════════════════════════════
# 6. TYPE 20Y — Nonfederal
# ══════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("TYPE 20Y — NONFEDERAL DISBURSEMENTS")
print(f"{'='*70}")
t20y = itcont[itcont["transaction_tp"] == "20Y"].copy()
print(f"Rows: {len(t20y):,}, Total: ${t20y['transaction_amt'].sum():,.2f}")
print(f"\nMemo code:")
print(t20y.groupby("memo_cd").agg(rows=("transaction_amt","size"), total=("transaction_amt","sum")).to_string())
print(f"\nEntity types:")
print(t20y["entity_tp"].value_counts().to_string())
print(f"\nTop 10 by amount:")
t20y_top = t20y.nlargest(10, "transaction_amt")[["name","entity_tp","transaction_amt","memo_cd","memo_text","cmte_id"]]
print(t20y_top.to_string(index=False))
print(f"\nRecipient committee types:")
t20y_cm = t20y.merge(cm[["cmte_id","cmte_nm","cmte_tp"]], on="cmte_id", how="left")
print(t20y_cm["cmte_tp"].value_counts().to_string())

# ══════════════════════════════════════════════════════════════════════
# 7. MEMO X across all types — how much $ is behind memo X?
# ══════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("MEMO X ACROSS ALL TRANSACTION TYPES")
print(f"{'='*70}")
memo_x = itcont[itcont["memo_cd"] == "X"]
memo_summary = (
    memo_x.groupby("transaction_tp")
    .agg(rows=("transaction_amt","size"), total=("transaction_amt","sum"))
    .reset_index()
    .sort_values("total", ascending=False)
)
print(memo_summary.to_string(index=False))
print(f"\nTotal memo X: {len(memo_x):,} rows, ${memo_x['transaction_amt'].sum():,.2f}")

# Compare to non-memo for each type
print(f"\nMemo X as % of type total (by $):")
for tt in memo_summary["transaction_tp"].head(15):
    total_tt = itcont.loc[itcont["transaction_tp"] == tt, "transaction_amt"].sum()
    memo_tt = memo_x.loc[memo_x["transaction_tp"] == tt, "transaction_amt"].sum()
    if total_tt != 0:
        pct = memo_tt / total_tt * 100
        print(f"  {tt:>5}: memo X ${memo_tt:>18,.0f} / total ${total_tt:>18,.0f} = {pct:>6.1f}%")

# ══════════════════════════════════════════════════════════════════════
# 8. Deep look at memo_text patterns on memo X rows for key types
# ══════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("MEMO TEXT PATTERNS ON MEMO X ROWS")
print(f"{'='*70}")
for tt in ["10", "15", "15E", "15C", "11"]:
    subset = itcont[(itcont["transaction_tp"] == tt) & (itcont["memo_cd"] == "X")]
    if len(subset) == 0:
        continue
    print(f"\n--- Type {tt} memo X ({len(subset):,} rows, ${subset['transaction_amt'].sum():,.0f}) ---")
    print("Top memo texts:")
    print(subset["memo_text"].value_counts().head(15).to_string())

# ══════════════════════════════════════════════════════════════════════
# 9. Types 15I and 15T — earmark variants
# ══════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("TYPES 15I AND 15T — EARMARK VARIANTS")
print(f"{'='*70}")
for tt in ["15I", "15T"]:
    subset = itcont[itcont["transaction_tp"] == tt]
    if len(subset) > 0:
        print(f"\n{tt}: {len(subset):,} rows, ${subset['transaction_amt'].sum():,.2f}")
    else:
        print(f"\n{tt}: not present in 2024 itcont")

# Check 31E too since it appeared in our EDA
print(f"\n{'='*70}")
print("TYPE 31E — EARMARKED TO HEADQUARTERS ACCOUNT")
print(f"{'='*70}")
t31e = itcont[itcont["transaction_tp"] == "31E"]
print(f"Rows: {len(t31e):,}, Total: ${t31e['transaction_amt'].sum():,.2f}")
if len(t31e) > 0:
    print(f"\nTop filers:")
    t31e_f = t31e.groupby("cmte_id").agg(rows=("transaction_amt","size"),total=("transaction_amt","sum")).reset_index()
    t31e_f = t31e_f.merge(cm[["cmte_id","cmte_nm","cmte_tp"]], on="cmte_id", how="left")
    print(t31e_f.nlargest(5,"total")[["cmte_id","cmte_nm","cmte_tp","rows","total"]].to_string(index=False))

print(f"\nDone.")
