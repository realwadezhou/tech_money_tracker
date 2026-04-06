"""
Dig deeper into medium/low confidence transaction types.

Types worth investigating further:
  itcont:
    - 22Y sign convention on committee types (verified for individuals, what about committees?)
    - 15C memo X loan forgiveness — how big is this channel?

  itoth:
    - 22Z — verify sign convention matches 22Y
    - 24C ($89M) — coordinated party expenditure, who does this?
    - 16C ($424M) — candidate loans, any forgiveness patterns?
    - 18U ($22M) — unregistered committee contributions
    - 42 ($132M) — convention account, what is this really?
    - 24Z ($5.4M) — in-kind contributions
    - Various K/G/F special account suffixes
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

print("Loading cm...")
cm = pd.read_csv(
    DATA_ROOT / "interim" / "fec" / "2024" / "cm24" / "cm.txt",
    sep="|", header=None, names=CM_COLS, dtype="string", na_filter=False,
)

print("Loading itoth...")
itoth = pd.read_csv(
    DATA_ROOT / "interim" / "fec" / "2024" / "oth24" / "itoth.txt",
    sep="|", header=None, names=ITCONT_COLS, dtype="string", na_filter=False,
)
itoth["transaction_amt"] = pd.to_numeric(itoth["transaction_amt"], errors="coerce").fillna(0.0)


def add_cmte_info(df, id_col="cmte_id", prefix="filer"):
    return df.merge(
        cm[["cmte_id", "cmte_nm", "cmte_tp", "connected_org_nm", "cand_id"]].rename(
            columns={c: f"{prefix}_{c}" if c != "cmte_id" else id_col for c in ["cmte_nm", "cmte_tp", "connected_org_nm", "cand_id", "cmte_id"]}
        ),
        on=id_col, how="left",
    )


# ══════════════════════════════════════════════════════════════════════
# 1. 22Z — Committee refund. Verify sign convention.
# ══════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("22Z — COMMITTEE REFUND (itoth)")
print(f"{'='*70}")
t22z = itoth[itoth["transaction_tp"] == "22Z"].copy()
print(f"Rows: {len(t22z):,}, Total: ${t22z['transaction_amt'].sum():,.2f}")
print(f"Positive: {(t22z['transaction_amt'] > 0).sum():,} rows, ${t22z.loc[t22z['transaction_amt'] > 0, 'transaction_amt'].sum():,.2f}")
print(f"Negative: {(t22z['transaction_amt'] < 0).sum():,} rows, ${t22z.loc[t22z['transaction_amt'] < 0, 'transaction_amt'].sum():,.2f}")

# Who files 22Z and who receives?
t22z = add_cmte_info(t22z, "cmte_id", "filer")
print(f"\nFiler committee types:")
print(t22z["filer_cmte_tp"].value_counts().head(10).to_string())
print(f"\nTop filers by amount:")
top_filers = t22z.groupby(["cmte_id", "filer_cmte_nm", "filer_cmte_tp"]).agg(
    rows=("transaction_amt", "size"), total=("transaction_amt", "sum")
).reset_index().nlargest(10, "total")
print(top_filers.to_string(index=False))

# Match some 22Z to original contributions (same pattern as 22Y)
print(f"\nLargest positive 22Z rows (potential refunds issued):")
big_pos = t22z[t22z["transaction_amt"] > 0].nlargest(10, "transaction_amt")
print(big_pos[["filer_cmte_nm", "name", "transaction_amt", "memo_cd", "memo_text", "transaction_dt"]].to_string(index=False))

print(f"\nLargest negative 22Z rows:")
big_neg = t22z[t22z["transaction_amt"] < 0].nsmallest(10, "transaction_amt")
print(big_neg[["filer_cmte_nm", "name", "transaction_amt", "memo_cd", "memo_text", "transaction_dt"]].to_string(index=False))

# ══════════════════════════════════════════════════════════════════════
# 2. 24C — Coordinated party expenditure
# ══════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("24C — COORDINATED PARTY EXPENDITURE (itoth)")
print(f"{'='*70}")
t24c = itoth[itoth["transaction_tp"].isin(["24C"])].copy()
t24c_x = t24c[t24c["memo_cd"] == "X"]
print(f"Non-memo: {len(t24c) - len(t24c_x):,} rows, ${(t24c['transaction_amt'].sum() - t24c_x['transaction_amt'].sum()):,.2f}")
print(f"Memo X: {len(t24c_x):,} rows, ${t24c_x['transaction_amt'].sum():,.2f}")
t24c = add_cmte_info(t24c, "cmte_id", "filer")
print(f"\nWho makes coordinated expenditures? (filer committee type)")
print(t24c["filer_cmte_tp"].value_counts().to_string())
print(f"\nTop filers:")
t24c_filers = t24c.groupby(["cmte_id", "filer_cmte_nm", "filer_cmte_tp"]).agg(
    rows=("transaction_amt", "size"), total=("transaction_amt", "sum")
).reset_index().nlargest(10, "total")
print(t24c_filers.to_string(index=False))
print(f"\nWho are the beneficiaries? (name field = who the money supports)")
print(t24c.groupby("name").agg(rows=("transaction_amt","size"), total=("transaction_amt","sum")).reset_index().nlargest(15,"total").to_string(index=False))
print(f"\nSample rows:")
print(t24c.nlargest(5, "transaction_amt")[["filer_cmte_nm", "filer_cmte_tp", "name", "transaction_amt", "memo_cd", "memo_text", "other_id"]].to_string(index=False))

# ══════════════════════════════════════════════════════════════════════
# 3. 16C — Candidate loans
# ══════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("16C — CANDIDATE LOANS (itoth)")
print(f"{'='*70}")
t16c = itoth[itoth["transaction_tp"].isin(["16C"])].copy()
t16c_x = t16c[t16c["memo_cd"] == "X"]
print(f"Non-memo: {len(t16c) - len(t16c_x):,} rows, ${(t16c['transaction_amt'].sum() - t16c_x['transaction_amt'].sum()):,.2f}")
print(f"Memo X: {len(t16c_x):,} rows, ${t16c_x['transaction_amt'].sum():,.2f}")
t16c = add_cmte_info(t16c, "cmte_id", "filer")
print(f"\nTop candidates lending to their campaigns:")
t16c_top = t16c.groupby(["cmte_id", "filer_cmte_nm", "name"]).agg(
    rows=("transaction_amt","size"), total=("transaction_amt","sum")
).reset_index().nlargest(15, "total")
print(t16c_top.to_string(index=False))

# Check: are there 20C (loan repayment) rows that offset these?
t20c = itoth[itoth["transaction_tp"] == "20C"]
print(f"\n20C loan repayments: {len(t20c):,} rows, ${t20c['transaction_amt'].sum():,.2f}")

# ══════════════════════════════════════════════════════════════════════
# 4. 18U — Unregistered committee contributions
# ══════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("18U — UNREGISTERED COMMITTEE CONTRIBUTIONS (itoth)")
print(f"{'='*70}")
t18u = itoth[itoth["transaction_tp"].isin(["18U"])].copy()
t18u_x = t18u[t18u["memo_cd"] == "X"]
print(f"Non-memo: {len(t18u) - len(t18u_x):,} rows, ${(t18u['transaction_amt'].sum() - t18u_x['transaction_amt'].sum()):,.2f}")
print(f"Memo X: {len(t18u_x):,} rows, ${t18u_x['transaction_amt'].sum():,.2f}")
t18u = add_cmte_info(t18u, "cmte_id", "filer")
print(f"\nRecipient committee types:")
print(t18u["filer_cmte_tp"].value_counts().to_string())
print(f"\nWho are the unregistered contributors?")
t18u_givers = t18u.groupby(["name", "entity_tp"]).agg(
    rows=("transaction_amt","size"), total=("transaction_amt","sum")
).reset_index().nlargest(15, "total")
print(t18u_givers.to_string(index=False))
print(f"\nMemo texts:")
print(t18u["memo_text"].value_counts().head(10).to_string())

# ══════════════════════════════════════════════════════════════════════
# 5. 42 — Convention account
# ══════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("42 — CONVENTION ACCOUNT (itoth)")
print(f"{'='*70}")
t42 = itoth[itoth["transaction_tp"] == "42"].copy()
t42_x = itoth[itoth["transaction_tp"] == "42"] [itoth["memo_cd"] == "X"]
print(f"Non-memo: {len(t42) - len(t42_x):,} rows, ${(t42['transaction_amt'].sum() - t42_x['transaction_amt'].sum()):,.2f}")
print(f"Memo X: {len(t42_x):,} rows, ${t42_x['transaction_amt'].sum():,.2f}")
t42 = add_cmte_info(t42, "cmte_id", "filer")
print(f"\nFiler committee types:")
print(t42["filer_cmte_tp"].value_counts().to_string())
print(f"\nTop filers:")
t42_top = t42.groupby(["cmte_id","filer_cmte_nm","filer_cmte_tp"]).agg(
    rows=("transaction_amt","size"), total=("transaction_amt","sum")
).reset_index().nlargest(10,"total")
print(t42_top.to_string(index=False))
print(f"\nTop contributors (name field):")
t42_names = t42.groupby("name").agg(rows=("transaction_amt","size"),total=("transaction_amt","sum")).reset_index().nlargest(10,"total")
print(t42_names.to_string(index=False))

# ══════════════════════════════════════════════════════════════════════
# 6. 24Z — In-kind contributions
# ══════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("24Z — IN-KIND CONTRIBUTIONS (itoth)")
print(f"{'='*70}")
t24z = itoth[itoth["transaction_tp"] == "24Z"].copy()
t24z_x = t24z[t24z["memo_cd"] == "X"]
print(f"Non-memo: {len(t24z) - len(t24z_x):,} rows, ${(t24z['transaction_amt'].sum() - t24z_x['transaction_amt'].sum()):,.2f}")
print(f"Memo X: {len(t24z_x):,} rows, ${t24z_x['transaction_amt'].sum():,.2f}")
t24z = add_cmte_info(t24z, "cmte_id", "filer")
print(f"\nFiler committee types:")
print(t24z["filer_cmte_tp"].value_counts().to_string())
print(f"\nTop filers:")
t24z_top = t24z.groupby(["cmte_id","filer_cmte_nm"]).agg(
    rows=("transaction_amt","size"), total=("transaction_amt","sum")
).reset_index().nlargest(10,"total")
print(t24z_top.to_string(index=False))
print(f"\nSample memo texts:")
print(t24z["memo_text"].value_counts().head(10).to_string())

# ══════════════════════════════════════════════════════════════════════
# 7. Special account K/G/F suffixes — what are these?
# ══════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("SPECIAL ACCOUNT SUFFIXES (K, G, F) IN ITOTH")
print(f"{'='*70}")
special_types = ["30K", "31K", "32K", "30G", "31G", "32G", "30F", "31F", "32F",
                 "30J", "31J", "32J", "11J"]
for tt in special_types:
    sub = itoth[itoth["transaction_tp"] == tt]
    if len(sub) > 0:
        sub_x = sub[sub["memo_cd"] == "X"]
        print(f"\n{tt}: {len(sub):,} rows, ${sub['transaction_amt'].sum():,.0f} (memo X: {len(sub_x):,} rows, ${sub_x['transaction_amt'].sum():,.0f})")
        # Just show what the K/G/F means by checking who files them
        sub_cm = sub.merge(cm[["cmte_id","cmte_nm","cmte_tp"]], on="cmte_id", how="left")
        print(f"  Filer types: {sub_cm['cmte_tp'].value_counts().head(5).to_dict()}")

# ══════════════════════════════════════════════════════════════════════
# 8. 16G — Loan from individual (itoth)
# ══════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("16G — LOAN FROM INDIVIDUAL (itoth)")
print(f"{'='*70}")
t16g = itoth[itoth["transaction_tp"] == "16G"].copy()
print(f"Rows: {len(t16g):,}, Total: ${t16g['transaction_amt'].sum():,.2f}")
t16g = add_cmte_info(t16g, "cmte_id", "filer")
print(f"\nTop lenders:")
t16g_top = t16g.groupby(["name","filer_cmte_nm"]).agg(
    rows=("transaction_amt","size"), total=("transaction_amt","sum")
).reset_index().nlargest(10,"total")
print(t16g_top.to_string(index=False))

# Check 20G (loan repayment from individual)
t20g = itoth[itoth["transaction_tp"] == "20G"]
print(f"\n20G loan repayments to individuals: {len(t20g):,} rows, ${t20g['transaction_amt'].sum():,.2f}")

# ══════════════════════════════════════════════════════════════════════
# 9. 15Z — In-kind from registered filer (itoth)
# ══════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("15Z — IN-KIND FROM REGISTERED FILER (itoth)")
print(f"{'='*70}")
t15z = itoth[itoth["transaction_tp"] == "15Z"].copy()
print(f"Rows: {len(t15z):,}, Total: ${t15z['transaction_amt'].sum():,.2f}")
t15z = add_cmte_info(t15z, "cmte_id", "filer")
print(f"\nFiler committee types:")
print(t15z["filer_cmte_tp"].value_counts().head(5).to_string())
print(f"\nSample memo texts:")
print(t15z["memo_text"].value_counts().head(10).to_string())

print(f"\nDone.")
