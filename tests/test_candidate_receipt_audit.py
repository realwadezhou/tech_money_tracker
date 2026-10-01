import unittest

import pandas as pd

from scripts.audit_candidate_receipts import AUDIT_COLUMNS, current_authorized_links, select_evidence, summarize


def rows(values):
    defaults = dict.fromkeys(AUDIT_COLUMNS, "")
    defaults.update(cmte_id="campaign", transaction_tp="15", entity_tp="IND",
                    name="DOE, JANE", employer="NVIDIA", transaction_amt="100", sub_id="1")
    return pd.DataFrame([defaults | row for row in values]).astype("string")


class CandidateReceiptAuditTests(unittest.TestCase):
    def test_routes_memos_and_name_only_refunds_do_not_inflate_headline(self):
        frame = rows([
            {},
            {"transaction_tp": "22Y", "transaction_amt": "20", "sub_id": "2"},
            {"transaction_tp": "22Y", "transaction_amt": "-5", "sub_id": "3"},
            {"transaction_tp": "22Y", "transaction_amt": "10", "employer": "", "sub_id": "4"},
            {"cmte_id": "conduit", "other_id": "campaign", "transaction_tp": "24T", "sub_id": "5"},
            {"transaction_tp": "15E", "memo_cd": "X", "sub_id": "6"},
            {"cmte_id": "former", "sub_id": "7"},
            {"employer": "NVIDIA UNVERIFIED AFFILIATE", "sub_id": "8"},
        ])
        evidence = select_evidence(frame, {"NVIDIA"}, {"campaign"}, {"former"}, "candidate", "nvidia")
        evidence["source_file"] = "itcont.txt"
        result = summarize(evidence)
        self.assertEqual(result["current_authorized_net"], 85)
        self.assertEqual(result["current_authorized_rows"], 3)
        self.assertEqual(result["potential_same_name_unmatched_refund_rows"], 1)
        self.assertEqual(result["potential_same_name_unmatched_refunds_signed"], -10)
        self.assertEqual(evidence.loc[evidence.sub_id.eq("5"), "exclusion_reason"].iloc[0], "intermediary_routing")

    def test_identical_dollar_events_keep_separate_ids_and_name_count(self):
        frame = rows([{"sub_id": "1", "tran_id": "a"}, {"sub_id": "2", "tran_id": "b"}])
        evidence = select_evidence(frame, {"NVIDIA"}, {"campaign"}, set(), "candidate", "nvidia")
        evidence["source_file"] = "itcont.txt"
        result = summarize(evidence)
        self.assertEqual(result["current_authorized_net"], 200)
        self.assertEqual(result["current_authorized_distinct_name_strings"], 1)
        self.assertEqual(result["duplicate_sub_id_rows"], 0)

    def test_ambiguous_and_joint_fundraising_committees_are_not_current_campaigns(self):
        linkage = pd.DataFrame([
            {"cand_id": candidate, "cmte_id": committee, "fec_election_yr": "2026", "cand_election_yr": "2026"}
            for candidate, committee in [("candidate", "main"), ("candidate", "shared"), ("third_party", "shared"), ("candidate", "joint")]
        ])
        committees = pd.DataFrame([
            {"cmte_id": committee, "cmte_tp": "H", "cmte_dsgn": designation, "cand_id": owner, "cmte_nm": committee}
            for committee, designation, owner in [("main", "P", "candidate"), ("shared", "P", ""), ("joint", "J", "candidate")]
        ])
        result = current_authorized_links(linkage, committees, "candidate", 2026)
        self.assertEqual(result.cmte_id.tolist(), ["main"])

    def test_invalid_included_amount_fails_instead_of_claiming_zero(self):
        evidence = select_evidence(rows([{"transaction_amt": "invalid"}]), {"NVIDIA"}, {"campaign"}, set(), "candidate", "nvidia")
        evidence["source_file"] = "itcont.txt"
        with self.assertRaises(ValueError):
            summarize(evidence)


if __name__ == "__main__":
    unittest.main()
