import unittest

import pandas as pd

from pipeline.fec.committee_profiles import (KINDS, TOPICS, load_registry, summarize_committee_rows,
                                             summarize_receipts)
from pipeline.tagging import committees as tagging
from pipeline.tagging.registry import load_companies


def gift(name, amount, entity="IND", kind="10", memo="", employer=""):
    return {"cmte_id": "C1", "transaction_tp": kind, "entity_tp": entity, "name": name, "employer": employer,
            "occupation": "", "transaction_dt": "01012026", "transaction_amt": str(amount), "other_id": "",
            "memo_cd": memo, "sub_id": name + str(amount)}


def flow(kind, other_id, amount, name="", cand_id="", memo=""):
    return {"cmte_id": "C1", "transaction_tp": kind, "other_id": other_id, "name": name,
            "transaction_amt": str(amount), "memo_cd": memo, "cand_id": cand_id}


class RegistryFileTests(unittest.TestCase):
    def test_registry_is_valid_and_points_at_known_companies(self):
        registry = pd.read_csv(tagging.REGISTRY_PATH, dtype="string", na_filter=False)
        self.assertFalse(registry.cmte_id.duplicated().any())
        self.assertEqual(set(registry.kind) - KINDS, set())
        self.assertEqual(set(registry.topic) - TOPICS, set())
        self.assertTrue(registry.include.isin(["TRUE", "FALSE"]).all())
        self.assertEqual(set(registry.company[registry.company != ""]) - set(load_companies().canonical_name), set())
        # A company only belongs on corporate PAC rows; rejected general committees are never included.
        self.assertTrue((registry.kind[registry.company != ""] == "corporate_pac").all())
        self.assertTrue((registry.include[registry.kind == "general"] == "FALSE").all())
        included = load_registry()
        self.assertIn("C00916114", set(included.cmte_id))       # Leading the Future
        self.assertNotIn("C00892471", set(included.cmte_id))    # MAGA Inc. is a recipient, not a tech vehicle
        self.assertEqual(set(included.topic[included.cmte_id.isin(["C00835959", "C00836221"])]), {"crypto"})


class ProfileRuleTests(unittest.TestCase):
    def test_memo_rows_are_listed_but_not_summed(self):
        rows = pd.DataFrame([
            gift("BIG FUND LLC", 50, entity="ORG"),
            gift("PARTNER, A", 25, memo="X", employer="BIG FUND"),
            gift("PARTNER, B", 25, memo="X", employer="BIG FUND"),
            gift("DONOR, C", 10, employer="OPENAI"),
            gift("DONOR, C", 4, kind="22Y"),                  # refund
            gift("OTHER PAC", 7, entity="PAC"),
            gift("IGNORED", 99, kind="24T"),                  # not a receipt type
        ])
        totals, contributors, attributions = summarize_receipts(rows)
        self.assertEqual(totals, {"individuals": 6.0, "organizations": 50.0, "committees": 7.0,
                                  "candidates": 0.0, "other": 0.0})
        self.assertEqual([(c["name"], c["amount"]) for c in contributors],
                         [("BIG FUND LLC", 50.0), ("OTHER PAC", 7.0), ("DONOR, C", 6.0)])
        self.assertEqual({a["name"] for a in attributions}, {"PARTNER, A", "PARTNER, B"})
        self.assertEqual(sum(a["amount"] for a in attributions), 50.0)

    def test_transfers_and_independent_expenditures(self):
        rows = pd.DataFrame([
            flow("24K", "C2", 20, name="AFFILIATE"),
            flow("24G", "C2", 5, name="AFFILIATE"),
            flow("18G", "C3", 9, name="PARENT"),
            flow("18K", "C3", 100, name="PARENT", memo="X"),   # memo: ignored
            flow("24E", "", 8, name="SMITH", cand_id="H1"),
            flow("24A", "", 3, name="SMITH", cand_id="H1"),
            flow("24A", "", 2, name="AD VENDOR LLC"),          # no candidate ID in the bulk file
        ])
        names = pd.Series({"C2": "AFFILIATE PAC", "C3": "PARENT PAC"})
        candidates = pd.DataFrame([{"cand_id": "H1", "cand_name": "SMITH, JANE", "cand_pty_affiliation": "DEM",
                                    "cand_office": "H", "cand_office_st": "NY", "cand_office_district": "12"}]
                                  ).set_index("cand_id")
        result = summarize_committee_rows(rows, names, candidates, {"C1", "C2"})
        self.assertEqual(result["given_to_committees"],
                         [{"cmte_id": "C2", "name": "AFFILIATE PAC", "amount": 25.0, "in_registry": True}])
        self.assertEqual(result["received_from_committees"],
                         [{"cmte_id": "C3", "name": "PARENT PAC", "amount": 9.0, "in_registry": False}])
        spending = result["independent_expenditures"]
        self.assertEqual((spending["support"], spending["oppose"]), (8.0, 5.0))
        by_key = {(r["cand_id"], r["position"]): r for r in spending["by_candidate"]}
        self.assertEqual(by_key[("H1", "support")]["candidate"], "SMITH, JANE")
        self.assertEqual(by_key[("H1", "support")]["office"], "H-NY-12")
        self.assertIn("AD VENDOR LLC", by_key[("", "oppose")]["candidate"])
        self.assertIn("not identified", by_key[("", "oppose")]["candidate"])


class DiscoveryTests(unittest.TestCase):
    def test_name_search_finds_ai_and_crypto_but_not_ordinary_words(self):
        hits = lambda name: bool(tagging.NAME_SEARCH.search(name))
        for name in ("AI ACCOUNTABILITY SUPER PAC", "STOP AI", "C3.AI INC. PAC", "BITCOIN FREEDOM PAC",
                     "AMERICANS FOR RESPONSIBLE INNOVATION PAC", "TECHNOLOGY NETWORK (TECHNET) FEDERAL PAC"):
            self.assertTrue(hits(name), name)
        for name in ("MAINE PAC", "FAIRSHAKE", "SENATE LEADERSHIP FUND", "AIR LINE PILOTS ASSOCIATION PAC"):
            self.assertFalse(hits(name), name)

    def test_review_queue_leaves_out_everything_already_decided(self):
        decided = set(pd.read_csv(tagging.REGISTRY_PATH, dtype="string", na_filter=False).cmte_id)
        candidates = pd.DataFrame({"cmte_id": ["C00916114", "C00892471", "C99999999"], "name": ["a", "b", "c"]})
        queue = tagging.build_review_queue(candidates)
        self.assertEqual(queue.cmte_id.tolist(), ["C99999999"])
        self.assertTrue({"C00916114", "C00892471"} <= decided)
        self.assertEqual(queue.include.tolist(), [""])


if __name__ == "__main__":
    unittest.main()
