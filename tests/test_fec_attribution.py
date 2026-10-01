import json
import unittest
from dataclasses import fields, replace
from decimal import Decimal
from pathlib import Path

from pipeline.fec.attribution import AttributionRecord, attribute_campaign


ALIASES = {"NVIDIA": "nvidia", "NVIDIA CORPORATION": "nvidia", "NVDA": "nvidia",
           "META PLATFORMS": "meta", "MICROSOFT": "microsoft", "A16Z": "a16z",
           "PALANTIR": "palantir", "OTHER EMPLOYER": "other"}
EVIDENCE = ("source-reviewed test relationship",)


def record(key, amount="100.00", **changes):
    values = dict(record_id=key, committee_id="C00000001", file_number=1,
                  transaction_id=key, schedule="SA11AI", entity_type="IND",
                  amount=amount, employer="NVIDIA", date="20260101")
    return AttributionRecord(**(values | changes))


def attribute(rows, company="nvidia"):
    # Synthetic cases here deliberately supply their complete refund scope.
    return attribute_campaign(rows, ALIASES, target_company=company, refund_scope_complete=True)


class CampaignAttributionTests(unittest.TestCase):
    def test_identity_conflict_cannot_hide_behind_a_blank_intermediate_adjustment(self):
        original = record("original", "100", identity_key="reviewed:alice")
        intermediate = record("intermediate", "-10", employer="", role="adjustment",
                              related_record_id="original", evidence=EVIDENCE)
        for role, schedule, amount in [("refund", "SB20A", "25"),
                                       ("adjustment", "SA11AI", "5")]:
            descendant = record("descendant", amount, employer="", role=role, schedule=schedule,
                                identity_key="reviewed:bob", related_record_id="intermediate",
                                evidence=EVIDENCE)
            result = attribute([original, intermediate, descendant])
            with self.subTest(role=role):
                self.assertIsNone(result.net_total)
                self.assertIn("conflicting_donor_identity", {i.code for i in result.issues})
                if role == "refund":
                    self.assertEqual(result.attributed_total, Decimal("90"))
                else:
                    self.assertIsNone(result.attributed_total)
            valid = attribute([original, intermediate, replace(descendant, identity_key="reviewed:alice")])
            self.assertIsNotNone(valid.net_total)

    def test_direct_receipt_and_linked_conduit_are_one_gift(self):
        rows = [record("gift", back_reference_transaction_id="conduit"),
                record("conduit", entity_type="PAC", employer="", memo=True)]
        result = attribute(rows)
        self.assertEqual(result.attributed_total, Decimal("100.00"))
        self.assertEqual(result.components["direct_receipts"], Decimal("100.00"))
        self.assertEqual(result.decisions[1].status, "context")

    def test_exact_employer_matching_never_uses_fuzzy_or_name_identity(self):
        rows = [record("one", employer=" nvidia ", donor_name="SAME, PERSON"),
                record("two", employer="NVIDIA UNKNOWN AFFILIATE", donor_name="SAME, PERSON")]
        self.assertEqual(attribute(rows).attributed_total, Decimal("100.00"))

    def test_jfc_allocation_and_signed_election_adjustments_preserve_donor_amount(self):
        rows = [record("transfer", "20.00", schedule="SA12", entity_type="PAC", employer=""),
                record("allocation", "25.00", schedule="SA12", employer="META PLATFORMS",
                       memo=True, role="jfc_allocation", related_record_id="transfer",
                       back_reference_transaction_id="transfer", evidence=EVIDENCE),
                record("primary", "-25.00", employer="META PLATFORMS", memo=True,
                       role="adjustment", related_record_id="allocation", evidence=EVIDENCE,
                       election="P2026"),
                record("general", "25.00", employer="META PLATFORMS", memo=True,
                       role="adjustment", related_record_id="allocation", evidence=EVIDENCE,
                       election="G2026")]
        result = attribute(rows, "meta")
        self.assertEqual(result.components["jfc_allocations"], Decimal("25.00"))
        self.assertEqual(result.components["signed_adjustments"], Decimal("0.00"))
        self.assertEqual(result.attributed_total, Decimal("25.00"))
        self.assertEqual(result.campaign_nonmemo_receipts, Decimal("20.00"))

    def test_partnership_parent_and_partner_memos_are_separate_views(self):
        parent = record("partnership", "25000000", entity_type="ORG", employer="")
        children = [record(key, "12500000", employer="A16Z", memo=True,
                           role="partnership_attribution", related_record_id="partnership",
                           back_reference_transaction_id="partnership", evidence=EVIDENCE)
                    for key in ["partner_a", "partner_b"]]
        result = attribute([parent, *children], "a16z")
        self.assertEqual(result.attributed_total, Decimal("25000000"))
        self.assertEqual(result.components["partnership_attributions"], Decimal("25000000"))
        self.assertEqual(result.campaign_nonmemo_receipts, Decimal("25000000"))
        incomplete = attribute([parent, children[0]], "a16z")
        self.assertIsNone(incomplete.attributed_total)
        self.assertIn("incomplete_partnership_family", {issue.code for issue in incomplete.issues})

    def test_negative_allocation_requires_retained_donor_adjustment_origin(self):
        transfer = record("transfer", "100", schedule="SA12", entity_type="PAC", employer="")
        negative = record("allocation", "-50", schedule="SA12", memo=True, role="jfc_allocation",
                          related_record_id="transfer", back_reference_transaction_id="transfer", evidence=EVIDENCE)
        result = attribute([transfer, negative])
        self.assertIsNone(result.attributed_total)
        self.assertEqual(result.components["jfc_allocations"], Decimal("0"))
        self.assertIn("invalid_allocation_relationship", {issue.code for issue in result.issues})
        parent = record("partnership", "100", entity_type="ORG", employer="")
        negative_partner = replace(negative, schedule="SA11AI", role="partnership_attribution",
                                   related_record_id="partnership", back_reference_transaction_id="partnership")
        positive_partner = replace(negative_partner, record_id="other-partner", transaction_id="other-partner",
                                   amount="150", employer="MICROSOFT")
        # Arithmetic conservation does not identify the prior donor receipt
        # that a negative partner amount would purport to reverse.
        result = attribute([parent, negative_partner, positive_partner])
        self.assertIsNone(result.attributed_total)
        self.assertEqual(result.components["partnership_attributions"], Decimal("0"))
        supported = replace(negative, amount="100")
        correction = record("correction", "-50", schedule="SA12", memo=True, role="adjustment",
                            related_record_id="allocation", evidence=EVIDENCE)
        self.assertEqual(attribute([transfer, supported, correction]).attributed_total, Decimal("50"))

    def test_source_reviewed_repeated_original_needs_retained_original(self):
        original = record("old:gift", "6600", file_number=1, transaction_id="gift",
                          employer="PALANTIR", schedule="SA17A")
        repeated = replace(original, record_id="new:gift", file_number=2, memo=True,
                           role="repeated_original", related_record_id=original.record_id,
                           evidence=EVIDENCE)
        adjustments = [record(key, amount, employer="PALANTIR", memo=True, file_number=2,
                              schedule="SA17A", role="adjustment", related_record_id=repeated.record_id,
                              evidence=EVIDENCE)
                       for key, amount in [("primary", "-3300"), ("general", "3300")]]
        result = attribute([original, repeated, *adjustments], "palantir")
        self.assertEqual(result.attributed_total, Decimal("6600.00"))
        self.assertEqual(result.decisions[1].status, "excluded_repeated_original")
        self.assertIsNone(attribute([repeated, *adjustments], "palantir").attributed_total)
        changed = replace(original, amount="6500")
        self.assertIsNone(attribute([changed, repeated], "palantir").attributed_total)

    def test_two_equal_real_gifts_are_not_deduplicated(self):
        rows = [record("first", "3500", employer="MICROSOFT", donor_name="SAME, NAME"),
                record("second", "3500", employer="MICROSOFT", donor_name="SAME, NAME"),
                record("minus", "-3500", employer="MICROSOFT", memo=True, role="adjustment",
                       related_record_id="second", evidence=EVIDENCE),
                record("plus", "3500", employer="MICROSOFT", memo=True, role="adjustment",
                       related_record_id="second", evidence=EVIDENCE)]
        self.assertEqual(attribute(rows, "microsoft").attributed_total, Decimal("7000.00"))

    def test_unexplained_equal_memo_does_not_invent_repeat_relationship(self):
        rows = [record("one", "100"), record("two", "100", memo=True)]
        result = attribute(rows)
        self.assertEqual(result.components["direct_receipts"], Decimal("100.00"))
        self.assertIsNone(result.attributed_total)
        self.assertIsNone(result.net_total)
        self.assertIn("unclassified_record", {issue.code for issue in result.issues})

    def test_blank_employer_refund_links_by_evidence_not_donor_name(self):
        gift = record("gift", "100", donor_name="SAME, NAME")
        refund = record("refund", "30", schedule="SB20A", employer="", donor_name="SAME, NAME")
        unresolved = attribute([gift, refund])
        self.assertEqual(unresolved.attributed_total, Decimal("100.00"))
        self.assertIsNone(unresolved.net_total)
        self.assertEqual(unresolved.components["refunds"], Decimal("0.00"))
        linked = replace(refund, role="refund", related_record_id="gift", evidence=EVIDENCE)
        reversal = replace(linked, record_id="reversal", transaction_id="reversal", amount="-10")
        resolved = attribute([gift, linked, reversal])
        self.assertEqual(resolved.attributed_total, Decimal("100.00"))
        self.assertEqual(resolved.components["refunds"], Decimal("-20.00"))
        self.assertEqual(resolved.net_total, Decimal("80.00"))

    def test_unlinked_refund_for_other_reported_company_still_requires_review(self):
        result = attribute([record("gift"), record("refund", "20", schedule="SB20A", employer="OTHER EMPLOYER")])
        self.assertEqual(result.attributed_total, Decimal("100.00"))
        self.assertIsNone(result.net_total)

    def test_explicit_refund_identity_contradiction_blocks_only_net(self):
        original = record("original", "100", identity_key="review:alice")
        refund = record("refund", "25", schedule="SB20A", employer="", role="refund",
                        related_record_id="original", identity_key="review:bob", evidence=EVIDENCE)
        result = attribute([original, refund])
        self.assertEqual(result.attributed_total, Decimal("100"))
        self.assertIsNone(result.net_total)
        self.assertEqual(result.components["refunds"], Decimal("0"))
        self.assertIn("different_refund_identity", {issue.code for issue in result.issues})
        supported = attribute([original, replace(refund, identity_key=original.identity_key)])
        self.assertEqual(supported.net_total, Decimal("75"))
        self.assertEqual(supported.components["refunds"], Decimal("-25"))

    def test_explicit_repeated_original_identity_contradiction_cannot_delete_receipt(self):
        original = record("original", "100", identity_key="review:alice")
        repeated = replace(original, record_id="later:original", file_number=2, memo=True,
                           role="repeated_original", related_record_id=original.record_id,
                           identity_key="review:bob", evidence=EVIDENCE)
        result = attribute([original, repeated])
        self.assertIsNone(result.attributed_total)
        self.assertEqual(result.decisions[1].status, "unresolved")
        self.assertIn("different_repeated_original_identity", {issue.code for issue in result.issues})
        supported = attribute([original, replace(repeated, identity_key=original.identity_key)])
        self.assertEqual(supported.attributed_total, Decimal("100"))
        self.assertEqual(supported.decisions[1].status, "excluded_repeated_original")

    def test_relabeling_refund_as_unknown_or_context_cannot_certify_net(self):
        for role in ("unresolved", "context"):
            with self.subTest(role=role):
                result = attribute([record("gift"), record("refund", "20", schedule="SB20A",
                                                         employer="", role=role)])
                self.assertEqual(result.attributed_total, Decimal("100.00"))
                self.assertIsNone(result.net_total)
                self.assertIn("unclassified_refund", {issue.code for issue in result.issues})

    def test_unrelated_unknown_memo_does_not_block_known_company_component(self):
        result = attribute([record("gift"), record("other", memo=True, employer="OTHER EMPLOYER")])
        self.assertEqual(result.attributed_total, Decimal("100.00"))
        self.assertFalse(result.issues[0].blocks_attributed_total)

    def test_conflicting_linked_employers_are_not_resolved_by_same_name(self):
        gift = record("gift", donor_name="SAME, NAME")
        adjustment = record("adjust", "-20", employer="OTHER EMPLOYER", donor_name="SAME, NAME",
                            role="adjustment", related_record_id="gift", evidence=EVIDENCE)
        self.assertIsNone(attribute([gift, adjustment]).attributed_total)

    def test_negative_nonmemo_requires_adjustment_evidence(self):
        result = attribute([record("gift"), record("negative", "-10")])
        self.assertIsNone(result.attributed_total)
        reviewed = record("negative", "-10", role="adjustment", related_record_id="gift", evidence=EVIDENCE)
        self.assertEqual(attribute([record("gift"), reviewed]).attributed_total, Decimal("90.00"))

    def test_unresolved_negative_origin_cannot_be_excluded_by_missing_or_other_employer(self):
        for employer in ("", "Information Requested", "OTHER EMPLOYER"):
            for memo in (False, True):
                with self.subTest(employer=employer, memo=memo):
                    deduction = record("returned", "-100", employer=employer, memo=memo,
                                       memo_text="Returned Item")
                    result = attribute([record("gift"), deduction])
                    self.assertEqual(result.components["direct_receipts"], Decimal("100"))
                    self.assertIsNone(result.attributed_total)
                    self.assertIsNone(result.net_total)

    def test_reviewed_negative_origin_outside_company_does_not_reduce_company_total(self):
        other = record("other", "100", employer="OTHER EMPLOYER")
        deduction = record("returned", "-100", employer="OTHER EMPLOYER", role="adjustment",
                           related_record_id="other", evidence=EVIDENCE)
        result = attribute([record("gift"), other, deduction])
        self.assertEqual(result.attributed_total, Decimal("100"))

    def test_positive_nonmemo_correction_text_requires_review(self):
        row = record("possible_adjustment", "20", memo_text="Redesignation to general election")
        self.assertIsNone(attribute([row]).attributed_total)
        reviewed = replace(row, role="adjustment", related_record_id="original", evidence=EVIDENCE)
        self.assertEqual(attribute([record("original"), reviewed]).attributed_total, Decimal("120.00"))

    def test_absent_refund_scope_blocks_net_by_default_but_not_attribution(self):
        result = attribute_campaign([record("gift")], ALIASES, target_company="nvidia")
        self.assertEqual(result.attributed_total, Decimal("100.00"))
        self.assertIsNone(result.net_total)
        self.assertFalse(result.refund_scope_complete)
        self.assertEqual(result.issues[0].code, "refund_scope_incomplete")
        self.assertFalse(result.issues[0].blocks_attributed_total)

    def test_mislabeled_conduit_context_cannot_hide_a_donor_memo(self):
        row = record("unknown", memo=True, role="conduit_memo")
        self.assertIsNone(attribute([row]).attributed_total)

    def test_empty_scope_and_generic_context_do_not_certify_zero(self):
        self.assertIsNone(attribute([]).attributed_total)
        self.assertIsNone(attribute([record("unknown", memo=True, role="context")]).attributed_total)

    def test_unreviewed_filed_relationship_can_block_but_never_count_a_correction(self):
        rows = [record("gift", "100"),
                record("minus", "-50", memo=True, employer="", back_reference_transaction_id="gift"),
                record("plus", "50", memo=True, employer="OTHER EMPLOYER", back_reference_transaction_id="gift")]
        result = attribute(rows)
        self.assertIsNone(result.attributed_total)
        self.assertEqual(result.components["direct_receipts"], Decimal("100"))
        self.assertEqual(result.components["signed_adjustments"], Decimal("0"))
        self.assertTrue(all(x.blocks_attributed_total for x in result.issues))

    def test_unresolved_parent_invalidates_even_reviewed_allocation(self):
        for role, schedule, entity in [("partnership_attribution", "SA11AI", "ORG"),
                                       ("jfc_allocation", "SA12", "PAC")]:
            with self.subTest(role=role):
                parent = record("parent", "100", schedule=schedule, entity_type=entity,
                                employer="", role="unresolved")
                child = record("child", "100", schedule=schedule, memo=True, role=role,
                               related_record_id="parent", back_reference_transaction_id="parent", evidence=EVIDENCE)
                result = attribute([parent, child])
                self.assertIsNone(result.attributed_total)
                self.assertEqual(result.components["jfc_allocations"], Decimal("0"))
                self.assertEqual(result.components["partnership_attributions"], Decimal("0"))
                self.assertEqual(result.decisions[1].status, "unresolved")

    def test_unknown_sibling_correction_blocks_known_partnership_family(self):
        parent = record("parent", "100", entity_type="ORG", employer="")
        known = record("known", "100", memo=True, role="partnership_attribution",
                       related_record_id="parent", back_reference_transaction_id="parent", evidence=EVIDENCE)
        unknown = record("unknown", "-20", memo=True, employer="", back_reference_transaction_id="parent")
        result = attribute([parent, known, unknown])
        self.assertIsNone(result.attributed_total)
        self.assertTrue(next(x for x in result.issues if x.record_id == "unknown").blocks_attributed_total)

    def test_conflicting_employer_cannot_hide_behind_blank_linked_adjustment(self):
        rows = [record("gift", "100"),
                record("middle", "-10", employer="", role="adjustment", related_record_id="gift", evidence=EVIDENCE),
                record("last", "5", employer="OTHER EMPLOYER", role="adjustment", related_record_id="middle", evidence=EVIDENCE)]
        result = attribute(rows)
        self.assertIsNone(result.attributed_total)
        self.assertIn("conflicting_employer_attribution", {x.code for x in result.issues})

    def test_shared_conduit_and_nonrefund_expenses_do_not_join_unrelated_donor_families(self):
        rows = [record("nvidia", back_reference_transaction_id="conduit"),
                record("other", employer="OTHER EMPLOYER", back_reference_transaction_id="conduit"),
                record("conduit", "200", entity_type="PAC", employer="", memo=True),
                record("other_adjustment", "20", employer="OTHER EMPLOYER", memo=True, back_reference_transaction_id="other"),
                record("expense", "100", employer="", memo=True, schedule="SB17", back_reference_transaction_id="nvidia")]
        result = attribute(rows)
        self.assertEqual(result.attributed_total, Decimal("100"))

    def test_shared_jfc_transfer_does_not_merge_unrelated_donor_allocations(self):
        parent = record("transfer", "1000", entity_type="PAC", employer="", schedule="SA12")
        known = record("allocation", "25", memo=True, schedule="SA12", role="jfc_allocation",
                       related_record_id="transfer", back_reference_transaction_id="transfer", evidence=EVIDENCE)
        for employer in ("", "OTHER EMPLOYER"):
            with self.subTest(employer=employer):
                other = record("other_allocation", "50", memo=True, employer=employer,
                               schedule="SA12", back_reference_transaction_id="transfer")
                result = attribute([parent, known, other])
                self.assertEqual(result.attributed_total, Decimal("25"))
                self.assertFalse(next(x for x in result.issues if x.record_id == other.record_id).blocks_attributed_total)

    def test_raw_transaction_references_are_filing_scoped_not_global_identity(self):
        first = record("1:reused", "100", transaction_id="reused", file_number=1)
        second = record("2:reused", "100", transaction_id="reused", file_number=2,
                        employer="OTHER EMPLOYER")
        other_unknown = record("2:child", "20", memo=True, employer="", file_number=2,
                               back_reference_transaction_id="reused")
        self.assertEqual(attribute([first, second, other_unknown]).attributed_total, Decimal("100"))
        both = replace(second, employer="NVIDIA")
        self.assertEqual(attribute([first, both]).attributed_total, Decimal("200"))
        with self.assertRaises(ValueError):
            attribute([first, replace(first, record_id="another-row-id")])

    def test_missing_evidence_wrong_parent_and_cycles_fail_closed(self):
        base = record("gift")
        adjustment = record("adjust", "-20", role="adjustment", related_record_id="gift")
        self.assertIsNone(attribute([base, adjustment]).attributed_total)
        wrong_committee = replace(adjustment, evidence=EVIDENCE, committee_id="C99999999")
        self.assertIsNone(attribute([base, wrong_committee]).attributed_total)
        cycle_a = replace(adjustment, evidence=EVIDENCE, related_record_id="cycle")
        cycle_b = record("cycle", "20", role="adjustment", related_record_id="adjust", evidence=EVIDENCE)
        self.assertIsNone(attribute([cycle_a, cycle_b]).attributed_total)

    def test_invalid_parent_propagates_to_linked_children(self):
        invalid = record("parent", "-100", role="direct_receipt")
        child = record("child", "10", role="adjustment", related_record_id="parent", evidence=EVIDENCE)
        result = attribute([invalid, child])
        self.assertEqual(result.components["signed_adjustments"], Decimal("0.00"))
        self.assertIsNone(result.attributed_total)

    def test_decimal_serialization_and_invalid_input_guards(self):
        result = attribute([record("one", "0.10"), record("two", "0.20")])
        self.assertEqual(result.to_dict()["attributed_total"], "0.30")
        json.dumps(result.to_dict(), allow_nan=False)
        for invalid in [0.1, "NaN", "Infinity", True]:
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                record("bad", invalid)
        with self.assertRaises(ValueError):
            record("bad", memo="X")
        with self.assertRaises(ValueError):
            attribute([record("same"), record("same")])
        with self.assertRaises(ValueError):
            attribute_campaign([record("one")], {"NVIDIA": "nvidia", "nvidia ": "other"}, target_company="nvidia")


    def test_source_reported_reverse_org_conduit_is_context_with_strict_boundaries(self):
        # FEC 1903281, original rows 114/115. Address-free filed facts; source:
        # https://docquery.fec.gov/dcdev/posted/1903281.fec
        donor = record("1903281:A1F6DD9B7900B41D087A", "3333.00", file_number=1903281,
                       transaction_id="A1F6DD9B7900B41D087A", employer="A16Z")
        conduit = record("1903281:A3A9E191EFD4A495CB7F", "3333.00", file_number=1903281,
                         transaction_id="A3A9E191EFD4A495CB7F", entity_type="ORG", employer="",
                         memo=True, back_reference_transaction_id=donor.transaction_id,
                         memo_text="TOTAL EARMARKED THROUGH CONDUIT. PAC LIMIT NOT AFFECTED.")
        result = attribute([donor, conduit], "a16z")
        self.assertEqual(result.attributed_total, Decimal("3333.00"))
        self.assertEqual(result.decisions[1].role, "conduit_memo")
        for altered in (replace(conduit, memo_text=""), replace(conduit, amount="3334.00"),
                        replace(conduit, entity_type="IND")):
            with self.subTest(altered=altered):
                self.assertIsNone(attribute([donor, altered], "a16z").attributed_total)

    def test_negative_organizational_parent_and_transfer_cannot_silently_certify_total(self):
        gift = record("gift")
        for entity, schedule in (("ORG", "SA11AI"), ("PART", "SA11AI"), ("PAC", "SA12")):
            reversal = record("reversal", "-100", employer="", entity_type=entity, schedule=schedule)
            result = attribute([gift, reversal])
            self.assertEqual(result.components["direct_receipts"], Decimal("100"))
            self.assertIsNone(result.attributed_total)
            self.assertIn("unresolved_attribution_parent_reversal", {x.code for x in result.issues})
        # Ordinary PAC receipts on a different line do not imply employee origin.
        self.assertEqual(attribute([gift, record("pac", "-100", employer="", entity_type="PAC",
                                                  schedule="SA11C")]).attributed_total, Decimal("100"))

    def reattribution_rows(self, employer="MICROSOFT"):
        return [record("original", "100", identity_key="review:origin"),
                record("negative", "-40", memo=True, role="adjustment", related_record_id="original",
                       identity_key="review:origin", evidence=EVIDENCE),
                record("destination", "40", memo=True, role="reattribution", related_record_id="original",
                       identity_key="review:destination", evidence=EVIDENCE, employer=employer)]

    def test_reviewed_different_donor_reattribution_uses_destination_employer(self):
        rows = self.reattribution_rows()
        self.assertEqual(attribute(rows).attributed_total, Decimal("60"))
        self.assertEqual(attribute(rows, "microsoft").attributed_total, Decimal("40"))
        self.assertEqual(attribute(rows).components["signed_adjustments"], Decimal("-40"))
        for unknown in ("", "INFORMATION REQUESTED"):
            result = attribute(self.reattribution_rows(unknown))
            self.assertEqual(result.attributed_total, Decimal("60"))
            self.assertIsNone(result.decisions[-1].canonical_company)
            self.assertEqual(result.decisions[-1].status, "outside_employer_scope")

    def test_reattribution_requires_balanced_complete_reviewed_identity_family(self):
        original, negative, destination = self.reattribution_rows()
        bad_cases = [[original, destination],
                     [original, negative, replace(destination, amount="41")],
                     [original, negative, replace(destination, identity_key="")],
                     [original, negative, replace(destination, identity_key=original.identity_key)],
                     [original, replace(negative, identity_key=""), destination],
                     [original, negative, replace(destination, evidence=())],
                     [original, replace(negative, amount="-101"), replace(destination, amount="101")]]
        for rows in bad_cases:
            with self.subTest(rows=rows):
                self.assertIsNone(attribute(rows).attributed_total)
                self.assertIsNone(attribute(rows, "microsoft").attributed_total)

    def test_distinct_reviewed_identity_cannot_use_ordinary_adjustment_inheritance(self):
        rows = self.reattribution_rows("")
        rows[-1] = replace(rows[-1], role="adjustment")
        result = attribute(rows)
        self.assertIsNone(result.attributed_total)
        self.assertIn("different_adjustment_identity", {x.code for x in result.issues})

    def test_reviewed_organization_election_adjustment_requires_closed_nonindividual_family(self):
        transfer = record("transfer", "8000", schedule="SA12", entity_type="PAC", employer="")
        original = record("org", "7000", schedule="SA12", entity_type="ORG", employer="", memo=True,
                          role="context", identity_key="review:tribe", evidence=EVIDENCE,
                          back_reference_transaction_id="transfer")
        negative = record("minus", "-3500", entity_type="ORG", employer="", memo=True,
                          role="organization_adjustment", related_record_id="org",
                          identity_key="review:tribe", evidence=EVIDENCE)
        positive = replace(negative, record_id="plus", transaction_id="plus", amount="3500",
                           back_reference_transaction_id="org")
        rows = [record("gift"), transfer, original, negative, positive]
        self.assertEqual(attribute(rows).attributed_total, Decimal("100"))
        bad_cases = [rows[:-1], rows[:-1] + [replace(positive, back_reference_transaction_id="")],
                     rows[:-1] + [replace(positive, identity_key="unrelated")],
                     rows + [record("partner", "7000", memo=True, back_reference_transaction_id="org")],
                     [*rows[:2], replace(original, memo=False), negative, positive]]
        for broken in bad_cases:
            with self.subTest(broken=broken):
                self.assertIsNone(attribute(broken).attributed_total)

    def test_reviewed_exact_organization_memo_offset_keeps_transfer_donors_separate(self):
        transfer = record("transfer", "24000", schedule="SA12", entity_type="PAC", employer="", evidence=EVIDENCE)
        positive = record("org-plus", "7000", schedule="SA12", entity_type="ORG", employer="", memo=True,
                          role="organization_adjustment", related_record_id="transfer", evidence=EVIDENCE,
                          identity_key="review:tribe", back_reference_transaction_id="transfer")
        negative = replace(positive, record_id="org-minus", transaction_id="org-minus", amount="-7000")
        donor = record("donor", "100", schedule="SA12", memo=True, role="jfc_allocation",
                       related_record_id="transfer", back_reference_transaction_id="transfer", evidence=EVIDENCE)
        rows = [transfer, positive, negative, donor]
        self.assertEqual(attribute(rows).attributed_total, Decimal("100"))
        bad_cases = [[transfer, negative, donor],
                     [transfer, positive, replace(negative, memo_text="different purpose"), donor],
                     rows + [record("partner", "7000", memo=True, back_reference_transaction_id="org-plus")],
                     [replace(transfer, evidence=()), positive, negative, donor],
                     [transfer, positive, replace(negative, role="context"), donor]]
        for broken in bad_cases:
            with self.subTest(broken=broken):
                self.assertIsNone(attribute(broken).attributed_total)
        # A lone positive organizational memo remains unresolved, but does not
        # merge a separate donor into its family via the shared transfer.
        lone_positive = attribute([transfer, positive, donor])
        self.assertEqual(lone_positive.decisions[1].status, "unresolved")
        self.assertEqual(lone_positive.attributed_total, Decimal("100"))


class SourceBackedAttributionTests(unittest.TestCase):
    """Expected amounts were adjudicated from filings, not generated by engine."""

    FIXTURES = Path(__file__).parent / "fixtures" / "fec_attribution"
    CASES = {
        "nvidia_el_sayed_direct_receipts": ("nvidia", {"NVIDIA": "nvidia", "NVIDIA CORPORATION": "nvidia"}),
        "thayer_jfc_allocation": ("meta", {"META PLATFORMS": "meta"}),
        "a16z_partnership": ("a16z", {"ANDREESSEN HOROWITZ": "a16z"}),
        "flores_repeated_original": ("palantir", {"PALANTIR": "palantir"}),
        "vandiver_offsetting_adjustments": ("google", {"GOOGLE": "google"}),
        "unresolved_same_value_memo": ("amazon", {"AMAZON.COM": "amazon"}),
    }

    def load_case(self, name, *, with_review=True):
        payload = json.loads((self.FIXTURES / f"{name}.json").read_text(encoding="utf-8"))
        reviews = {row["record_id"]: row for row in payload["reviewed_context"]} if with_review else {}
        contract = {field.name for field in fields(AttributionRecord)}
        records = []
        for source in payload["records"]:
            review = reviews.get(source["record_id"], {})
            if review:
                self.assertEqual(review["source_row_sha256"], source["source_row_sha256"])
            values = {key: value for key, value in source.items() if key in contract}
            values["memo_text"] = source["source_fields"]["memo_text"]
            # Review changes interpretation, never the verified filed facts.
            values.update({key: value for key, value in review.items()
                           if key in {"role", "related_record_id", "identity_key", "evidence"}})
            records.append(AttributionRecord(**values))
        return payload, records

    def test_reviewed_source_cases_match_independent_literal_expectations(self):
        for name, (company, aliases) in self.CASES.items():
            with self.subTest(case=name):
                payload, records = self.load_case(name)
                result = attribute_campaign(records, aliases, target_company=company)
                expected = payload["expected"]
                for component in ("direct_receipts", "jfc_allocations", "partnership_attributions", "signed_adjustments"):
                    self.assertEqual(result.components[component], Decimal(expected[component]))
                total = expected["attributed_total"]
                self.assertEqual(result.attributed_total, None if total is None else Decimal(total))
                self.assertEqual(result.campaign_nonmemo_receipts, Decimal(expected["campaign_cash_receipts_in_excerpt"]))
                self.assertEqual(sum(issue.blocks_attributed_total for issue in result.issues),
                                 expected["unresolved_receipt_count_after_review"])
                if "election_attribution" in expected:
                    by_id = {row.record_id: row for row in records}
                    elections = {}
                    for decision in result.decisions:
                        if decision.status == "included" and decision.component != "refunds":
                            election = by_id[decision.record_id].election
                            elections[election] = elections.get(election, Decimal(0)) + decision.amount
                    self.assertEqual(elections, {key: Decimal(value) for key, value in expected["election_attribution"].items()})

    def test_flores_without_review_does_not_silently_suppress_a_memo(self):
        _, rows = self.load_case("flores_repeated_original", with_review=False)
        result = attribute_campaign(rows, {"PALANTIR": "palantir"}, target_company="palantir")
        self.assertIsNone(result.attributed_total)
        # Even the original carries redesignation text: without its source
        # review, the positive nonmemo record is not presumed to be new money.
        self.assertEqual(result.components["direct_receipts"], Decimal("0.00"))
        self.assertFalse(any(x.status == "excluded_repeated_original" for x in result.decisions))

    def test_actual_nvidia_receipts_and_source_linked_conduits_are_separate(self):
        _, rows = self.load_case("nvidia_el_sayed_direct_receipts")
        result = attribute_campaign(rows, {"NVIDIA": "nvidia", "NVIDIA CORPORATION": "nvidia"}, target_company="nvidia")
        self.assertEqual(sum(x.status == "included" for x in result.decisions), 23)
        self.assertEqual(sum(x.role == "conduit_memo" for x in result.decisions), 23)
        self.assertEqual(result.attributed_total, Decimal("33500"))
        self.assertIsNone(result.net_total)  # Receipt-only excerpt cannot certify net.


if __name__ == "__main__":
    unittest.main()
