"""Pure, conservative attribution for an already selected campaign filing set.

This module does not select amendment versions, discover committees, establish
employment, or resolve identities from names. Callers provide latest original
filing records, all relevant relationship context, and evidence for adjudicated
relationships. A component sum is not a complete campaign or donor history.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Iterable, Mapping


DIRECT_LINES = frozenset({"SA11AI", "SA17AI", "SA17A"})
TRANSFER_LINES = frozenset({"SA12", "SA18"})
REFUND_LINES = frozenset({"SB20A", "SB28A"})
COMPONENTS = ("direct_receipts", "jfc_allocations", "partnership_attributions",
              "signed_adjustments", "refunds")
ROLES = frozenset({"auto", "direct_receipt", "conduit_memo", "jfc_allocation",
                   "partnership_attribution", "adjustment", "reattribution", "organization_adjustment", "repeated_original",
                   "refund", "transfer_context", "context", "unresolved"})
RELATED_ROLES = frozenset({"jfc_allocation", "partnership_attribution",
                          "adjustment", "reattribution", "organization_adjustment", "repeated_original", "refund"})


def _decimal(value: Decimal | str | int) -> Decimal:
    if isinstance(value, (float, bool)):
        raise ValueError("Amounts must be Decimal, decimal strings, or integers; not float/bool")
    try:
        amount = Decimal(value)
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("Invalid decimal amount") from exc
    if not amount.is_finite():
        raise ValueError("Amount must be finite")
    return amount


def _text(value: str) -> str:
    return str(value or "").strip().upper()


def _dollars(value: Decimal | None) -> str | None:
    return None if value is None else format(value, "f")


@dataclass(frozen=True)
class AttributionRecord:
    record_id: str
    committee_id: str
    file_number: int | str
    transaction_id: str
    schedule: str
    entity_type: str
    amount: Decimal | str | int
    employer: str = ""
    memo: bool = False
    back_reference_transaction_id: str = ""
    date: str = ""
    election: str = ""
    donor_name: str = ""  # Display only; never an identity or deduplication key.
    identity_key: str = ""  # Opaque caller evidence; never inferred from name.
    memo_text: str = ""
    role: str = "auto"
    related_record_id: str | None = None
    evidence: tuple[str, ...] = ()

    def __post_init__(self):
        if not self.record_id or not self.committee_id or not self.transaction_id:
            raise ValueError("Record, committee, and transaction identifiers are required")
        if type(self.memo) is not bool:
            raise ValueError("memo must be an explicit boolean")
        if self.role not in ROLES:
            raise ValueError(f"Unknown attribution role: {self.role}")
        if isinstance(self.evidence, str):
            raise ValueError("evidence must be a sequence of source references")
        object.__setattr__(self, "amount", _decimal(self.amount))
        object.__setattr__(self, "schedule", _text(self.schedule))
        object.__setattr__(self, "entity_type", _text(self.entity_type))
        object.__setattr__(self, "evidence", tuple(str(x) for x in self.evidence if x))


@dataclass(frozen=True)
class AttributionIssue:
    record_id: str
    code: str
    message: str
    blocks_attributed_total: bool = False
    blocks_net_total: bool = False

    def to_dict(self) -> dict:
        return dict(self.__dict__)


@dataclass(frozen=True)
class AttributionDecision:
    record_id: str
    role: str
    canonical_company: str | None
    status: str
    component: str | None = None
    amount: Decimal | None = None
    related_record_id: str | None = None
    evidence: tuple[str, ...] = ()

    def to_dict(self) -> dict:
        return dict(self.__dict__) | {"amount": _dollars(self.amount), "evidence": list(self.evidence)}


@dataclass(frozen=True)
class AttributionResult:
    target_company: str
    components: dict[str, Decimal]
    attributed_total: Decimal | None
    net_total: Decimal | None
    campaign_nonmemo_receipts: Decimal
    campaign_refunds: Decimal
    refund_scope_complete: bool
    decisions: tuple[AttributionDecision, ...]
    issues: tuple[AttributionIssue, ...]

    def to_dict(self) -> dict:
        return {
            "target_company": self.target_company,
            "components": {key: _dollars(value) for key, value in self.components.items()},
            "attributed_total": _dollars(self.attributed_total),
            "net_total": _dollars(self.net_total),
            "campaign_nonmemo_receipts": _dollars(self.campaign_nonmemo_receipts),
            "campaign_refunds": _dollars(self.campaign_refunds),
            "refund_scope_complete": self.refund_scope_complete,
            "decisions": [value.to_dict() for value in self.decisions],
            "issues": [value.to_dict() for value in self.issues],
            "scope": "Reported employer attribution within the supplied campaign filing set; employment, filing completeness and identity are not independently established.",
        }


def attribute_campaign(
    records: Iterable[AttributionRecord], employer_aliases: Mapping[str, str], *, target_company: str,
    refund_scope_complete: bool = False,
) -> AttributionResult:
    """Return disjoint supported components; withhold totals affected by unknowns.

    ``related_record_id`` plus ``evidence`` represents a caller's source-backed
    adjudication, not a hint for matching. Same names, amounts, or dates never
    create relationships. Missing parents and contradictory structure fail
    closed. Unlinked campaign refunds block only the refund-adjusted total.
    The caller must explicitly establish refund scope completeness before any
    net total is available. Nonmemo receipts are a diagnostic, not necessarily
    cash: they can include in-kind contributions and reporting adjustments.
    """
    rows = tuple(records)
    by_id = {row.record_id: row for row in rows}
    if len(by_id) != len(rows):
        raise ValueError("Duplicate source record_id; input records must not be concatenated twice")
    by_transaction = {(row.committee_id, str(row.file_number), row.transaction_id): row for row in rows}
    if len(by_transaction) != len(rows):
        raise ValueError("Duplicate transaction identity within one filing; source relationships are ambiguous")
    if not target_company:
        raise ValueError("A target canonical company is required")
    if type(refund_scope_complete) is not bool:
        raise ValueError("refund_scope_complete must be an explicit boolean")
    aliases: dict[str, str] = {}
    for alias, company in employer_aliases.items():
        normalized = _text(alias)
        if not normalized or not company:
            raise ValueError("Aliases and canonical companies must be nonempty")
        if normalized in aliases and aliases[normalized] != company:
            raise ValueError(f"Conflicting canonical companies for exact alias {normalized}")
        aliases[normalized] = company
    companies = {row.record_id: aliases.get(_text(row.employer)) for row in rows}
    roles: dict[str, str] = {}
    # A recipient's donor row points to its supporting conduit memo. Reversing
    # that explicit backreference is not name/amount matching.
    conduit_ids = {
        (row.committee_id, str(row.file_number), row.back_reference_transaction_id)
        for row in rows if row.entity_type == "IND" and not row.memo
        and row.schedule in DIRECT_LINES and row.back_reference_transaction_id
    }
    # Some filers reverse the link: the intermediary memo points to the donor.
    # Require the filed conduit description as well as a same-filing donor
    # parent; an arbitrary organizational memo is not supporting context.
    reverse_conduit_ids = set()
    for row in rows:
        parent = by_transaction.get((row.committee_id, str(row.file_number), row.back_reference_transaction_id))
        if (row.memo and row.entity_type in {"PAC", "ORG"} and row.schedule in DIRECT_LINES
                and "EARMARKED THROUGH CONDUIT" in _text(row.memo_text)
                and parent is not None and parent.entity_type == "IND" and not parent.memo
                and parent.schedule == row.schedule and row.amount == parent.amount and row.amount >= 0):
            reverse_conduit_ids.add(row.record_id)
    for row in rows:
        role = row.role
        if role == "auto":
            if row.schedule in REFUND_LINES:
                role = "refund"
            elif row.schedule in DIRECT_LINES and not row.memo and row.entity_type == "IND":
                possible_correction = any(token in _text(row.memo_text) for token in (
                    "REDESIGNAT", "RE-DESIGNAT", "REATTRIBUT", "RE-ATTRIBUT", "PREVIOUSLY REPORTED",
                ))
                role = "direct_receipt" if row.amount >= 0 and not possible_correction else "unresolved"
            elif row.record_id in reverse_conduit_ids or (row.memo and row.entity_type == "PAC" and (
                row.committee_id, str(row.file_number), row.transaction_id
            ) in conduit_ids):
                role = "conduit_memo"
            elif row.schedule in TRANSFER_LINES and not row.memo and row.entity_type in {"PAC", "PTY", "COM"}:
                role = "transfer_context"
            elif row.schedule in DIRECT_LINES and not row.memo and row.entity_type in {"ORG", "PART", "PAC", "PTY"}:
                role = "context"
            else:
                role = "unresolved"
        roles[row.record_id] = role

    issues: list[AttributionIssue] = []
    if not rows:
        issues.append(AttributionIssue("", "empty_source_scope",
                                       "No source records were supplied; zero attribution is not established.",
                                       blocks_attributed_total=True, blocks_net_total=True))
    if not refund_scope_complete:
        issues.append(AttributionIssue("", "refund_scope_incomplete",
                                       "The supplied records do not establish a complete refund review scope.",
                                       blocks_net_total=True))
    failures: dict[str, str] = {}

    # Filed backreferences establish a relevant relationship even before its
    # accounting meaning has been adjudicated. Use them only to propagate
    # uncertainty, never to infer an amount, identity, or duplicate deletion.
    # Reviewed links can cross reporting periods; raw IDs are filing-scoped.
    # Supporting conduit memos and JFC transfers can cover unrelated donors,
    # so they must not merge those donors into one attribution family. Unreviewed
    # Schedule B expense backreferences likewise do not create donor families.
    risk_links: dict[str, set[str]] = {row.record_id: set() for row in rows}
    shared_context = {
        row.record_id for row in rows if roles[row.record_id] == "conduit_memo"
        or (not row.memo and row.schedule in TRANSFER_LINES and row.entity_type in {"PAC", "COM", "PTY"})
    }
    for row in rows:
        related = [by_id.get(row.related_record_id)]
        if row.schedule.startswith("SA") and row.back_reference_transaction_id:
            parent = by_transaction.get((row.committee_id, str(row.file_number), row.back_reference_transaction_id))
            if parent is not None and parent.schedule.startswith("SA"):
                related.append(parent)
        for parent in related:
            if parent is not None and not {row.record_id, parent.record_id} & shared_context:
                risk_links[row.record_id].add(parent.record_id)
                risk_links[parent.record_id].add(row.record_id)
    target_family = {key for key, company in companies.items() if company == target_company}
    pending = list(target_family)
    while pending:
        for key in risk_links[pending.pop()] - target_family:
            target_family.add(key)
            pending.append(key)

    def linked_target(row: AttributionRecord) -> bool:
        return row.record_id in target_family

    def fail(row: AttributionRecord, code: str, message: str, *, refund: bool = False):
        if row.record_id in failures:
            return
        refund = refund or row.schedule in REFUND_LINES
        unknown_negative_origin = row.amount < 0 and (
            (row.entity_type == "IND" and row.schedule in DIRECT_LINES | TRANSFER_LINES)
            or (row.entity_type in {"ORG", "PART"} and row.schedule in DIRECT_LINES | TRANSFER_LINES)
            or (row.entity_type in {"PAC", "PTY", "COM"} and row.schedule in TRANSFER_LINES)
        )
        relevant = linked_target(row) or unknown_negative_origin
        if unknown_negative_origin:
            message += " Its unresolved donor origin may reduce an earlier employer-attributed contribution; a different or missing employer on the correction does not establish otherwise."
        issues.append(AttributionIssue(row.record_id, code, message,
                                       blocks_attributed_total=relevant and not refund,
                                       blocks_net_total=refund or relevant))
        failures[row.record_id] = code

    # Verify every reviewed relationship before deriving any monetary component.
    for row in rows:
        role = roles[row.record_id]
        if row.amount < 0 and role in {"context", "transfer_context"} and (
            (row.entity_type in {"ORG", "PART"} and row.schedule in DIRECT_LINES | TRANSFER_LINES)
            or (row.entity_type in {"PAC", "PTY", "COM"} and row.schedule in TRANSFER_LINES)
        ):
            fail(row, "unresolved_attribution_parent_reversal", "An organizational receipt or transfer reversal needs review of its donor-allocation consequences; a context label does not establish those consequences.")
        if row.schedule in REFUND_LINES and not row.memo and role != "refund":
            fail(row, "unclassified_refund", "A nonmemo refund cannot be omitted from donor-relationship review.", refund=True)
        if role == "direct_receipt" and not (
            row.entity_type == "IND" and not row.memo and row.schedule in DIRECT_LINES and row.amount >= 0
        ):
            fail(row, "invalid_direct_receipt", "Direct receipt requires a nonnegative, nonmemo individual contribution on a supported line.")
        elif role == "unresolved":
            fail(row, "unclassified_record", "Record has no source-supported attribution interpretation.")
        elif role == "conduit_memo" and not (row.record_id in reverse_conduit_ids or (
            row.memo and row.entity_type == "PAC" and
            (row.committee_id, str(row.file_number), row.transaction_id) in conduit_ids
        )):
            fail(row, "invalid_conduit_context", "Conduit context needs a supported same-filing individual receipt relationship and, for reverse links, an explicit conduit description.")
        elif role == "transfer_context" and not (
            not row.memo and row.entity_type in {"PAC", "PTY", "COM"} and row.schedule in TRANSFER_LINES
        ):
            fail(row, "invalid_transfer_context", "Transfer context must be a nonmemo committee transfer on a supported line.")
        elif role == "context" and row.entity_type == "IND" and row.amount != 0 and row.schedule in DIRECT_LINES | TRANSFER_LINES:
            fail(row, "unclassified_individual_context", "An individual receipt or attribution cannot be suppressed using a generic context label.")
        if role not in RELATED_ROLES:
            continue
        refund = role == "refund"
        parent = by_id.get(row.related_record_id)
        if parent is None:
            fail(row, "unlinked_refund" if refund else "missing_related_record",
                 "No retained source record establishes this relationship.", refund=refund)
            continue
        if not row.evidence:
            fail(row, "missing_relationship_evidence", "Relationship lacks a source or reviewed evidence reference.", refund=refund)
            continue
        if parent.committee_id != row.committee_id:
            fail(row, "cross_committee_relationship", "Attribution relationship crosses campaign account IDs.", refund=refund)
            continue
        seen = {row.record_id}
        cursor = parent
        while cursor is not None:
            if cursor.record_id in seen:
                fail(row, "relationship_cycle", "Source relationship is cyclic.", refund=refund)
                break
            seen.add(cursor.record_id)
            cursor = by_id.get(cursor.related_record_id)
        if row.record_id in failures:
            continue
        if role in {"jfc_allocation", "partnership_attribution"}:
            same_source_link = (str(row.file_number) == str(parent.file_number)
                                and row.back_reference_transaction_id == parent.transaction_id)
            if not same_source_link or not row.memo or row.entity_type != "IND" or parent.memo or row.amount < 0:
                fail(row, "invalid_allocation_relationship", "Allocation requires a nonnegative individual memo explicitly linked to a same-filing nonmemo parent; a negative correction needs a retained donor-adjustment origin.")
            elif role == "jfc_allocation" and not (
                row.schedule in TRANSFER_LINES and parent.schedule in TRANSFER_LINES
                and parent.entity_type in {"PAC", "PTY", "COM"}
            ):
                fail(row, "invalid_jfc_parent", "Joint-fundraising allocation lacks its campaign transfer parent.")
            elif role == "partnership_attribution" and not (
                row.schedule in DIRECT_LINES and parent.schedule in DIRECT_LINES
                and parent.entity_type in {"ORG", "PART"}
            ):
                fail(row, "invalid_partnership_parent", "Partnership allocation lacks its organizational receipt parent.")
        elif role == "repeated_original":
            if not (row.memo and not parent.memo and roles[parent.record_id] == "direct_receipt"
                    and row.entity_type == parent.entity_type == "IND"
                    and row.transaction_id == parent.transaction_id and row.amount == parent.amount
                    and row.date == parent.date and _text(row.employer) == _text(parent.employer)):
                fail(row, "invalid_repeated_original", "Reviewed repeated original disagrees with the retained original's source facts.")
            elif row.identity_key and parent.identity_key and row.identity_key != parent.identity_key:
                fail(row, "different_repeated_original_identity", "A memo assigned to a different reviewed donor cannot be excluded as this donor's repeated original.")
        elif role == "adjustment":
            if row.entity_type != "IND" or not row.schedule.startswith("SA"):
                fail(row, "invalid_adjustment", "A donor adjustment must be an individual receipt-schedule record.")
            elif row.identity_key and parent.identity_key and row.identity_key != parent.identity_key:
                fail(row, "different_adjustment_identity", "A correction assigned to a distinct donor requires an explicit reviewed reattribution family.")
        elif role == "reattribution":
            if not (row.memo and row.amount > 0 and row.entity_type == "IND"
                    and row.schedule in DIRECT_LINES | TRANSFER_LINES
                    and row.identity_key and parent.identity_key
                    and row.identity_key != parent.identity_key
                    and roles[parent.record_id] in {"direct_receipt", "jfc_allocation", "partnership_attribution"}):
                fail(row, "invalid_reattribution", "Reattribution requires a positive individual memo and explicit, distinct reviewed donor identities for the retained original and destination.")
        elif role == "organization_adjustment":
            transfer = by_transaction.get((parent.committee_id, str(parent.file_number), parent.back_reference_transaction_id))
            under_organization = (row.identity_key == parent.identity_key
                                  and parent.memo and parent.entity_type == "ORG"
                                  and parent.schedule in TRANSFER_LINES and parent.amount > 0
                                  and roles[parent.record_id] == "context" and transfer is not None
                                  and roles[transfer.record_id] == "transfer_context" and transfer.amount >= 0)
            under_transfer = (roles[parent.record_id] == "transfer_context" and not parent.memo
                              and parent.amount >= 0 and row.back_reference_transaction_id == parent.transaction_id)
            if not (row.memo and row.entity_type == "ORG" and row.schedule in DIRECT_LINES | TRANSFER_LINES
                    and row.identity_key and parent.evidence
                    and str(row.file_number) == str(parent.file_number)
                    and (under_organization or under_transfer)):
                fail(row, "invalid_organization_adjustment", "Organizational election corrections require a reviewed same-identity memo allocation under a retained committee transfer.")
        elif role == "refund":
            if row.memo or row.schedule not in REFUND_LINES:
                fail(row, "invalid_refund", "Refund must be a nonmemo record on a supported individual-refund line.", refund=True)
            elif row.identity_key and parent.identity_key and row.identity_key != parent.identity_key:
                fail(row, "different_refund_identity", "A refund assigned to a different reviewed donor cannot inherit this contribution's employer attribution.", refund=True)

    # A fully represented partnership family conserves the parent amount.
    families: dict[str, list[AttributionRecord]] = {}
    for row in rows:
        if roles[row.record_id] == "partnership_attribution" and row.related_record_id:
            families.setdefault(row.related_record_id, []).append(row)
    for parent_id, children in families.items():
        parent = by_id.get(parent_id)
        if parent is not None and sum((child.amount for child in children), Decimal(0)) != parent.amount:
            for child in children:
                fail(child, "incomplete_partnership_family", "Partner attribution amounts do not conserve the organizational receipt; complete source context is required.")

    # A reviewed transfer to another donor must have its complete signed family.
    # Destination employer is its own filed fact, never inherited from the donor
    # whose original is reduced. Balance is necessary evidence integrity, not
    # an automatic method for finding or adjudicating these relationships.
    reattributions: dict[str, list[AttributionRecord]] = {}
    for row in rows:
        if roles[row.record_id] == "reattribution" and row.related_record_id:
            reattributions.setdefault(row.related_record_id, []).append(row)
    for parent_id, destinations in reattributions.items():
        parent = by_id.get(parent_id)
        negatives = [row for row in rows if row.related_record_id == parent_id
                     and roles[row.record_id] == "adjustment" and row.amount < 0]
        valid = (parent is not None and negatives
                 and all(row.memo and row.identity_key == parent.identity_key and row.evidence
                         and row.record_id not in failures for row in negatives)
                 and sum((row.amount for row in destinations), Decimal(0))
                     == -sum((row.amount for row in negatives), Decimal(0))
                 and sum((row.amount for row in destinations), Decimal(0)) <= parent.amount
                 and all(row.record_id not in failures for row in destinations))
        if not valid:
            for row in [*destinations, *negatives]:
                fail(row, "incomplete_reattribution_family", "Reviewed reattribution needs complete balancing negative corrections with the original donor identity, and cannot exceed the retained original amount.")

    organizational: dict[tuple[str, str], list[AttributionRecord]] = {}
    for row in rows:
        if roles[row.record_id] == "organization_adjustment" and row.related_record_id:
            organizational.setdefault((row.related_record_id, row.identity_key), []).append(row)
    for (parent_id, _identity), children in organizational.items():
        parent = by_id.get(parent_id)
        under_transfer = parent is not None and roles[parent_id] == "transfer_context"
        # A transfer remains shared context; only the organization's own linked
        # rows form this family. Other donors under that transfer remain separate.
        family = {row.record_id for row in children} | (set() if under_transfer else {parent_id})
        todo = list(family)
        while todo:
            for key in risk_links.get(todo.pop(), set()) - family:
                family.add(key)
                todo.append(key)
        valid = (parent is not None and len(children) >= 2
                 and sum((row.amount for row in children), Decimal(0)) == 0
                 and 0 < sum((row.amount for row in children if row.amount > 0), Decimal(0)) <= parent.amount
                 and all(row.record_id not in failures for row in children)
                 and all(row.back_reference_transaction_id == parent.transaction_id for row in children if row.amount > 0)
                 and not any(by_id[key].entity_type == "IND" for key in family))
        if valid and under_transfer:
            # This narrower case is a reviewed exact memo offset, not an
            # inferred redesignation or a rule to cancel arbitrary equal values.
            facts = [{key: value for key, value in row.__dict__.items()
                      if key not in {"record_id", "transaction_id", "amount", "evidence"}}
                     for row in children]
            valid = len(children) == 2 and facts[0] == facts[1]
        if not valid:
            for row in children:
                fail(row, "incomplete_organization_adjustment_family", "Organizational corrections require a complete balanced family with explicit positive links and no individual attribution descendants.")

    def reviewed_identity(row: AttributionRecord, visiting: set[str]) -> str:
        # Carry only caller-reviewed identity through same-donor relationships.
        # A reattribution starts a new donor identity; shared transfer/conduit
        # context never supplies one. Blank intermediate keys cannot hide an
        # explicit contradiction farther along the reviewed chain.
        if row.record_id in visiting:
            return ""
        if row.identity_key:
            return row.identity_key
        if roles[row.record_id] in {"adjustment", "refund", "repeated_original"}:
            parent = by_id.get(row.related_record_id)
            if parent is not None:
                return reviewed_identity(parent, visiting | {row.record_id})
        return ""

    # Resolve an explicitly linked adjustment/refund's donor origin. A blank
    # employer may inherit through the reviewed relationship, never through name.
    def resolve_company(row: AttributionRecord, visiting: set[str]) -> str | None:
        own = companies[row.record_id]
        if roles[row.record_id] not in {"adjustment", "refund", "repeated_original"}:
            return own
        if row.record_id in failures or row.record_id in visiting:
            return own
        parent = by_id.get(row.related_record_id)
        if parent is None:
            return own
        parent_company = resolve_company(parent, visiting | {row.record_id})
        if parent.record_id in failures or roles[parent.record_id] not in {
            "direct_receipt", "jfc_allocation", "partnership_attribution", "adjustment", "repeated_original"
        }:
            fail(row, "unresolved_donor_origin", "Related record is not a resolved donor contribution or attribution.", refund=roles[row.record_id] == "refund")
            return own
        parent_identity = reviewed_identity(parent, set())
        if row.identity_key and parent_identity and row.identity_key != parent_identity:
            fail(row, "conflicting_donor_identity", "Reviewed donor identity conflicts with the resolved origin through an intermediate record.", refund=roles[row.record_id] == "refund")
            return own
        if _text(row.employer) and own != parent_company:
            fail(row, "conflicting_employer_attribution", "Linked records report conflicting company attribution; a donor name cannot resolve it.", refund=roles[row.record_id] == "refund")
            return own
        return own if _text(row.employer) else parent_company

    resolved = {row.record_id: resolve_company(row, set()) for row in rows}
    # Resolving parents can expose failures; propagate them through dependent rows.
    for _ in range(len(rows)):
        before = len(failures)
        for row in rows:
            if roles[row.record_id] in RELATED_ROLES:
                parent = by_id.get(row.related_record_id)
                if parent is not None and parent.record_id in failures:
                    fail(row, "unresolved_donor_origin", "A required donor-origin record remains unresolved.", refund=roles[row.record_id] == "refund")
        if len(failures) == before:
            break

    components = {key: Decimal("0.00") for key in COMPONENTS}
    decisions: list[AttributionDecision] = []
    role_component = {"direct_receipt": "direct_receipts", "jfc_allocation": "jfc_allocations",
                      "partnership_attribution": "partnership_attributions",
                      "adjustment": "signed_adjustments", "reattribution": "signed_adjustments", "refund": "refunds"}
    for row in rows:
        role = roles[row.record_id]
        company = resolved[row.record_id]
        if row.record_id in failures:
            status, component, amount = "unresolved", None, None
        elif role == "repeated_original":
            status, component, amount = "excluded_repeated_original", None, None
        elif role in {"context", "transfer_context", "conduit_memo", "organization_adjustment"}:
            status, component, amount = "context", None, None
        elif company != target_company:
            status, component, amount = "outside_employer_scope", None, None
        else:
            component = role_component[role]
            amount = -row.amount if role == "refund" else row.amount
            components[component] += amount
            status = "included"
        decisions.append(AttributionDecision(row.record_id, role, company, status, component,
                                              amount, row.related_record_id, row.evidence))
    # These are source diagnostics, separate from company donor attribution.
    cash = sum((row.amount for row in rows if not row.memo and row.schedule in DIRECT_LINES | TRANSFER_LINES), Decimal("0.00"))
    campaign_refunds = sum((-row.amount for row in rows if not row.memo and row.schedule in REFUND_LINES), Decimal("0.00"))
    attributed = None if any(issue.blocks_attributed_total for issue in issues) else sum(
        (components[key] for key in COMPONENTS if key != "refunds"), Decimal("0.00"))
    net = None if attributed is None or any(issue.blocks_net_total for issue in issues) else attributed + components["refunds"]
    return AttributionResult(target_company, components, attributed, net, cash, campaign_refunds, refund_scope_complete,
                             tuple(decisions), tuple(issues))
