# Individual Donor Review Experiment

## Prominent-person pilot (September 20, 2026)

The newer evidence-scoped workflow is documented in [METHODOLOGY.md](METHODOLOGY.md).
Start with the [20-person review index](pilot/README.md) or the
[interactive audit](pilot/review.html). It records source references, criteria,
accepted/rejected/unresolved decisions, and one dossier per person.

```powershell
python experiments/individual_review/app.py --watchlist --port 8766
```

The pilot retains raw records and uses stable person IDs and scoped evidence
decisions. It does not import the legacy alias decisions below or change production
totals. The rest of this README describes the older broad discovery experiment.

This is an isolated prototype for exploring individual donor consolidation.
Nothing here is imported by the production pipeline, and nothing here writes to
`data/reference`, `exports`, or `docs`.

## Prominent Recall Warning

This app is **not yet a complete-recall system**.

The current raw-data build seeds review candidates from:

- exact raw contributor names whose net giving reaches the major-donor threshold
  (default: `$100,000`)
- exact raw contributor names with curated tech-employer matches
- exact raw contributor names with possible tech context in employer/occupation
- additional aliases that share a parsed `last + first` key with one of those
  seeds

That means we can still miss salient people. The most important failure mode:
if every exact alias for a person is below `$100,000`, and those rows do not
have tech-employer or possible-tech context, that person will not enter this
queue. We may also miss low-dollar aliases for an otherwise salient person when
the alias does not share the current parsed strong-name key because of a typo,
initial-only first name, unusual nickname, changed surname, or bad source data.

For now, treat this as a high-priority review queue, not as proof that all
salient individual donors have been captured.

## Files

| File | Purpose |
|---|---|
| `build_candidates.py` | Generates donor review candidates and an unreviewed queue. |
| `app.py` | Local browser app for reviewing candidates. |
| `curated_individuals.csv` | Experimental durable human decisions. |
| `cluster_alias_decisions.csv` | Experimental per-alias yes/no/research decisions against a proposed canonical identity. |
| `state/name_universe_<cycle>.csv` | Generated cached exact-name universe for each FEC cycle. Ignored by git. |
| `state/tuple_evidence_<cycle>.csv` | Generated tuple evidence only for selected review candidates. Ignored by git. |
| `candidates.csv` | Generated candidate table. Ignored by git. |
| `review_queue.csv` | Generated queue after removing final reviewed decisions. Ignored by git. |

## Quick Start

Build from raw FEC interim data for the 2024 and 2026 cycles:

```bash
python experiments/individual_review/build_candidates.py
```

This is now the preferred path. It scans the raw `itcont.txt` files directly.

Use the older manual top-donor CSVs only for a fast UI demo:

```bash
python experiments/individual_review/build_candidates.py --manual
python experiments/individual_review/app.py
```

Then open `http://127.0.0.1:8765/`.

You can also specify cycles explicitly:

```bash
python experiments/individual_review/build_candidates.py --cycle 2024 --cycle 2026
```

The raw build reads `data/fec/interim/<cycle>/indiv<yy>/itcont.txt` in chunks,
applies the same basic transaction sign rules as the main FEC pipeline, and
adds tech-linked hints by reading `data/reference/companies/curated.csv`.

Generated state is cached in `experiments/individual_review/state/`. If a raw
FEC file has the same path, size, and modified time as the last run, the cached
stage is reused. Use `--force-state` when you intentionally want to rebuild:

```bash
python experiments/individual_review/build_candidates.py --cycle 2024 --cycle 2026 --force-state
```

## Review Philosophy

The generated queue is allowed to be noisy. The durable file is not.

Algorithms here only suggest clusters. A name consolidation should affect the
future production pipeline only after a human decision is recorded in
`curated_individuals.csv`.

Final review statuses are omitted from regenerated `review_queue.csv`:

- `reviewed_not_tech`
- `tech_figure`
- `same_identity`
- `not_individual`

Non-final statuses stay in the queue:

- `needs_research`
- `needs_split`

## Matching Notes

The prototype generates cheap OpenRefine-style and donor-specific keys. The
raw-data build is deliberately staged so it does not aggregate every
name-state-employer-occupation tuple before it knows which names are worth
reviewing.

Current algorithm, in order:

1. Preserve the raw FEC `contributor_name`.
   This stays visible in the app and is what a future lookup should map from.
2. Build a separate `normalized_name` helper:
   uppercase, trim whitespace, strip accents, collapse repeated spaces, and
   remove most punctuation noise.
3. Parse FEC-style names:
   `LAST, FIRST MIDDLE/TITLE/SUFFIX`.
4. Ignore common suffixes/titles in the given-name side:
   `JR`, `SR`, `II`, `III`, `MR`, `MRS`, `MS`, `DR`, `MD`, `PHD`, `ESQ`, etc.
5. Build name keys:
   - `strong_name_key = last_name + canonical_first`
   - `loose_name_key = last_name + first_initial`
   - `name_fingerprint = sorted unique normalized name tokens`
6. Apply a small nickname/canonical-first map:
   `BEN -> BENJAMIN`, `BOB -> ROBERT`, `MARK -> MARC`, etc.
7. Build a cached per-cycle exact-name universe:
   one row per raw contributor name, with totals and lightweight evidence
   summaries. This is `state/name_universe_<cycle>.csv`.
8. Identify salient seed names:
   - exact raw names above the major-donor threshold
   - exact raw names with a matched tech employer
   - exact raw names with possible tech context in employer/occupation
9. Expand around those salient seeds:
   every raw name sharing a seed's `strong_name_key` is included, even if the
   alias itself is low-dollar.
10. Collect tuple evidence only for the selected review universe:
   `state/tuple_evidence_<cycle>.csv`.
11. Suggest clusters by shared `strong_name_key`.
   Example: `HOFFMAN, REID`, `HOFFMAN, REID G`, and `HOFFMAN, REID G.`
   all share `HOFFMAN|REID`.

For high-stakes data, treat these as prioritization signals, not facts.

This two-pass shape matters mathematically. The old prototype could create a
large intermediate table across:

```text
name x state x zip x employer x occupation x cycle
```

That can approach the number of raw rows. The revised build first collapses to
roughly:

```text
name x cycle
```

and only then revisits the raw files for the much smaller set of selected names.
It still scans the raw files, but it avoids keeping a combinatorial tuple table
for everybody.

### Matching Algorithm Tiers

Different matching algorithms find different kinds of potential aliases:

| Tier | Examples | What it catches | Risk |
|---|---|---|---|
| Exact normalized key | raw name after light normalization | case/spacing/punctuation variants | low |
| Parsed strong key | `last + first`, titles stripped | middle initials, `MR`, `DR`, suffix noise | low/medium |
| Nickname key | `BEN -> BENJAMIN`, `MARK -> MARC` | common first-name variants | medium |
| Loose initial key | `last + first initial` | initials and abbreviated first names | high for common names |
| Fingerprint / n-gram / edit distance | OpenRefine-style fuzzy keys | misspellings and typos | high |
| Context-assisted fuzzy | fuzzy name plus employer/state/zip evidence | misspellings with corroborating context | medium |

The current app uses exact normalized keys, parsed strong keys, and a small
nickname map. It does **not** yet use loose initial expansion or fuzzy/edit
distance matching for queue inclusion. Those should be added as additional
review queues, not as automatic merges.

### Current Important Limitation Of `--manual`

The quick command:

```bash
python experiments/individual_review/build_candidates.py --manual
```

uses the old `manual_tagging/top_donors_summary.csv`, which only contains the
top 2,000 exact contributor names from that earlier manual pass. If an alias is
not in that file, it cannot appear in the app, even if the parser would cluster
it correctly.

For example, `ANDREESSEN, MARC L. MR.` parses to the same strong key as
`ANDREESSEN, MARC`:

```text
ANDREESSEN|MARC
```

If it is missing from a `--manual` app build, that is because the seed file
does not contain that exact contributor name. Rebuilding from raw FEC data is
the path that should surface it:

```bash
python experiments/individual_review/build_candidates.py --cycle 2024 --cycle 2026
```

## App Workflow

The app has three tabs:

- **Needs review**: generated candidates with no current decision.
- **Needs research**: rows you explicitly parked as `needs_research` or
  `needs_split`.
- **Reviewed**: final decisions. Use **Revert decision** here to remove an
  experimental decision and send the row back to Needs review.

The app now separates two concepts:

- **Canonical person decision** in `curated_individuals.csv`.
- **Alias membership decision** in `cluster_alias_decisions.csv`.

For a selected donor, the suggested cluster is presented as a set of alias
cards. Each alias can be marked **Yes**, **No**, or **Research** for the
proposed canonical identity. This is closer to the intended workflow:

> Is this raw alias evidence part of our canonical Reid Hoffman?

The page is laid out in review order:

1. **Canonical Target**: confirm the person and read the selected candidate's
   evidence.
2. **Alias Adjudication**: decide whether each suggested alias belongs to that
   canonical person.
3. **Final Person Decision**: click one final action. There is no separate
   status dropdown; the final buttons are the save actions.

The generator also adds `variant_evidence`, which lists the underlying
amount/state/employer/occupation tuples for that raw contributor name. The next
likely upgrade is to move from alias-level decisions to tuple-level decisions,
where each tuple can be accepted into, excluded from, or split out of a
canonical identity.

In this context, a **tuple** means one distinct bundle of filing evidence, such
as:

```text
contributor_name + state/zip + employer + occupation
```

For example, `HOFFMAN, REID | CA | LINKEDIN | CHAIRMAN` and
`HOFFMAN, REID | WA | GREYLOCK | PARTNER` are two separate evidence tuples
under the same raw alias. Tuple adjudication would let the reviewer mark each
bundle as belonging to the canonical identity, not belonging, or needing
research.
