# Four connected Tech Money website prototypes

Open `index.html` directly, or serve the repository and open `/frontend/mockups/`.
The page is self-contained and needs no packages or external assets.

The concept selector keeps the current page, record, and election cycle, so each
design can be compared on the same content. Appearance defaults to light;
the design-study selector also offers system and dark views.

| Concept | Visual direction | Scalable page structure |
| --- | --- | --- |
| Reference | Quiet blue links, white space, document hierarchy | Persistent section navigation, main record, contextual outline |
| Atlas | Teal accents, compact controls, bounded data sections | Shared workspace shell with directory, profile, and source templates |
| Ledger | Warm paper, serif headings, open tables | Horizontal publication navigation, wide content, source margin |
| Index | Compact rail, square controls, monospaced figures | Wide directories and repeatable entity records; table-first overview |

## Working interactions

- Overview, employer directory, employer profile, candidate directory/profile,
  donor directory/profile, committee directory/profile, lobbying, methodology,
  data downloads, and about pages.
- URL-addressable hash routes, browser back/forward, and cycle switching.
- Employer sector filters, candidate office filters, text search, sortable tables,
  CSV downloads of displayed records, and expanded lobbying passages.
- Real 2024 and 2026 export snapshots. All employers are included; selected detail
  pages and leading twelve candidate/donor/committee rows are included. Lobbying
  includes eight real passages, labeled as a sample. Original source links open
  externally. This is a design prototype, not a production replacement.

## Source files

- `shell.html`: shared semantic application shell.
- `base.css`: shared layout and controls.
- `themes.css`: four responsive visual systems.
- `prototype.js`: navigation, page templates, filters, exports, and local state.
- `prototype-data.js`: compact data derived from repository exports.
- `build_preview.py`: bundles the offline page and optional inline fragment.

Rebuild with `python frontend/mockups/build_preview.py`.

## Production integration

Retain the current Python static generator and ordinary page URLs. Apply the
selected theme to shared header/navigation, directory, profile, and document
templates. Feed them the existing export schema, and reuse the current table and
lobbying helpers. The prototype's hash router and snapshot bundle are solely for
comparing designs; they do not require a framework migration.

Maintain the current distinctions between employer-matched contributions,
candidate-associated account receipts, and lobbying passages. Carry through
source dates, coverage, unavailable values, and recipient-subtotal provenance.

Visual reference: https://www.longtermwiki.com/wiki/E838 — restrained navigation,
document structure, and linked reference material. No page content or assets copied.
