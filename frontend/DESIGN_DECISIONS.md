# Design Decisions

## Accepted direction: Reference

The user selected the light Reference design from the four prototypes and approved its readability revision. Apply it throughout the production site, including election pages, entity profiles, explanatory articles, downloads, and federal lobbying. The shared system is a modern, quiet public research site: readable prose, compact data, persistent navigation, and visible source context.

The comparison reference is [Longterm Wiki](https://www.longtermwiki.com/wiki/E838). The approved local prototype remains in `frontend/mockups/`; it records the visual direction, while the production generators and `frontend/assets/` implement the functioning site.

## Palette

- Background: `#ffffff`
- Main text: `#202124`
- Supporting text: `#566170`
- Links and selected navigation: `#315f9b`
- Selected navigation background: `#edf3fa`
- Borders: `#e2e7ed`
- Table headers: `#f6f8fa`
- Democrats: `#557fae`
- Republicans: `#be7d73`

Party colors retain their existing meaning; labels carry that meaning alongside color. The site explicitly uses a light theme, including when the operating system prefers dark mode.

## Typography and density

- Use the local system sans-serif stack, without external font requests.
- Reading text is 16px with a 1.7 line height.
- Tables use 14px text and tabular numerals; headers and supporting notes use 13px.
- Main headings are 32px and bold; section headings are 24px and semibold.
- Keep data compact through spacing, alignment, and thin horizontal rules rather than small, faint text.
- Monospace is reserved for code, identifiers, and specialized metadata.

## Shared page structure

- A compact header provides the site identity, primary links, and election cycle control where relevant.
- The desktop layout has a 208px browse sidebar, a flexible content column, and a 180px context column inside a maximum 1560px site width.
- Breadcrumbs and selected navigation identify the current location; in-page links help scan long pages.
- Source coverage and definitions stay available on every page. Below 1200px, that context moves beneath the main content.
- Below 760px, a native expandable navigation menu replaces the sidebar and content uses the full width.
- Summary figures use a simple divided row. Charts and tables occupy the document flow without decorative card containers.

## Interaction and accessibility

- Keep normal static page links, existing URLs, cycle switching, downloads, and original-source links.
- Tables retain sorting and row-wide filtering through a small vanilla JavaScript helper.
- Wide tables scroll inside their own container instead of widening the page.
- Links, buttons, native disclosures, and search inputs have visible keyboard focus. Selected navigation uses `aria-current`.
- Source notes and data caveats remain readable alongside the relevant information.
- Use native controls and minimal local JavaScript; avoid client-side application frameworks and third-party tracking.

## Boundaries

This adoption changes presentation and navigation, not financial calculations, data coverage, attribution, or the meaning of existing figures. The readability update supersedes the earlier Times New Roman, cream background, purple link, and heavy table grid rules.
