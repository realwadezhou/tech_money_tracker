# Frontend Data

Generated frontend-ready exports no longer live in this directory.

They are now written to:

- `exports/site/<cycle>/`

This keeps publishable site exports outside the frontend source tree while still making them easy for the static site builder to consume.

The site builder copies those exports into `frontend/site/<cycle>/data/` as part of the generated static site.
