# Frontend Data

Generated frontend-ready exports no longer live in this directory.

They are now written to:

- `outputs/frontend_data/<cycle>/`

This keeps generated data with other pipeline outputs instead of mixing it into the frontend source tree.

The site builder still copies those exports into `frontend/site/data/<cycle>/` for the static site output.
