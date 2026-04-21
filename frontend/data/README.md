# frontend/data/

This directory is empty on purpose. Site-ready data exports no longer live
here.

They are written to **`exports/site/<cycle>/`** and consumed by the static-site
builder, which copies the needed pieces into `frontend/site/<cycle>/data/` as
part of generating the site.

This split keeps publishable exports outside the frontend source tree while
still making them easy for the builder to pick up.
