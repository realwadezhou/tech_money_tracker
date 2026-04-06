from __future__ import annotations

import shutil
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SITE_ROOT = REPO_ROOT / "frontend" / "site"
DOCS_ROOT = REPO_ROOT / "docs"


def main() -> None:
    if not SITE_ROOT.exists():
        raise SystemExit(
            f"Built site not found at {SITE_ROOT}. Run `python -m frontend.build_site` first."
        )

    if DOCS_ROOT.exists():
        shutil.rmtree(DOCS_ROOT)

    shutil.copytree(SITE_ROOT, DOCS_ROOT)
    (DOCS_ROOT / ".nojekyll").write_text("", encoding="utf-8")

    print(f"Copied {SITE_ROOT} -> {DOCS_ROOT}")


if __name__ == "__main__":
    main()
