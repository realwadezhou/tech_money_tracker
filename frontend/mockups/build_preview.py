"""Bundle the four frontend concepts as an offline page and optional inline preview."""
from pathlib import Path
import argparse

ROOT = Path(__file__).resolve().parent


def build(inline_path: Path | None = None) -> None:
    css = '\n'.join((ROOT / name).read_text(encoding='utf-8') for name in ['base.css', 'themes.css'])
    shell = (ROOT / 'shell.html').read_text(encoding='utf-8')
    js = '\n'.join((ROOT / name).read_text(encoding='utf-8') for name in ['prototype-data.js', 'prototype.js'])
    fragment = f'<style>\n{css}\n</style>\n{shell}\n<script>\n{js}\n</script>\n'
    standalone = '''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="light"><title>Tech Money · Four website concepts</title>
<style>html{color-scheme:light}body{margin:0;padding:26px;background:#f0f2f4}#tm-prototypes{max-width:1500px;margin:auto}@media(max-width:760px){body{padding:10px}}</style>
</head><body>''' + fragment + '</body></html>\n'
    (ROOT / 'index.html').write_text(standalone, encoding='utf-8')
    if inline_path:
        inline_path.parent.mkdir(parents=True, exist_ok=True)
        inline_path.write_text(fragment, encoding='utf-8')
    print(f'Built {ROOT / "index.html"} ({len(standalone.encode())} bytes)')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--inline', type=Path)
    args = parser.parse_args()
    build(args.inline)
