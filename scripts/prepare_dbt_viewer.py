#!/usr/bin/env python3
"""Stage dbt's bundled Docs viewer without generating local dbt artifacts."""

from __future__ import annotations

import re
import sys
from pathlib import Path

from dbt.task.docs.generate import DOCS_INDEX_FILE_PATH

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = ROOT / "portal" / "public" / "docs" / "viewer-template.html"
EXPECTED_TITLE = "dbt Docs"
EXPECTED_PLACEHOLDERS = {
    "MANIFEST.JSON INLINE DATA",
    "CATALOG.JSON INLINE DATA",
}
PLACEHOLDER_PATTERN = re.compile(r"[A-Z][A-Z0-9_]*\.JSON INLINE DATA")
TITLE_PATTERN = re.compile(r"<title\b[^>]*>\s*dbt\s+docs\s*</title>", re.IGNORECASE)


def fail(message: str) -> None:
    raise RuntimeError(f"Cannot prepare the dbt Docs viewer template: {message}")


def main() -> int:
    source_path = Path(DOCS_INDEX_FILE_PATH)
    if not source_path.is_file():
        fail(f"the installed dbt Docs template is missing at {source_path}")

    template = source_path.read_text(encoding="utf-8")
    if len(TITLE_PATTERN.findall(template)) != 1:
        fail(f"expected exactly one {EXPECTED_TITLE!r} document title")

    found_placeholders = PLACEHOLDER_PATTERN.findall(template)
    unexpected = sorted(set(found_placeholders) - EXPECTED_PLACEHOLDERS)
    missing = sorted(EXPECTED_PLACEHOLDERS - set(found_placeholders))
    duplicates = sorted(
        placeholder
        for placeholder in EXPECTED_PLACEHOLDERS
        if found_placeholders.count(placeholder) != 1
    )
    if unexpected:
        fail(f"unexpected inline-data placeholders: {', '.join(unexpected)}")
    if missing:
        fail(f"missing inline-data placeholders: {', '.join(missing)}")
    if duplicates:
        fail(f"inline-data placeholders must each occur once: {', '.join(duplicates)}")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = OUTPUT_PATH.with_suffix(".html.tmp")
    temporary_path.write_text(template, encoding="utf-8")
    temporary_path.replace(OUTPUT_PATH)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as error:
        print(error, file=sys.stderr)
        raise SystemExit(1)
