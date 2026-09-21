"""Shared immutable native-object identity rules for the DBW stage boundary."""
from __future__ import annotations

from pathlib import Path
import re
from typing import Any


def versioned_name(name: str, sha256_hex: str) -> str:
    """Keep a logical name recognizable while making a changed revision immutable."""
    path = Path(name)
    suffix = "".join(path.suffixes)
    stem = name[: -len(suffix)] if suffix else name
    marker = f"--sha256-{sha256_hex}"
    max_stem = max(1, 240 - len(marker) - len(suffix))
    return f"{stem[:max_stem]}{marker}{suffix}"


def indicator_scoped_bulk_name(indicator_id: int, source_name: str) -> str:
    """Give each indicator exclusive durable ownership of its provider ZIP."""
    if (
        not isinstance(indicator_id, int)
        or not isinstance(source_name, str)
        or not source_name
        or Path(source_name).name != source_name
        or "\\" in source_name
    ):
        raise RuntimeError("DBW bulk identity requires a safe indicator and provider filename.")
    name = f"indicator-{indicator_id}--{source_name}"
    if len(name.encode("utf-8")) > 240:
        raise RuntimeError("DBW indicator-scoped bulk filename exceeds the storage limit.")
    return name


def is_indicator_scoped_bulk_descriptor(
    indicator_id: int, descriptor: dict[str, Any]
) -> bool:
    """Return whether a receipt binds a current indicator-owned bulk identity."""
    source_name = descriptor.get("source_name")
    object_name = descriptor.get("name")
    digest = descriptor.get("sha256")
    if (
        descriptor.get("role") != "bulk_zip"
        or not isinstance(object_name, str)
        or not isinstance(digest, str)
        or re.fullmatch(r"[0-9a-f]{64}", digest) is None
    ):
        return False
    try:
        base_name = indicator_scoped_bulk_name(indicator_id, source_name)
    except RuntimeError:
        return False
    return object_name in {base_name, versioned_name(base_name, digest)}
