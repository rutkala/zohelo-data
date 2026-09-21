"""Shared immutable native-object identity rules for the DBW stage boundary."""
from __future__ import annotations

import hashlib
from pathlib import Path
import re
from typing import Any


def versioned_name(name: str, sha256_hex: str) -> str:
    """Bind logical and content identities in a domain-separated 240-byte name."""
    path = Path(name)
    full_suffix = "".join(path.suffixes)
    logical_digest = hashlib.sha256(name.encode("utf-8")).hexdigest()
    marker = (
        f"--logical-sha256-{logical_digest}"
        f"--content-sha256-{sha256_hex}"
    )
    suffix = full_suffix
    if 240 - len((marker + suffix).encode("utf-8")) < 1:
        suffix = path.suffix
    stem = name[: -len(suffix)] if suffix else name
    max_stem_bytes = 240 - len((marker + suffix).encode("utf-8"))
    if max_stem_bytes < 1:
        raise RuntimeError("DBW versioned filename has no room for a logical stem.")
    stem_bytes = stem.encode("utf-8")
    if len(stem_bytes) > max_stem_bytes:
        stem = stem_bytes[:max_stem_bytes].decode("utf-8", errors="ignore")
    if not stem:
        raise RuntimeError("DBW versioned filename cannot retain a safe logical stem.")
    versioned = f"{stem}{marker}{suffix}"
    if len(versioned.encode("utf-8")) > 240:
        raise RuntimeError("DBW versioned filename exceeds the storage limit.")
    return versioned


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
    if object_name == base_name:
        return True
    try:
        return object_name == versioned_name(base_name, digest)
    except RuntimeError:
        return False
