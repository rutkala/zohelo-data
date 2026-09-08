"""Bounded, transport-only credentials for source ingestion.

Secrets are read from the environment when one of the public functions is called.
They are never added to an adapter request specification or effective settings.
"""

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, field
import os
from typing import Callable
from urllib.parse import urlsplit


_BDL_SOURCE_ID = "gus_bdl"
_BDL_SECRET_NAME = "GUS_BDL_API_KEY"
_BDL_HEADER_NAME = "X-ClientId"
_BDL_ORIGIN = "bdl.stat.gov.pl"
_BDL_PATH = "/api/v1"
_REGISTERED_BDL_QUOTA_WINDOWS = (
    {"seconds": 900, "requests": 400},
    {"seconds": 43_200, "requests": 4_000},
    {"seconds": 604_800, "requests": 40_000},
)
_CURRENT_SOURCE_IDS = frozenset(("world_bank_wdi", _BDL_SOURCE_ID, "eurostat"))


class SourceCredentialError(ValueError):
    """A credential or credential-bound request violates the source policy."""


@dataclass(frozen=True)
class SourceAccessStatus:
    """Non-secret source access information safe for reports and logs."""

    mode: str
    secret_name: str | None


@dataclass(frozen=True)
class _ResolvedCredential:
    status: SourceAccessStatus
    _value: str | None = field(default=None, repr=False, compare=False)


def _environment(environ: Mapping[str, str] | None) -> Mapping[str, str]:
    # Resolve the process environment at call time rather than module import time.
    return os.environ if environ is None else environ


def _validated_bdl_credential(environ: Mapping[str, str] | None) -> _ResolvedCredential:
    source = _environment(environ)
    # GitHub Actions renders an unavailable expression-backed secret as an empty
    # environment value. Treat that exact value like an absent optional secret.
    if _BDL_SECRET_NAME not in source or source[_BDL_SECRET_NAME] == "":
        return _ResolvedCredential(SourceAccessStatus("anonymous", _BDL_SECRET_NAME))

    value = source[_BDL_SECRET_NAME]
    # A bounded visible-ASCII header value excludes whitespace/control injection and
    # produces a fixed error that never contains the rejected value.
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= 512
        or value.strip() != value
        or any(ord(character) < 0x21 or ord(character) > 0x7E for character in value)
    ):
        raise SourceCredentialError("Configured source credential is invalid")
    return _ResolvedCredential(
        SourceAccessStatus("registered", _BDL_SECRET_NAME),
        value,
    )


def _resolved(source_id: str, environ: Mapping[str, str] | None) -> _ResolvedCredential:
    if source_id == _BDL_SOURCE_ID:
        return _validated_bdl_credential(environ)
    if source_id in _CURRENT_SOURCE_IDS:
        return _ResolvedCredential(SourceAccessStatus("public", None))
    raise SourceCredentialError("Source credential policy is not configured")


def source_access_status(
    source_id: str, *, environ: Mapping[str, str] | None = None
) -> SourceAccessStatus:
    """Return only non-secret runtime status for a configured source."""

    return _resolved(source_id, environ).status


def effective_source_settings(
    source_id: str,
    settings: Mapping[str, object],
    *,
    environ: Mapping[str, str] | None = None,
) -> dict[str, object]:
    """Copy settings and select the bounded profile for current source access.

    This function has no campaign-state argument and cannot clear or rewrite durable
    ``quota_attempts`` when access changes between runs.
    """

    resolved = _resolved(source_id, environ)
    effective = deepcopy(dict(settings))
    if source_id == _BDL_SOURCE_ID and resolved.status.mode == "registered":
        effective["min_request_interval_seconds"] = 1
        effective["quota_windows"] = [
            dict(window) for window in _REGISTERED_BDL_QUOTA_WINDOWS
        ]
    return effective


def source_request_headers(
    source_id: str,
    url: str,
    *,
    environ: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Return credential headers for one transport request.

    The caller must pass this result directly to the no-redirect HTTP transport. It
    must not merge it into the adapter request specification or a durable receipt.
    """

    resolved = _resolved(source_id, environ)
    if resolved._value is None:
        return {}
    if source_id != _BDL_SOURCE_ID or not isinstance(url, str):
        raise SourceCredentialError("Source credential cannot be sent to this request")
    try:
        parsed = urlsplit(url)
        valid_port = parsed.port is None
    except (TypeError, ValueError):
        raise SourceCredentialError(
            "Source credential cannot be sent to this request"
        ) from None
    if (
        parsed.scheme != "https"
        or parsed.netloc != _BDL_ORIGIN
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or not valid_port
        or not (
            parsed.path == _BDL_PATH
            or parsed.path.startswith(_BDL_PATH + "/")
        )
    ):
        raise SourceCredentialError("Source credential cannot be sent to this request")
    return {_BDL_HEADER_NAME: resolved._value}


def make_authenticated_fetch(
    source_id: str,
    base_fetch: Callable[..., tuple[int, bytes, dict[str, str]]],
    *,
    environ: Mapping[str, str] | None = None,
) -> Callable[..., tuple[int, bytes, dict[str, str]]]:
    """Wrap the no-redirect transport with just-in-time source authentication.

    ``base_fetch`` must accept an ephemeral ``headers`` keyword separately from the
    persisted adapter request specification.
    """

    def authenticated_fetch(request_spec, allowed_hosts, *, max_bytes, timeout):
        headers = source_request_headers(
            source_id, request_spec["url"], environ=environ
        )
        return base_fetch(
            request_spec,
            allowed_hosts,
            max_bytes=max_bytes,
            timeout=timeout,
            headers=headers,
        )

    return authenticated_fetch
