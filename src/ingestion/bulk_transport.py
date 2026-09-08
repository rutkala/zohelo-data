"""Streaming transport and immutable Drive storage for official bulk files.

Bulk responses are deliberately kept out of the in-memory campaign object store.
The local path is disposable working state; the verified, content-addressed Drive
object is the durable copy.
"""
from __future__ import annotations

from email.utils import parsedate_to_datetime
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
from typing import Any, Mapping
from urllib.error import HTTPError
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


DEFAULT_DOWNLOAD_CHUNK_BYTES = 1024 * 1024
DEFAULT_DRIVE_CHUNK_BYTES = 8 * 1024 * 1024
MAX_METADATA_BYTES = 1024 * 1024
_SOURCE_ID_RE = re.compile(r"^[a-z][a-z0-9_]{0,119}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MD5_RE = re.compile(r"^[0-9a-f]{32}$")
_STRONG_ETAG_RE = re.compile(r'^"[^"\r\n]{1,1022}"$')
_SENSITIVE_KEYS = {
    "authorization",
    "proxy-authorization",
    "cookie",
    "set-cookie",
    "x-api-key",
    "api-key",
    "apikey",
    "access-token",
    "access_token",
    "refresh-token",
    "refresh_token",
    "client-secret",
    "client_secret",
    "password",
    "auth-headers",
    "auth_headers",
}


class BulkTransportError(RuntimeError):
    """A bulk transfer failed without exposing response contents or credentials."""

    status_code = None
    status = None
    headers: dict[str, str] = {}
    uncertain = False


class InvalidBulkRequestError(BulkTransportError):
    """A request or redirect escaped the approved official HTTPS boundary."""


class BulkHTTPStatusError(BulkTransportError):
    """An HTTP response did not contain a completed bulk object."""

    def __init__(
        self,
        status_code: int,
        *,
        retry_after: str | None = None,
        async_location: str | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self.status = status_code
        self.retry_after = retry_after
        self.async_location = async_location
        self.headers = dict(headers or {})
        detail = f"bulk source returned HTTP {status_code}"
        if status_code == 202:
            detail += "; asynchronous preparation is still pending"
        elif status_code == 413:
            detail += "; the provider rejected the requested object size"
        elif status_code == 429:
            detail += "; retry only after the provider's rate-limit delay"
        super().__init__(detail)

    @property
    def retry_info(self) -> dict[str, Any]:
        return {
            "status_code": self.status_code,
            "retry_after": self.retry_after,
            "async_location": self.async_location,
        }


class BulkResponseTooLargeError(BulkTransportError):
    """One response exceeded its explicit object-size bound."""


class LocalDiskCapacityError(BulkTransportError):
    """The runner cannot stage the requested object on its disposable disk."""

    def __init__(self, required_bytes: int, available_bytes: int) -> None:
        self.required_bytes = required_bytes
        self.available_bytes = available_bytes
        super().__init__(
            "runner disk headroom is below the bulk object's required bytes "
            f"(required={required_bytes}, available={available_bytes})"
        )


class DriveCapacityError(BulkTransportError):
    """Drive cannot prove enough current account storage for this object."""

    def __init__(
        self, required_bytes: int, available_bytes: int | None
    ) -> None:
        self.required_bytes = required_bytes
        self.available_bytes = available_bytes
        if available_bytes is None:
            detail = "Drive did not report a usable storage limit and usage"
        else:
            detail = (
                "Drive headroom is below the bulk object's actual bytes "
                f"(required={required_bytes}, available={available_bytes})"
            )
        super().__init__(detail)


class DriveIntegrityError(BulkTransportError):
    """A Drive object did not match its immutable descriptor."""


class UncertainDriveWriteError(BulkTransportError):
    """A preallocated Drive object could not be proven after an upload error."""

    uncertain = True


class _StrictRedirectHandler(HTTPRedirectHandler):
    def __init__(
        self,
        allowed_hosts: frozenset[str],
        origin_host: str,
        authenticated: bool,
    ) -> None:
        super().__init__()
        self._allowed_hosts = allowed_hosts
        self._origin_host = origin_host
        self._authenticated = authenticated

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        _validate_url(newurl, self._allowed_hosts, "redirect")
        target_host = urlsplit(newurl).hostname
        if self._authenticated and target_host != self._origin_host:
            raise InvalidBulkRequestError(
                "authenticated bulk requests cannot redirect to another host"
            )
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class _HashingWriter:
    """File-like Drive download sink that retains hashes and byte count only."""

    def __init__(self) -> None:
        self.sha256 = hashlib.sha256()
        self.md5 = hashlib.md5(usedforsecurity=False)
        self.size_bytes = 0
        self.output: Any | None = None

    def write(self, chunk: bytes) -> int:
        self.sha256.update(chunk)
        self.md5.update(chunk)
        self.size_bytes += len(chunk)
        if self.output is not None:
            self.output.write(chunk)
        return len(chunk)

    def tell(self) -> int:
        return self.size_bytes


def fetch_to_file(
    request_descriptor: Mapping[str, Any],
    path: str | os.PathLike[str],
    allowed_hosts: set[str] | frozenset[str] | tuple[str, ...] | list[str],
    timeout: int | float,
    max_bytes: int,
    previous_descriptor: Mapping[str, Any] | None = None,
    *,
    auth_headers: Mapping[str, str] | None = None,
    opener: Any | None = None,
    chunk_size: int = DEFAULT_DOWNLOAD_CHUNK_BYTES,
    accepted_statuses: tuple[int, ...] | set[int] | frozenset[int] = (200,),
) -> dict[str, Any]:
    """Stream one allowlisted HTTPS response to ``path`` and return exact hashes.

    A verified complete prior file may be revalidated with a strong ETag or a
    valid Last-Modified date. Partial downloads are never resumed: without a
    provider-specific immutable version guard, restarting the whole response is
    the only safe recovery from a lost connection.
    """
    hosts = _allowed_hosts(allowed_hosts)
    url = _request_url(request_descriptor, hosts)
    timeout = _positive_number(timeout, "timeout")
    max_bytes = _positive_int(max_bytes, "max_bytes")
    chunk_size = _positive_int(chunk_size, "chunk_size")
    if (
        not isinstance(accepted_statuses, (tuple, set, frozenset))
        or not accepted_statuses
        or any(type(status) is not int or not 200 <= status <= 599
               or status in {304, 429, 503}
               for status in accepted_statuses)
    ):
        raise ValueError("accepted_statuses must contain HTTP statuses other than 304")
    accepted = frozenset(accepted_statuses)
    destination = Path(path)
    if destination.exists() and (destination.is_symlink() or not destination.is_file()):
        raise InvalidBulkRequestError("bulk destination must be a regular file")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.parent.is_symlink() or not destination.parent.is_dir():
        raise InvalidBulkRequestError("bulk destination parent must be a real directory")

    conditional_headers: dict[str, str] = {}
    prior = _verified_previous(
        destination, previous_descriptor, url, chunk_size, max_bytes
    )
    if prior is not None:
        response_headers = prior.get("response_headers")
        assert isinstance(response_headers, Mapping)
        etag = response_headers.get("etag")
        modified = response_headers.get("last_modified")
        if isinstance(etag, str) and _STRONG_ETAG_RE.fullmatch(etag):
            conditional_headers["If-None-Match"] = etag
        elif isinstance(modified, str) and _valid_http_date(modified):
            conditional_headers["If-Modified-Since"] = modified

    transport_headers = {
        "Accept": "application/octet-stream, application/zip, text/tab-separated-values;q=0.9, */*;q=0.5",
        "User-Agent": "zohelo-data/1.0 (+https://github.com/rutkala/zohelo-data)",
        **conditional_headers,
    }
    if auth_headers is not None:
        if not isinstance(auth_headers, Mapping) or any(
            not isinstance(key, str) or not isinstance(value, str)
            or "\r" in key or "\n" in key or "\r" in value or "\n" in value
            for key, value in auth_headers.items()
        ):
            raise InvalidBulkRequestError("transport auth headers must be string pairs")
        if any(
            key.lower() in {
                "host", "range", "if-range", "if-match", "if-none-match",
                "if-modified-since", "if-unmodified-since", "content-length",
                "transfer-encoding", "connection",
            }
            for key in auth_headers
        ):
            raise InvalidBulkRequestError(
                "transport auth headers cannot override request integrity controls"
            )
        transport_headers.update(auth_headers)

    if opener is not None and auth_headers:
        raise InvalidBulkRequestError(
            "a custom opener cannot safely carry transport auth headers"
        )
    origin_host = urlsplit(url).hostname
    assert origin_host is not None
    transport = opener or build_opener(
        _StrictRedirectHandler(hosts, origin_host, bool(auth_headers))
    )
    try:
        response = transport.open(Request(url, headers=transport_headers), timeout=timeout)
    except HTTPError as error:
        response = error
    except (OSError, TimeoutError) as exc:
        raise BulkTransportError("bulk source request failed") from exc

    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.part")
    try:
        with response:
            final_url = response.geturl() if hasattr(response, "geturl") else url
            _validate_url(final_url, hosts, "response")
            _reject_sensitive_query(final_url)
            status = int(getattr(response, "status", response.getcode()))
            selected_headers = _selected_response_headers(response.headers)
            location = selected_headers.pop("location", None)
            if status == 202 and location is not None:
                async_location = urljoin(final_url, location)
                _validate_url(async_location, hosts, "asynchronous status")
                _reject_sensitive_query(async_location)
                selected_headers["location"] = async_location
            if status == 304:
                if prior is None or not conditional_headers:
                    raise BulkHTTPStatusError(status)
                result = dict(prior)
                result.update({
                    "status_code": 304,
                    "not_modified": True,
                    "request_url": url,
                    "final_url": final_url,
                })
                if selected_headers:
                    revalidation_headers = {
                        key: value for key, value in selected_headers.items()
                        if key in {"etag", "last_modified"}
                    }
                    result["response_headers"] = {
                        **dict(prior["response_headers"]),
                        **revalidation_headers,
                    }
                return result
            if status not in accepted:
                async_location = selected_headers.get("location") if status == 202 else None
                raise BulkHTTPStatusError(
                    status,
                    retry_after=selected_headers.get("retry_after"),
                    async_location=async_location,
                    headers={
                        key.replace("_", "-"): value
                        for key, value in selected_headers.items()
                    },
                )

            declared = _content_length(selected_headers.get("content_length"), max_bytes)
            _require_disk_headroom(destination.parent, declared or max_bytes)
            sha256 = hashlib.sha256()
            md5 = hashlib.md5(usedforsecurity=False)
            size = 0
            try:
                with temporary.open("xb") as handle:
                    while True:
                        chunk = response.read(chunk_size)
                        if not chunk:
                            break
                        if not isinstance(chunk, bytes):
                            raise BulkTransportError("bulk response stream returned non-byte data")
                        size += len(chunk)
                        if size > max_bytes:
                            raise BulkResponseTooLargeError(
                                f"bulk response exceeds max_bytes={max_bytes}"
                            )
                        sha256.update(chunk)
                        md5.update(chunk)
                        handle.write(chunk)
                    handle.flush()
                    os.fsync(handle.fileno())
            except OSError as exc:
                raise BulkTransportError("unable to stage the bulk response on runner disk") from exc
            if declared is not None and size != declared:
                raise BulkTransportError(
                    "bulk response ended before its declared Content-Length"
                )
            if size == 0:
                raise BulkTransportError("bulk response body is empty")
            os.replace(temporary, destination)
            return {
                "request_url": url,
                "final_url": final_url,
                "status_code": status,
                "not_modified": False,
                "sha256": sha256.hexdigest(),
                "md5": md5.hexdigest(),
                "size_bytes": size,
                "response_headers": selected_headers,
            }
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


class BulkDriveRawStore:
    """Immutable, source-scoped Drive store for large raw files."""

    def __init__(
        self,
        storage: Any,
        source_id: str,
        responses_root_id: str | None = None,
        *,
        chunk_size: int = DEFAULT_DRIVE_CHUNK_BYTES,
    ) -> None:
        if not isinstance(source_id, str) or _SOURCE_ID_RE.fullmatch(source_id) is None:
            raise ValueError("source_id must be a lowercase source identifier")
        self.storage = storage
        self.source_id = source_id
        self.chunk_size = _positive_int(chunk_size, "chunk_size")
        if self.chunk_size % (256 * 1024):
            raise ValueError("Drive chunk_size must be a multiple of 256 KiB")
        self.files = storage.drive_service.files()
        self._write_session = None
        self.selected_root_id = storage.resolve_root(create=False)
        self._require_owned_folder(self.selected_root_id, "selected Drive root")
        if responses_root_id is None:
            storage.authorize_writes()
            landing_root = storage.resolve_zone("landing", create=True)
            storage._assert_parent_within_selected_root(landing_root)
            self._require_owned_folder(landing_root, "Landing root")
            responses_root_id = storage.get_or_create_nested_folder(
                [source_id, "responses"], root_id=landing_root
            )
        self.responses_root_id = _drive_id(responses_root_id, "responses root ID")
        self._validate_namespace()

    def put_file(
        self,
        path: str | os.PathLike[str],
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Upload, or reuse, one hash-named file and stream-verify Drive bytes."""
        local_path = Path(path)
        if local_path.is_symlink() or not local_path.is_file():
            raise ValueError("bulk raw path must be a regular file")
        size = local_path.stat().st_size
        if size <= 0:
            raise ValueError("bulk raw file must be nonempty")
        local = _hash_file(local_path, self.chunk_size)
        if local["size_bytes"] != size:
            raise DriveIntegrityError("bulk raw file changed while it was hashed")
        durable_metadata = _safe_metadata(metadata)
        name = f"raw-{local['sha256']}.bin"
        self._require_owned_folder(self.selected_root_id, "selected Drive root")
        self._require_owned_folder(self.responses_root_id, "source responses root")
        found = self._find(name)
        if len(found) > 1:
            raise DriveIntegrityError("content-addressed bulk raw file is ambiguous")
        if found:
            return self._verified_descriptor(
                found[0], name, local, durable_metadata
            )

        self._require_drive_headroom(size)
        self._authorize_write_parent()
        self._require_owned_folder(self.selected_root_id, "selected Drive root")
        self._require_owned_folder(self.responses_root_id, "source responses root")
        file_id = self._allocate_id()
        from googleapiclient.http import MediaFileUpload

        media = MediaFileUpload(
            str(local_path),
            mimetype="application/octet-stream",
            chunksize=self.chunk_size,
            resumable=True,
        )
        upload_error: Exception | None = None
        try:
            created = self.files.create(
                body={
                    "id": file_id,
                    "name": name,
                    "parents": [self.responses_root_id],
                    "mimeType": "application/octet-stream",
                },
                media_body=media,
                fields="id",
            ).execute(num_retries=2)
            if created.get("id") != file_id:
                raise DriveIntegrityError("Drive returned an unexpected bulk raw file ID")
        except Exception as exc:
            upload_error = exc

        try:
            descriptor = self._verified_descriptor(
                file_id, name, local, durable_metadata
            )
        except Exception as verify_error:
            if upload_error is not None:
                raise UncertainDriveWriteError(
                    "bulk upload failed and the preallocated Drive ID could not be proven"
                ) from verify_error
            raise
        if upload_error is not None:
            # The response was lost after Drive committed the exact reserved ID.
            return descriptor
        return descriptor

    def read_to_file(
        self,
        descriptor: Mapping[str, Any],
        path: str | os.PathLike[str],
    ) -> dict[str, Any]:
        """Stream a verified Drive object to disposable local working storage."""
        expected = _drive_descriptor(descriptor)
        destination = Path(path)
        if destination.exists() and (destination.is_symlink() or not destination.is_file()):
            raise DriveIntegrityError("bulk restore destination must be a regular file")
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.parent.is_symlink() or not destination.parent.is_dir():
            raise DriveIntegrityError("bulk restore parent must be a real directory")
        self._validate_namespace()
        if self._find(expected["name"]) != [expected["id"]]:
            raise DriveIntegrityError(
                "bulk raw file is missing, ambiguous, or outside its source namespace"
            )
        self._validated_remote_metadata(expected["id"], expected["name"], expected)
        _require_disk_headroom(destination.parent, expected["size_bytes"])
        temporary = destination.with_name(f".{destination.name}.{os.getpid()}.part")
        observed = _HashingWriter()
        try:
            with temporary.open("xb") as handle:
                observed.output = handle
                self._stream_remote(expected["id"], observed, expected["size_bytes"])
                handle.flush()
                os.fsync(handle.fileno())
            if (
                observed.size_bytes != expected["size_bytes"]
                or observed.sha256.hexdigest() != expected["sha256"]
                or observed.md5.hexdigest() != expected["md5"]
            ):
                raise DriveIntegrityError("restored bulk raw bytes do not match its descriptor")
            os.replace(temporary, destination)
            return dict(descriptor)
        except OSError as exc:
            raise BulkTransportError("unable to stage the restored bulk raw file") from exc
        finally:
            observed.output = None
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass

    def verify(self, descriptor: Mapping[str, Any]) -> dict[str, Any]:
        """Stream-verify an existing descriptor without materializing its bytes."""
        expected = _drive_descriptor(descriptor)
        file_id = expected["id"]
        name = expected["name"]
        self._validate_namespace()
        found = self._find(name)
        if found != [file_id]:
            raise DriveIntegrityError(
                "bulk raw file is missing, ambiguous, or outside its source namespace"
            )
        return self._verified_descriptor(
            file_id,
            name,
            expected,
            _safe_metadata(descriptor.get("metadata")),
        )

    def _validate_namespace(self) -> None:
        self.storage._assert_parent_within_selected_root(self.responses_root_id)
        self._require_owned_folder(self.selected_root_id, "selected Drive root")
        self._require_owned_folder(self.responses_root_id, "source responses root")

    def _authorize_write_parent(self) -> None:
        begin = getattr(self.storage, "begin_write_session", None)
        authorize = getattr(self.storage, "authorize_session_parent", None)
        if callable(begin) and callable(authorize):
            if self._write_session is None:
                self._write_session = begin()
                authorize(self._write_session, self.selected_root_id)
            authorize(self._write_session, self.responses_root_id)
            return
        self.storage.authorize_writes()
        self.storage._assert_parent_within_selected_root(self.responses_root_id)

    def _require_owned_folder(self, folder_id: str, label: str) -> None:
        folder_id = _drive_id(folder_id, label)
        try:
            item = self.files.get(
                fileId=folder_id,
                fields="id,mimeType,ownedByMe,trashed",
            ).execute(num_retries=2)
        except Exception as exc:
            raise DriveIntegrityError(f"{label} could not be verified") from exc
        if not (
            item.get("id") == folder_id
            and item.get("mimeType") == "application/vnd.google-apps.folder"
            and item.get("ownedByMe") is True
            and item.get("trashed") is False
        ):
            raise DriveIntegrityError(f"{label} must be an owned, untrashed folder")

    def _find(self, name: str) -> list[str]:
        quoted_name = _quote(name)
        quoted_parent = _quote(self.responses_root_id)
        result: list[str] = []
        token = None
        while True:
            response = self.files.list(
                q=(
                    f"name='{quoted_name}' and '{quoted_parent}' in parents "
                    "and trashed=false"
                ),
                fields="nextPageToken,files(id)",
                spaces="drive",
                pageSize=100,
                pageToken=token,
            ).execute(num_retries=2)
            for item in response.get("files", []):
                result.append(_drive_id(item.get("id"), "bulk raw file ID"))
            token = response.get("nextPageToken")
            if not token:
                return result

    def _allocate_id(self) -> str:
        allocated = self.files.generateIds(
            count=1, space="drive", type="files"
        ).execute(num_retries=2)
        ids = allocated.get("ids", [])
        if not isinstance(ids, list) or len(ids) != 1:
            raise UncertainDriveWriteError("Drive did not allocate one bulk raw file ID")
        return _drive_id(ids[0], "preallocated bulk raw file ID")

    def _require_drive_headroom(self, required: int) -> None:
        try:
            quota = self.storage.drive_service.about().get(
                fields="storageQuota"
            ).execute(num_retries=2).get("storageQuota", {})
            limit = _quota_bytes(quota.get("limit"))
            usage = _quota_bytes(quota.get("usage"))
        except Exception as exc:
            raise DriveCapacityError(required, None) from exc
        if limit is None or usage is None:
            raise DriveCapacityError(required, None)
        available = max(0, limit - usage)
        if available < required:
            raise DriveCapacityError(required, available)

    def _verified_descriptor(
        self,
        file_id: str,
        name: str,
        expected: Mapping[str, Any],
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        self._validated_remote_metadata(file_id, name, expected)

        observed = _HashingWriter()
        self._stream_remote(file_id, observed, expected["size_bytes"])
        if (
            observed.size_bytes != expected["size_bytes"]
            or observed.sha256.hexdigest() != expected["sha256"]
            or observed.md5.hexdigest() != expected["md5"]
        ):
            raise DriveIntegrityError("bulk raw Drive bytes do not match local hashes")
        return {
            "id": file_id,
            "sha256": expected["sha256"],
            "md5": expected["md5"],
            "size_bytes": expected["size_bytes"],
            "name": name,
            "metadata": metadata,
        }

    def _validated_remote_metadata(
        self, file_id: str, name: str, expected: Mapping[str, Any]
    ) -> dict[str, Any]:
        try:
            item = self.files.get(
                fileId=file_id,
                fields="id,name,size,md5Checksum,mimeType,parents,ownedByMe,trashed",
            ).execute(num_retries=2)
        except Exception as exc:
            raise DriveIntegrityError("bulk raw Drive metadata could not be read") from exc
        if not (
            item.get("id") == file_id
            and item.get("name") == name
            and item.get("mimeType") == "application/octet-stream"
            and item.get("parents") == [self.responses_root_id]
            and item.get("ownedByMe") is True
            and item.get("trashed") is False
        ):
            raise DriveIntegrityError("bulk raw Drive metadata escaped its immutable namespace")
        try:
            remote_size = int(item.get("size", -1))
        except (TypeError, ValueError):
            remote_size = -1
        if remote_size != expected["size_bytes"]:
            raise DriveIntegrityError("bulk raw Drive size does not match local bytes")
        drive_md5 = item.get("md5Checksum")
        if drive_md5 is not None and drive_md5 != expected["md5"]:
            raise DriveIntegrityError("bulk raw Drive MD5 metadata does not match local bytes")
        return item

    def _stream_remote(
        self, file_id: str, destination: _HashingWriter, maximum_bytes: int
    ) -> None:
        try:
            from googleapiclient.http import MediaIoBaseDownload

            download = MediaIoBaseDownload(
                destination,
                self.files.get_media(fileId=file_id),
                chunksize=self.chunk_size,
            )
            done = False
            while not done:
                _, done = download.next_chunk(num_retries=2)
                if destination.size_bytes > maximum_bytes:
                    raise DriveIntegrityError("bulk raw Drive download exceeded its descriptor")
        except DriveIntegrityError:
            raise
        except Exception as exc:
            raise DriveIntegrityError("bulk raw Drive bytes could not be streamed for verification") from exc


def _allowed_hosts(values: Any) -> frozenset[str]:
    if not isinstance(values, (set, frozenset, tuple, list)) or not values:
        raise InvalidBulkRequestError("allowed_hosts must be a nonempty collection")
    hosts = frozenset(str(value).lower().rstrip(".") for value in values)
    if any(
        not host or urlsplit(f"https://{host}").hostname != host
        or "/" in host or "@" in host or ":" in host
        for host in hosts
    ):
        raise InvalidBulkRequestError("allowed_hosts contains an invalid exact host")
    return hosts


def _request_url(descriptor: Mapping[str, Any], hosts: frozenset[str]) -> str:
    if not isinstance(descriptor, Mapping) or set(descriptor) - {"url", "params"}:
        raise InvalidBulkRequestError("request descriptor may contain only url and params")
    url = descriptor.get("url")
    if not isinstance(url, str):
        raise InvalidBulkRequestError("request descriptor url must be a string")
    _validate_url(url, hosts, "request")
    params = descriptor.get("params", {})
    if not isinstance(params, Mapping):
        raise InvalidBulkRequestError("request params must be a mapping")
    _reject_sensitive_keys(params, "request params")
    encoded = urlencode(params, doseq=True)
    if encoded:
        parsed = urlsplit(url)
        query = "&".join(part for part in (parsed.query, encoded) if part)
        url = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, ""))
    _reject_sensitive_query(url)
    return url


def _validate_url(url: Any, hosts: frozenset[str], label: str) -> None:
    if not isinstance(url, str):
        raise InvalidBulkRequestError(f"bulk {label} URL must be a string")
    parsed = urlsplit(url)
    hostname = parsed.hostname.lower().rstrip(".") if parsed.hostname else None
    try:
        port = parsed.port
    except ValueError:
        port = -1
    if (
        parsed.scheme.lower() != "https"
        or hostname not in hosts
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or port not in (None, 443)
    ):
        raise InvalidBulkRequestError(
            f"bulk {label} URL is outside the approved official HTTPS hosts"
        )


def _reject_sensitive_query(url: str) -> None:
    for key, _ in parse_qsl(urlsplit(url).query, keep_blank_values=True):
        if key.lower() in _SENSITIVE_KEYS:
            raise InvalidBulkRequestError(
                "credentials must be supplied only through transport auth_headers"
            )


def _reject_sensitive_keys(value: Any, label: str) -> None:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if not isinstance(key, str):
                raise InvalidBulkRequestError(f"{label} keys must be strings")
            if key.lower() in _SENSITIVE_KEYS:
                raise InvalidBulkRequestError(f"{label} contains credential material")
            _reject_sensitive_keys(nested, label)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _reject_sensitive_keys(item, label)


def _selected_response_headers(headers: Any) -> dict[str, str]:
    selected: dict[str, str] = {}
    names = {
        "etag": "etag",
        "last-modified": "last_modified",
        "content-type": "content_type",
        "content-length": "content_length",
        "retry-after": "retry_after",
        "location": "location",
    }
    for source, target in names.items():
        value = headers.get(source) if hasattr(headers, "get") else None
        if value is None and hasattr(headers, "get"):
            value = headers.get(source.title())
        if value is not None:
            value = str(value)
            if len(value) > 8192 or "\r" in value or "\n" in value:
                raise BulkTransportError("bulk response contains an unsafe oversized header")
            selected[target] = value
    return selected


def _content_length(value: str | None, maximum: int) -> int | None:
    if value is None:
        return None
    if not value.isdecimal():
        raise BulkTransportError("bulk response has an invalid Content-Length")
    length = int(value)
    if length > maximum:
        raise BulkResponseTooLargeError(
            f"bulk response Content-Length exceeds max_bytes={maximum}"
        )
    return length


def _require_disk_headroom(directory: Path, required: int) -> None:
    try:
        available = shutil.disk_usage(directory).free
    except OSError as exc:
        raise LocalDiskCapacityError(required, 0) from exc
    if available < required:
        raise LocalDiskCapacityError(required, available)


def _verified_previous(
    path: Path,
    descriptor: Mapping[str, Any] | None,
    request_url: str,
    chunk_size: int,
    maximum_bytes: int,
) -> dict[str, Any] | None:
    if descriptor is None or not path.is_file() or path.is_symlink():
        return None
    if not isinstance(descriptor, Mapping):
        return None
    if descriptor.get("request_url") != request_url:
        return None
    sha = descriptor.get("sha256")
    md5 = descriptor.get("md5")
    size = descriptor.get("size_bytes")
    headers = descriptor.get("response_headers")
    if (
        not isinstance(sha, str) or _SHA256_RE.fullmatch(sha) is None
        or not isinstance(md5, str) or _MD5_RE.fullmatch(md5) is None
        or type(size) is not int or not 0 < size <= maximum_bytes
        or not isinstance(headers, Mapping)
    ):
        return None
    observed = _hash_file(path, chunk_size)
    if observed != {"sha256": sha, "md5": md5, "size_bytes": size}:
        return None
    return dict(descriptor)


def _valid_http_date(value: str) -> bool:
    if len(value) > 256 or "\r" in value or "\n" in value:
        return False
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        return False
    return parsed.tzinfo is not None


def _hash_file(path: Path, chunk_size: int) -> dict[str, Any]:
    sha256 = hashlib.sha256()
    md5 = hashlib.md5(usedforsecurity=False)
    size = 0
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            size += len(chunk)
            sha256.update(chunk)
            md5.update(chunk)
    return {"sha256": sha256.hexdigest(), "md5": md5.hexdigest(), "size_bytes": size}


def _safe_metadata(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError("bulk raw metadata must be a mapping")
    _reject_sensitive_keys(value, "bulk raw metadata")
    try:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
        if len(encoded.encode("utf-8")) > MAX_METADATA_BYTES:
            raise ValueError("bulk raw metadata exceeds its 1 MiB safety limit")
        decoded = json.loads(encoded)
    except (TypeError, ValueError) as exc:
        raise ValueError("bulk raw metadata must be finite JSON data") from exc
    if not isinstance(decoded, dict):
        raise ValueError("bulk raw metadata must be a JSON object")
    return decoded


def _drive_descriptor(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise DriveIntegrityError("bulk Drive descriptor must be a mapping")
    file_id = _drive_id(value.get("id"), "bulk raw file ID")
    name = value.get("name")
    sha256 = value.get("sha256")
    md5 = value.get("md5")
    size = value.get("size_bytes")
    if (
        not isinstance(name, str)
        or not isinstance(sha256, str) or _SHA256_RE.fullmatch(sha256) is None
        or not isinstance(md5, str) or _MD5_RE.fullmatch(md5) is None
        or type(size) is not int or size <= 0
        or name != f"raw-{sha256}.bin"
    ):
        raise DriveIntegrityError("bulk Drive descriptor is invalid")
    return {
        "id": file_id,
        "name": name,
        "sha256": sha256,
        "md5": md5,
        "size_bytes": size,
    }


def _quota_bytes(value: Any) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _positive_int(value: Any, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{label} must be a positive integer")
    return value


def _positive_number(value: Any, label: str) -> int | float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or value <= 0
        or not math.isfinite(value)
    ):
        raise ValueError(f"{label} must be positive")
    return value


def _drive_id(value: Any, label: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[A-Za-z0-9_-]+", value) is None:
        raise DriveIntegrityError(f"{label} is invalid")
    return value


def _quote(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")
