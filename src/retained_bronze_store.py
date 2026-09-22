"""Publication-only DBW transport: reference retained Bronze; never create Landing.

Original references are pinned to the reviewed descriptor inventory. They remain
user-owned Drive files, not transactionally immutable objects: changed bytes fail
checksum verification. New oversized-file derivatives live under Bronze/query_parts;
legacy publication fragments and all historical manifests remain readable and intact.
"""
from __future__ import annotations

from hashlib import sha256
from pathlib import Path
import re
from typing import Any

from ingestion.source_campaign_store import (
    CampaignStoreError, DriveCampaignStore, MAX_LANDING_FILE_BYTES,
)
from retained_dbw_publication import SOURCE_ID, validate_audit

FIELDS = ("id", "name", "size", "sha256")


class RetainedBronzeDriveStore(DriveCampaignStore):
    direct_references = True

    def __init__(self, storage: Any, audit_dir: Path, guard) -> None:
        if not callable(guard):
            raise CampaignStoreError("An operational publication guard is required")
        _, inventory, _ = validate_audit(audit_dir, require_reviewed_snapshot=True)
        guard()  # Must precede even publication namespace initialization.
        self.storage = storage
        self.inventory = inventory
        self._references = {}
        self._reference_identities = {}
        for item in inventory["objects"]:
            if not item["path"].endswith(".parquet") or item["size"] > MAX_LANDING_FILE_BYTES:
                continue
            reference = {key: item[key] for key in FIELDS}
            self._references[item["id"]] = reference
            match = re.fullmatch(r"observations/part_(\d+)\.parquet", item["path"])
            identity = (f"observations-{int(match[1])}-1" if match
                        else f"{item['path'].split('/')[0]}-1")
            self._reference_identities[identity] = reference
        self._derived_root_id = None
        super().__init__(storage, SOURCE_ID, publication_only=True)
        self.retained_publication_guard = guard

    def assert_reference(self, value: dict[str, Any]) -> bool:
        """Accept an external descriptor only when all identity fields are pinned."""
        expected = self._references.get(value.get("id"))
        is_original_name = bool(re.fullmatch(
            r"(?:part_[1-9][0-9]*|br_dbw_(?:dictionaries|metadata|indicators))\.parquet",
            str(value.get("name", "")),
        ))
        if expected is None:
            if is_original_name:
                raise CampaignStoreError("Unreviewed retained Bronze reference")
            return False
        if any(value.get(key) != expected[key] for key in FIELDS):
            raise CampaignStoreError("Retained Bronze reference differs from the reviewed inventory")
        return True

    def _derived_root(self, *, create: bool = True) -> str | None:
        if self._derived_root_id is None:
            bronze = self.storage.resolve_zone("bronze", create=False)
            path = ["gus_dbw", "query_parts", self.inventory["inventory_sha256"]]
            if create:
                self._derived_root_id = self.storage.get_or_create_nested_folder(path, root_id=bronze)
            else:
                parent = bronze
                for name in path:
                    found = self._find(name, parent)
                    if len(found) > 1:
                        raise CampaignStoreError("Bronze query-parts namespace is ambiguous")
                    if not found:
                        return None
                    parent = found[0]
                self._derived_root_id = parent
        return self._derived_root_id

    def _fragment_parent(self, name: str, *, for_write: bool = False) -> tuple[str, list[str]]:
        # Reading/reusing old fragments must not initialize another data folder.
        old = self._landing_root()
        old_ids = self._find(name, old)
        current = self._derived_root(create=False)
        new_ids = self._find(name, current) if current is not None else []
        if len(old_ids) + len(new_ids) > 1:
            raise CampaignStoreError("Retained query fragment is ambiguous")
        if old_ids:
            return old, old_ids
        if current is None:
            current = self._derived_root() if for_write else old
        return current, new_ids

    def put_or_reuse_landing_object(self, identity: str, extension: str, data: bytes, *,
                                    maximum_bytes: int = MAX_LANDING_FILE_BYTES) -> dict[str, Any]:
        if extension != "parquet":
            return super().put_or_reuse_landing_object(identity, extension, data,
                                                       maximum_bytes=maximum_bytes)
        if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", identity):
            raise CampaignStoreError("Invalid publication identity")
        if not isinstance(data, bytes) or not 0 < len(data) <= min(maximum_bytes, MAX_LANDING_FILE_BYTES):
            raise CampaignStoreError("Query part exceeds the existing bounded-file contract")
        digest = sha256(data).hexdigest()
        reference = self._reference_identities.get(identity)
        if reference is not None:
            if reference["size"] != len(data) or reference["sha256"] != digest:
                raise CampaignStoreError("Retained reference bytes differ from the reviewed input")
            return dict(reference)  # No write and no duplicate upload.
        self.guard_publication_owner()
        name = f"fragment-{identity}-{digest}.parquet"
        parent, found = self._fragment_parent(name, for_write=True)
        if found:
            result = {"id": found[0], "name": name, "size": len(data), "sha256": digest}
            if self.read_landing_object(result, maximum_bytes=maximum_bytes) != data:
                raise CampaignStoreError("Existing query fragment bytes changed")
            return result
        identity = self._create_verified(name, data, parent, "Bronze query part")
        return {"id": identity, "name": name, "size": len(data), "sha256": digest}

    def read_landing_object(self, descriptor: dict[str, Any], *,
                            maximum_bytes: int = MAX_LANDING_FILE_BYTES) -> bytes:
        if self.assert_reference(descriptor):
            if descriptor["size"] > maximum_bytes:
                raise CampaignStoreError("Retained reference exceeds the reader bound")
            raw = self._read(descriptor["id"], "Retained Bronze reference")
        elif str(descriptor.get("name", "")).endswith(".parquet"):
            from retained_dbw_publication import FRAGMENT_NAME_RE
            if not FRAGMENT_NAME_RE.fullmatch(str(descriptor.get("name", ""))):
                raise CampaignStoreError("Invalid query fragment name")
            _, found = self._fragment_parent(descriptor["name"])
            if found != [descriptor.get("id")]:
                raise CampaignStoreError("Query fragment is outside the publication namespaces")
            if type(descriptor.get("size")) is not int or not 0 < descriptor["size"] <= min(maximum_bytes, MAX_LANDING_FILE_BYTES):
                raise CampaignStoreError("Invalid query fragment size")
            raw = self._read(descriptor["id"], "Bronze query part")
        else:
            return super().read_landing_object(descriptor, maximum_bytes=maximum_bytes)
        if len(raw) != descriptor["size"] or sha256(raw).hexdigest() != descriptor["sha256"]:
            raise CampaignStoreError("Published Bronze bytes do not match their pinned descriptor")
        return raw

    def verify_original_inventory(self) -> None:
        # Metadata re-inventory detects changed IDs, paths, sizes or recorded hashes.
        # Full-byte verification is performed by the cold input restore and readers.
        from audit_retained_dbw_bronze import discover, inventory_document, validate_inventory
        descriptors, _ = validate_inventory(discover(self.storage))
        if inventory_document(descriptors)["inventory_sha256"] != self.inventory["inventory_sha256"]:
            raise CampaignStoreError("Original Bronze inventory changed during publication")

    def verify_landing_objects_metadata(self, descriptors: list[dict[str, Any]]) -> None:
        self.verify_original_inventory()
        expected = {}
        seen = set()
        for item in descriptors:
            if item["id"] in seen:
                raise CampaignStoreError("Publication reuses a file identity")
            seen.add(item["id"])
            if not self.assert_reference(item):
                expected[item["id"]] = item
        if not expected:
            return
        observed = self._store.list_metadata(self._landing_root())
        derived = self._derived_root(create=False)
        if derived is not None:
            observed += self._store.list_metadata(derived)
        names = {item["name"] for item in expected.values()}
        relevant = [item for item in observed if item.get("id") in expected or item.get("name") in names]
        if len(relevant) != len(expected) or len({i.get("id") for i in relevant}) != len(expected):
            raise CampaignStoreError("Query fragment metadata is missing or duplicated")
        for item in relevant:
            ref = expected.get(item["id"])
            if (ref is None or any(item.get(key) != ref[key] for key in FIELDS)
                    or item.get("trashed") is not False or item.get("kind") != "application/octet-stream"):
                raise CampaignStoreError("Query fragment metadata changed")

    def promote_landing_pointer(self, pointer: dict[str, Any]) -> None:
        self.retained_publication_guard()
        self.verify_original_inventory()
        super().promote_landing_pointer(pointer)
