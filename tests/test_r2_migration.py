"""Offline regression coverage for all-layer, non-destructive migration."""
import base64
import copy
import hashlib
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from r2_migration import (ROOT_ID, CONTROL, FOLDER, SHORTCUT, MigrationError,
                          TimeBoundReached, snapshot, copy_object, migrate, component,
                          source_hashes)


def blob(identity, parent, name, body=b"abcd"):
    return {"id": identity, "name": name, "mimeType": "application/octet-stream",
            "parents": [parent], "size": str(len(body)), "version": "1",
            "md5Checksum": hashlib.md5(body).hexdigest(),
            "sha256Checksum": hashlib.sha256(body).hexdigest()}


def folder(identity, parent, name):
    return {"id": identity, "name": name, "mimeType": FOLDER, "parents": [parent]}


class Source:
    def __init__(self, nodes, bodies):
        self.nodes = nodes
        self.bodies = bodies
        self.downloads = []
    def metadata(self, identity):
        return copy.deepcopy(self.nodes[identity])
    def children(self, parents):
        return [copy.deepcopy(n) for n in self.nodes.values() if set(n.get("parents", [])) & set(parents)]
    def chunks(self, node):
        self.downloads.append(node["id"])
        yield self.bodies[node["id"]]


class Missing(Exception):
    response = {"Error": {"Code": "NoSuchKey"}}


class S3:
    def __init__(self):
        self.objects, self.uploads = {}, {}
        self.aborted = 0
    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise Missing()
        obj = self.objects[Bucket, Key]
        return {"ContentLength": len(obj["body"]), "ETag": obj["etag"], "Metadata": obj["meta"]}
    def put_object(self, Bucket, Key, Body, Metadata=None, ContentMD5=None, **kwargs):
        if kwargs.get("IfNoneMatch") == "*" and (Bucket, Key) in self.objects:
            raise RuntimeError("precondition")
        assert base64.b64encode(hashlib.md5(Body).digest()).decode() == ContentMD5
        etag = '"' + hashlib.md5(Body).hexdigest() + '"'
        self.objects[Bucket, Key] = {"body": Body, "meta": Metadata or {}, "etag": etag}
        return {"ETag": etag}
    def create_multipart_upload(self, Bucket, Key, Metadata, **kwargs):
        identity = str(len(self.uploads) + 1)
        self.uploads[identity] = {"key": (Bucket, Key), "meta": Metadata, "parts": {}}
        return {"UploadId": identity}
    def upload_part(self, UploadId, PartNumber, Body, ContentMD5, **kwargs):
        assert base64.b64encode(hashlib.md5(Body).digest()).decode() == ContentMD5
        self.uploads[UploadId]["parts"][PartNumber] = Body
        return {"ETag": '"' + hashlib.md5(Body).hexdigest() + '"'}
    def complete_multipart_upload(self, UploadId, MultipartUpload, **kwargs):
        upload = self.uploads.pop(UploadId)
        assert kwargs.get("IfNoneMatch") == "*"
        assert upload["key"] not in self.objects
        parts = [upload["parts"][i] for i in sorted(upload["parts"])]
        etag = '"' + hashlib.md5(b"".join(hashlib.md5(p).digest() for p in parts)).hexdigest() + '-' + str(len(parts)) + '"'
        self.objects[upload["key"]] = {"body": b"".join(parts), "meta": upload["meta"], "etag": etag}
        return {"ETag": etag}
    def abort_multipart_upload(self, UploadId, **kwargs):
        self.uploads.pop(UploadId, None)
        self.aborted += 1


class MigrationTests(unittest.TestCase):
    def fixture(self):
        nodes = {ROOT_ID: folder(ROOT_ID, "personal-root", "zohelo-data")}
        bodies = {}
        for i, layer in enumerate(("01_landing", "02_bronze", "03_silver", "04_gold", "05_archive", "06_control", "releases")):
            f, b = 'folder' + str(i), 'blob' + str(i)
            nodes[f] = folder(f, ROOT_ID, layer)
            body = (layer + " retained bytes").encode()
            nodes[b] = blob(b, f, "data.bin", body)
            bodies[b] = body
        return Source(nodes, bodies), S3()
    def first_node(self, source):
        return next(n for n in snapshot(source)["nodes"] if n["id"] == "blob0")
    def test_exhaustive_all_layers(self):
        source, s3 = self.fixture()
        result = migrate(lambda: source, lambda: s3, "landing", "lakehouse")
        self.assertEqual(result["result"], "copy_verified")
        self.assertEqual(result["verified_files"], 7)
        self.assertEqual(len(result["layers"]), 7)
        self.assertEqual(s3.objects["landing", "01_landing/data.bin"]["body"], source.bodies["blob0"])
        self.assertEqual(s3.objects["lakehouse", "04_gold/data.bin"]["body"], source.bodies["blob3"])
        self.assertNotIn(("lakehouse", CONTROL + "/current.json"), s3.objects)
        self.assertIn(("lakehouse", CONTROL + "/candidate.json"), s3.objects)
    def test_resume_downloads_nothing(self):
        source, s3 = self.fixture()
        migrate(lambda: source, lambda: s3, "landing", "lakehouse")
        source.downloads.clear()
        result = migrate(lambda: source, lambda: s3, "landing", "lakehouse")
        self.assertEqual(result["copied_files"], 0)
        self.assertEqual(result["reused_files"], 7)
        self.assertEqual(source.downloads, [])
    def test_multipart_is_bounded_and_preserves_bytes(self):
        source, s3 = self.fixture()
        node = self.first_node(source)
        result = copy_object(source, s3, node, "landing", "lakehouse", part_bytes=4)
        self.assertIn('-', result["etag"])
        self.assertEqual(s3.objects[result["bucket"], result["key"]]["body"], source.bodies[node["id"]])
    def test_bad_checksum_does_not_publish_small_object(self):
        source, s3 = self.fixture()
        node = self.first_node(source)
        source.bodies[node["id"]] = b"corrupt"
        with self.assertRaises(MigrationError):
            copy_object(source, s3, node, "landing", "lakehouse")
        self.assertFalse(s3.objects)
    def test_bad_checksum_aborts_multipart(self):
        source, s3 = self.fixture()
        node = self.first_node(source)
        source.bodies[node["id"]] = b"x" * int(node["size"])
        with self.assertRaises(MigrationError):
            copy_object(source, s3, node, "landing", "lakehouse", part_bytes=4)
        self.assertEqual(s3.aborted, 1)
        self.assertFalse(s3.objects)
    def test_truncation_rejected(self):
        source, s3 = self.fixture()
        node = self.first_node(source)
        source.bodies[node["id"]] = source.bodies[node["id"]][:-1]
        with self.assertRaises(MigrationError):
            copy_object(source, s3, node, "landing", "lakehouse")
    def test_oversize_rejected(self):
        source, s3 = self.fixture()
        node = self.first_node(source)
        source.bodies[node["id"]] += b"extra"
        with self.assertRaises(MigrationError):
            copy_object(source, s3, node, "landing", "lakehouse", part_bytes=4)
        self.assertFalse(s3.objects)
    def test_time_bound_does_not_publish(self):
        source, s3 = self.fixture()
        with self.assertRaises(TimeBoundReached):
            copy_object(source, s3, self.first_node(source), "landing", "lakehouse", deadline=0, part_bytes=4)
        self.assertFalse(s3.objects)
    def test_new_version_preserves_old(self):
        source, s3 = self.fixture()
        old = self.first_node(source)
        first = copy_object(source, s3, old, "landing", "lakehouse")
        source.bodies[old["id"]] = b"new version"
        changed = dict(old, **blob(old["id"], old["parent_id"], old["name"], b"new version"))
        second = copy_object(source, s3, changed, "landing", "lakehouse")
        self.assertNotEqual(first["key"], second["key"])
        self.assertIn((first["bucket"], first["key"]), s3.objects)
        self.assertTrue(second["key"].startswith("_drive_versions/"))
    def test_duplicate_names_retained(self):
        source, _ = self.fixture()
        source.nodes["duplicate"] = blob("duplicate", "folder0", "data.bin", b"another")
        plan = snapshot(source)
        keys = [n["key_path"] for n in plan["nodes"] if n["parent_id"] == "folder0"]
        self.assertEqual(len(set(keys)), 2)
        self.assertTrue(all("~drive-" in x for x in keys))
    def test_unsafe_names_are_encoded(self):
        self.assertEqual(component(".."), "%2E%2E")
        self.assertEqual(component("x/y"), "x%2Fy")
        self.assertEqual(component("x%2Fy"), "x%252Fy")
    def test_empty_folder_in_index(self):
        source, s3 = self.fixture()
        source.nodes["empty"] = folder("empty", ROOT_ID, "empty")
        result = migrate(lambda: source, lambda: s3, "landing", "lakehouse")
        raw = s3.objects["lakehouse", result["index_prefix"] + "/folders/empty.json"]["body"]
        self.assertEqual(json.loads(raw), {"files": []})
    def test_internal_shortcut_preserved_without_second_download(self):
        source, s3 = self.fixture()
        source.nodes["link"] = {"id": "link", "name": "link", "parents": ["folder0"],
                                "mimeType": SHORTCUT, "shortcutDetails": {"targetId": "blob1"}}
        result = migrate(lambda: source, lambda: s3, "landing", "lakehouse")
        self.assertEqual(result["result"], "copy_verified")
        self.assertEqual(len(source.downloads), 7)
    def test_external_shortcut_is_explicit_blocker(self):
        source, s3 = self.fixture()
        source.nodes["link"] = {"id": "link", "name": "link", "parents": ["folder0"],
                                "mimeType": SHORTCUT, "shortcutDetails": {"targetId": "private-personal-file"}}
        result = migrate(lambda: source, lambda: s3, "landing", "lakehouse")
        self.assertEqual(result["result"], "incomplete")
        self.assertNotIn(("lakehouse", CONTROL + "/candidate.json"), s3.objects)
        self.assertNotIn("private-personal-file", source.downloads)
    def test_source_change_blocks_candidate(self):
        source, s3 = self.fixture()
        original = source.chunks
        def chunks(node):
            yield from original(node)
            source.nodes[node["id"]]["version"] = "changed"
        source.chunks = chunks
        result = migrate(lambda: source, lambda: s3, "landing", "lakehouse")
        self.assertEqual(result["result"], "source_changed")
        self.assertNotIn(("lakehouse", CONTROL + "/candidate.json"), s3.objects)
    def test_other_drive_root_rejected(self):
        source, _ = self.fixture()
        with self.assertRaises(MigrationError):
            snapshot(source, root_id="personal-root")
    def test_checksum_conflict_rejected(self):
        source, _ = self.fixture()
        node = self.first_node(source)
        node["appProperties"] = {"sha256": "0" * 64}
        with self.assertRaises(MigrationError):
            source_hashes(node)
    def test_budget_is_failure_not_truncated_success(self):
        source, s3 = self.fixture()
        with self.assertRaises(MigrationError):
            migrate(lambda: source, lambda: s3, "landing", "lakehouse", max_bytes=1)
        self.assertEqual(source.downloads, [])
    def test_download_error_is_explicit_partial(self):
        source, s3 = self.fixture()
        source.bodies["blob3"] = b"wrong"
        result = migrate(lambda: source, lambda: s3, "landing", "lakehouse")
        self.assertEqual(result["result"], "incomplete")
        self.assertEqual(result["errors"], 1)
        self.assertFalse(result["portal_cutover"])
    def test_zero_byte_file(self):
        source, s3 = self.fixture()
        node = dict(self.first_node(source), **blob("blob0", "folder0", "empty.bin", b""))
        source.bodies["blob0"] = b""
        result = copy_object(source, s3, node, "landing", "lakehouse")
        self.assertEqual(result["size"], 0)


if __name__ == "__main__":
    unittest.main()
