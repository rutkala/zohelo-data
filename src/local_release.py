"""Restore one verified Drive release to a disposable, portable local workspace."""
from __future__ import annotations

import json
from pathlib import Path
import tempfile

import duckdb

from release_protocol import restore_current_release
from release_validation import verify_local_dataset


class CachedReadStore:
    """Pin each downloaded object for this restoration, with a total byte budget."""

    def __init__(self, store, max_bytes=512 * 1024 * 1024):
        self.store = store
        self.max_bytes = max_bytes
        self.bytes_read = 0
        self.cache = {}

    def find(self, name, parent_id):
        return self.store.find(name, parent_id)

    def read(self, file_id):
        if file_id not in self.cache:
            data = self.store.read(file_id)
            if self.bytes_read + len(data) > self.max_bytes:
                raise ValueError("Local restoration exceeds its 512 MiB download budget")
            self.bytes_read += len(data)
            self.cache[file_id] = data
        return self.cache[file_id]


def quoted_identifier(value):
    return '"' + value.replace('"', '""') + '"'


def materialize_database(manifest, store, directory):
    """Materialize exact released Parquet into ordinary named DuckDB tables."""
    directory = Path(directory)
    database = directory / "release.duckdb"
    with duckdb.connect(str(database)) as connection:
        for index, dataset in enumerate(manifest["datasets"]):
            files = []
            for part, entry in enumerate(dataset["files"]):
                path = directory / f"data-{index}-{part}.parquet"
                path.write_bytes(store.read(entry["id"]))
                files.append(str(path))
            schema = quoted_identifier(dataset["layer"])
            table = quoted_identifier(dataset["table_name"])
            verify_local_dataset(connection, dataset, [Path(path) for path in files])
            connection.execute("DROP VIEW release_validation_dataset")
            connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            connection.read_parquet(files).create_view("restore_input", replace=True)
            connection.execute(f"CREATE TABLE {schema}.{table} AS SELECT * FROM restore_input")
            count = connection.execute(f"SELECT count(*) FROM {schema}.{table}").fetchone()[0]
            columns = [{"name": row[0], "type": row[1]} for row in
                       connection.execute(f"DESCRIBE {schema}.{table}").fetchall()]
            if count != dataset["row_count"] or columns != dataset["columns"]:
                raise ValueError(f"Restored table differs from release metadata: {dataset['dataset_id']}")
            connection.execute("DROP VIEW restore_input")
            for path in files:
                Path(path).unlink()
    return database


def restore_local_release(store, root_id, output_dir):
    """Read Drive only; never overwrite an existing local workspace."""
    output = Path(output_dir).resolve()
    if output.exists():
        raise ValueError("Choose a new output directory; existing workspaces are never overwritten")
    pinned = CachedReadStore(store)
    manifest = restore_current_release(pinned, root_id)
    if manifest["release_scope"] != "nbp_platform":
        raise ValueError("Local platform restoration requires a complete NBP platform release")
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".zohelo-restore-", dir=output.parent) as temporary:
        workspace = Path(temporary) / "workspace"
        workspace.mkdir()
        materialize_database(manifest, pinned, workspace)
        for artifact in manifest["artifacts"]:
            (workspace / artifact["name"]).write_bytes(pinned.read(artifact["id"]))
        (workspace / "release.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
        workspace.rename(output)
    return {"status": "local_release_restored", "read_only_remote": True,
            "release_id": manifest["release_id"], "code_sha": manifest["code_sha"],
            "database": str(output / "release.duckdb"), "output_dir": str(output),
            "download_bytes": pinned.bytes_read, "tables": len(manifest["datasets"])}
