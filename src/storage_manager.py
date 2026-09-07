import json
import os
import re
import tempfile
import unicodedata
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import unquote, urlsplit

import httplib2
import yaml
from google.auth import load_credentials_from_dict
from google.auth.exceptions import DefaultCredentialsError
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from google.oauth2.credentials import Credentials as UserCredentials
from google_auth_httplib2 import Request as HttpLib2Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build


class StorageManager:
    """Address and manage the durable storage hierarchy."""

    FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"
    _DEFAULT_ROOT_NAME = "zohelo-data"
    _DEFAULT_ZONES = {
        "landing": ("01_landing",),
        "archive": ("05_archive",),
        "bronze": ("02_bronze",),
        "silver": ("03_silver",),
        "gold": ("04_gold",),
    }

    def __init__(self, backend="gdrive", *, allow_interactive_auth=True,
                 root_name=None, root_id=None, config_path=None):
        self.backend = backend
        self.allow_interactive_auth = allow_interactive_auth
        self.auth_source = "not_selected"

        config_root_name, config_zones = self._read_storage_config(config_path)
        env_root_name = os.environ.get("ZOHELO_DRIVE_ROOT_NAME")
        env_root_id = os.environ.get("ZOHELO_DRIVE_ROOT_ID")
        configured_root_name = (
            root_name if root_name is not None
            else env_root_name if env_root_name is not None
            else config_root_name
        )
        configured_root_id = root_id if root_id is not None else env_root_id
        self._validate_segment(configured_root_name, "root_name")
        if configured_root_id is not None:
            self._validate_drive_id(configured_root_id, "root_id")

        self.root_name = configured_root_name
        self.master_folder_name = configured_root_name
        self.root_id = str(configured_root_id) if configured_root_id is not None else None
        self._configured_root_name = configured_root_name
        self._resolved_root_metadata = None
        self.zone_paths = dict(config_zones)
        # Existing callers use physical numbered names as zone-ID map keys.
        self.zones = ["/".join(path) for path in self.zone_paths.values()]

        if self.backend == "gdrive":
            self.drive_service = self._authenticate_gdrive()

    @classmethod
    def _read_storage_config(cls, config_path=None):
        default_path = Path(__file__).resolve().parents[1] / "config" / "storage.yaml"
        path = Path(config_path) if config_path is not None else default_path
        try:
            with path.open("r", encoding="utf-8") as handle:
                document = yaml.safe_load(handle) or {}
        except FileNotFoundError:
            if config_path is not None:
                raise ValueError(f"Storage configuration file not found: {path}")
            document = {}
        except (OSError, yaml.YAMLError) as exc:
            raise ValueError(f"Unable to read storage configuration {path}: {exc}") from exc

        if not isinstance(document, dict):
            raise ValueError("Storage configuration root must be a mapping.")
        storage = document.get("storage", {})
        if not isinstance(storage, dict):
            raise ValueError("Storage configuration must contain a mapping at 'storage'.")
        protocol = storage.get("protocol", "gdrive")
        if protocol != "gdrive":
            raise ValueError(f"Unsupported storage protocol '{protocol}'; only 'gdrive' is implemented.")
        base_path = storage.get("base_path")
        root_name = storage.get("root_name")
        if root_name is None and base_path:
            root_name = cls._root_name_from_base_path(base_path)
        root_name = cls._DEFAULT_ROOT_NAME if root_name is None else root_name
        cls._validate_segment(root_name, "configured root name")

        configured_zones = storage.get("zones")
        if configured_zones is None:
            zone_paths = dict(cls._DEFAULT_ZONES)
        elif isinstance(configured_zones, dict):
            zone_paths = {}
            for alias, value in configured_zones.items():
                cls._validate_segment(alias, "zone alias")
                if not isinstance(value, str):
                    raise ValueError(f"Zone '{alias}' must map to a path string.")
                segments = tuple(value.split("/"))
                cls._validate_path_segments(segments, f"zone '{alias}'")
                zone_paths[str(alias)] = segments
        else:
            raise ValueError("Storage configuration 'zones' must be a mapping.")
        if not zone_paths:
            raise ValueError("Storage configuration must define at least one zone.")
        return root_name, zone_paths

    @staticmethod
    def _root_name_from_base_path(base_path: Any) -> str:
        if not isinstance(base_path, str) or not base_path.strip():
            raise ValueError("storage.base_path must be a non-empty URI.")
        parsed = urlsplit(base_path)
        if not parsed.scheme or not parsed.netloc or parsed.query or parsed.fragment:
            raise ValueError("storage.base_path must be a URI such as gdrive://zohelo-data.")
        if [part for part in parsed.path.split("/") if part]:
            raise ValueError("storage.base_path must identify one root folder, not a nested path.")
        return unquote(parsed.netloc)

    @classmethod
    def _validate_segment(cls, segment: Any, label="path segment"):
        if not isinstance(segment, str) or not segment or not segment.strip() or segment in {".", ".."}:
            raise ValueError(f"{label} must be a non-empty path segment.")
        if "/" in segment or "\\" in segment or any(unicodedata.category(c) == "Cc" for c in segment):
            raise ValueError(f"{label} contains invalid path characters.")
        return segment

    @staticmethod
    def _validate_drive_id(value: Any, label="Drive ID"):
        if not isinstance(value, str) or re.fullmatch(r"[A-Za-z0-9_-]+", value) is None:
            raise ValueError(f"{label} must contain only letters, numbers, '_' or '-'.")
        return value

    @classmethod
    def _validate_path_segments(cls, segments: Iterable[str], label="path"):
        segments = tuple(segments)
        if not segments:
            raise ValueError(f"{label} must contain at least one path segment.")
        for index, segment in enumerate(segments):
            cls._validate_segment(segment, f"{label} segment {index}")
        return segments

    @staticmethod
    def _escape_drive_query_literal(value: str) -> str:
        return value.replace("\\", "\\\\").replace("'", "\\'")

    def _authenticate_gdrive(self):
        """Prefer complete owner OAuth credentials; otherwise use legacy JSON auth."""
        scopes = ['https://www.googleapis.com/auth/drive']
        is_github_actions = os.environ.get("GITHUB_ACTIONS", "").lower() == "true"

        # One explicit, complete OAuth configuration wins in every runtime.
        # Never switch identities when its refresh fails, and do not parse
        # unused legacy credentials when this path is selected.
        oauth_client_id = os.environ.get("GOOGLE_OAUTH_CLIENT_ID")
        oauth_client_secret = os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET")
        oauth_refresh_token = os.environ.get("GOOGLE_OAUTH_REFRESH_TOKEN")

        if oauth_client_id and oauth_client_secret and oauth_refresh_token:
            self.auth_source = "oauth_environment"
            credentials = UserCredentials(
                token=None,
                refresh_token=oauth_refresh_token,
                token_uri="https://oauth2.googleapis.com/token",
                client_id=oauth_client_id,
                client_secret=oauth_client_secret,
                scopes=scopes,
            )
            try:
                credentials.refresh(HttpLib2Request(httplib2.Http()))
            except Exception as exc:
                raise RuntimeError("Failed to refresh Google OAuth access token from GOOGLE_OAUTH_* env vars.") from exc
            return build('drive', 'v3', credentials=credentials)

        creds_json = os.environ.get("GCP_SERVICE_ACCOUNT_JSON")
        creds_dict = json.loads(creds_json) if creds_json else None

        is_service_account_type = bool(
            creds_dict and creds_dict.get("type") == "service_account"
        )
        has_service_account_fields_without_type = bool(
            creds_dict
            and creds_dict.get("type") is None
            and creds_dict.get("client_email")
            and creds_dict.get("private_key")
        )
        if is_service_account_type or has_service_account_fields_without_type:
            self.auth_source = "service_account_json"
            service_account_info = dict(creds_dict)
            service_account_info.setdefault("type", "service_account")
            service_account_info.setdefault("token_uri", "https://oauth2.googleapis.com/token")
            credentials = ServiceAccountCredentials.from_service_account_info(
                service_account_info, scopes=scopes
            )
            return build('drive', 'v3', credentials=credentials)

        if creds_dict and creds_dict.get("type"):
            try:
                credentials, _ = load_credentials_from_dict(creds_dict, scopes=scopes)
            except (DefaultCredentialsError, ValueError):
                pass
            else:
                self.auth_source = (
                    "authorized_user_json"
                    if creds_dict.get("type") == "authorized_user"
                    else "typed_credentials_json"
                )
                return build("drive", "v3", credentials=credentials)

        # Fallback path: OAuth client JSON in GCP_SERVICE_ACCOUNT_JSON.
        if not creds_dict:
            raise ValueError(
                "Google credentials not found. Set GOOGLE_OAUTH_CLIENT_ID, GOOGLE_OAUTH_CLIENT_SECRET, "
                "GOOGLE_OAUTH_REFRESH_TOKEN or GCP_SERVICE_ACCOUNT_JSON."
            )
        
        oauth_client = creds_dict.get("installed") or creds_dict.get("web")
        if not oauth_client:
            raise ValueError("OAuth client JSON must include an 'installed' or 'web' section.")
        client_id = oauth_client.get("client_id")
        client_secret = oauth_client.get("client_secret")
        token_uri = oauth_client.get("token_uri", "https://oauth2.googleapis.com/token")
        resolved_client_id = client_id or oauth_client_id
        resolved_client_secret = client_secret or oauth_client_secret
        if not resolved_client_id or not resolved_client_secret:
            raise ValueError("OAuth client JSON is missing client_id/client_secret.")

        self.auth_source = "oauth_client_json"
        if is_github_actions or not self.allow_interactive_auth:
            refresh_token = oauth_refresh_token
            if not refresh_token:
                raise ValueError("Secret GOOGLE_OAUTH_REFRESH_TOKEN not found in environment!")

            credentials = UserCredentials(
                token=None,
                refresh_token=refresh_token,
                token_uri=token_uri,
                client_id=resolved_client_id,
                client_secret=resolved_client_secret,
                scopes=scopes,
            )
            try:
                credentials.refresh(HttpLib2Request(httplib2.Http()))
            except Exception as exc:
                raise RuntimeError("Failed to refresh Google OAuth access token in the non-interactive OAuth client flow.") from exc
        else:
            # Local development flow (interactive browser auth)
            flow = InstalledAppFlow.from_client_config(creds_dict, scopes=scopes)
            credentials = flow.run_local_server(port=0)
        
        return build('drive', 'v3', credentials=credentials)

    def _list_exact_folders(self, folder_name: str, parent_id: str | None = None):
        self._validate_segment(folder_name, "folder name")
        if parent_id is not None:
            self._validate_drive_id(parent_id, "parent_id")
        escaped_name = self._escape_drive_query_literal(folder_name)
        query = f"name='{escaped_name}' and mimeType='{self.FOLDER_MIME_TYPE}' and trashed=false"
        if parent_id:
            query += f" and '{self._escape_drive_query_literal(parent_id)}' in parents"
        folders = []
        page_token = None
        while True:
            list_args = {
                "q": query,
                "spaces": "drive",
                "fields": "nextPageToken, files(id, name, mimeType, trashed)",
            }
            if page_token:
                list_args["pageToken"] = page_token
            response = self.drive_service.files().list(**list_args).execute()
            folders.extend(response.get("files", []))
            page_token = response.get("nextPageToken")
            if not page_token:
                break
        return [item for item in folders
            if item.get("mimeType") == self.FOLDER_MIME_TYPE and item.get("name") == folder_name
            and item.get("trashed") is not True]

    def _get_or_create_folder(self, folder_name, parent_id=None):
        """Find an exact folder, or create it after authorizing the write."""
        self.authorize_writes()
        folders = self._list_exact_folders(folder_name, parent_id=parent_id)
        if len(folders) > 1:
            parent_desc = " under a parent folder" if parent_id else " at Drive root"
            raise ValueError(f"Ambiguous folder '{folder_name}'{parent_desc}: multiple exact folders found.")
        if folders:
            return folders[0]["id"]
        print(f"🏗️ Creating folder '{folder_name}'...")
        metadata = {"name": folder_name, "mimeType": self.FOLDER_MIME_TYPE}
        if parent_id:
            metadata["parents"] = [parent_id]
        folder_id = self.drive_service.files().create(body=metadata, fields="id").execute().get("id")
        if not folder_id:
            raise ValueError(f"Drive did not return an ID when creating folder '{folder_name}'.")
        return folder_id

    def _resolve_root_by_id(self):
        if not self.root_id:
            return None
        if self._resolved_root_metadata is not None:
            return self._resolved_root_metadata
        try:
            metadata = self.drive_service.files().get(fileId=self.root_id,
                fields="id, name, mimeType, trashed").execute()
        except Exception:
            raise ValueError("Configured Drive root ID could not be verified.") from None
        if (not metadata or metadata.get("id", self.root_id) != self.root_id
                or metadata.get("mimeType") != self.FOLDER_MIME_TYPE
                or metadata.get("trashed") is not False or not metadata.get("name")):
            raise ValueError("Configured Drive root ID must identify an untrashed folder.")
        self._resolved_root_metadata = metadata
        self.master_folder_name = metadata["name"]
        return metadata

    def resolve_root(self, create=False):
        """Return the configured root folder ID, optionally creating it."""
        if self.backend != "gdrive":
            raise ValueError("Drive root resolution is only available for the gdrive backend.")
        if self.root_id:
            return self._resolve_root_by_id()["id"]
        folders = self._list_exact_folders(self.master_folder_name)
        if len(folders) > 1:
            raise ValueError(f"Ambiguous root folder '{self.master_folder_name}': multiple exact folders found.")
        if folders:
            return folders[0]["id"]
        if not create:
            raise ValueError(f"Storage root folder '{self.master_folder_name}' was not found (read-only resolution).")
        return self._get_or_create_folder(self.master_folder_name)

    def _zone_segments(self, zone_name: str):
        self._validate_segment(zone_name, "zone name")
        if zone_name in self.zone_paths:
            return self.zone_paths[zone_name]
        for segments in self.zone_paths.values():
            if len(segments) == 1 and segments[0] == zone_name:
                return segments
        raise ValueError(f"Zone '{zone_name}' is not configured.")

    def resolve_zone(self, zone_name, create=False):
        """Resolve a configured zone (alias or physical name) below the root."""
        segments = self._zone_segments(zone_name)
        current_id = self.resolve_root(create=create)
        if create:
            self.authorize_writes()
        for segment in segments:
            folders = self._list_exact_folders(segment, parent_id=current_id)
            if len(folders) > 1:
                raise ValueError(f"Ambiguous zone path segment '{segment}' under a parent folder.")
            if folders:
                current_id = folders[0]["id"]
            elif create:
                current_id = self._get_or_create_folder(segment, parent_id=current_id)
            else:
                raise ValueError(f"Zone path '{'/'.join(segments)}' was not found (read-only resolution).")
        return current_id

    def authorize_writes(self):
        """Authorize a mutating Drive operation for the selected root."""
        selected_name = self.master_folder_name
        if self.root_id:
            selected_name = self._resolve_root_by_id()["name"]
        if os.environ.get("GITHUB_ACTIONS", "").lower() == "true":
            return True
        if selected_name == self._DEFAULT_ROOT_NAME and os.environ.get("ZOHELO_ALLOW_PRODUCTION_WRITES", "").lower() != "true":
            raise PermissionError("Writes to the production Drive root 'zohelo-data' require ZOHELO_ALLOW_PRODUCTION_WRITES=true outside GitHub Actions.")
        return True

    def _assert_parent_within_selected_root(self, parent_id: str):
        """Reject writes whose supplied parent is outside the selected root."""
        selected_root_id = self.resolve_root(create=False)
        if parent_id == selected_root_id:
            return

        current_id = parent_id
        visited = set()
        while current_id != selected_root_id:
            self._validate_drive_id(current_id, "parent_id")
            if current_id in visited:
                raise ValueError("Parent folder ancestry is cyclic and could not be verified.")
            visited.add(current_id)
            try:
                metadata = self.drive_service.files().get(
                    fileId=current_id,
                    fields="id, parents, mimeType, trashed",
                ).execute()
            except Exception:
                raise ValueError("Parent folder could not be verified inside the selected root.") from None
            if (
                not metadata
                or metadata.get("id", current_id) != current_id
                or metadata.get("mimeType") != self.FOLDER_MIME_TYPE
                or metadata.get("trashed") is not False
            ):
                raise ValueError("Parent folder must be an untrashed folder inside the selected root.")
            parents = metadata.get("parents") or []
            if not parents:
                raise ValueError("Parent folder is outside the selected root.")
            for candidate in parents:
                self._validate_drive_id(candidate, "parent_id")
            if selected_root_id in parents:
                return
            # Drive normally has one parent. If legacy multi-parent metadata
            # exists, verify each branch until one reaches the selected root.
            next_id = parents[0]
            for candidate in parents[1:]:
                try:
                    self._assert_ancestor_branch(candidate, selected_root_id, visited)
                except ValueError:
                    continue
                return
            current_id = next_id
        return

    def _assert_ancestor_branch(self, current_id: str, selected_root_id: str, visited: set):
        while current_id != selected_root_id:
            self._validate_drive_id(current_id, "parent_id")
            if current_id in visited:
                raise ValueError("Parent folder ancestry is cyclic and could not be verified.")
            visited.add(current_id)
            try:
                metadata = self.drive_service.files().get(
                    fileId=current_id,
                    fields="id, parents, mimeType, trashed",
                ).execute()
            except Exception:
                raise ValueError("Parent folder could not be verified inside the selected root.") from None
            if (
                not metadata
                or metadata.get("id", current_id) != current_id
                or metadata.get("mimeType") != self.FOLDER_MIME_TYPE
                or metadata.get("trashed") is not False
            ):
                raise ValueError("Parent folder must be an untrashed folder inside the selected root.")
            parents = metadata.get("parents") or []
            if not parents:
                raise ValueError("Parent folder is outside the selected root.")
            if selected_root_id in parents:
                return
            current_id = parents[0]
        return

    def init_infrastructure(self):
        """Deploy the configured folder structure to the storage backend."""
        print(f"Initializing {self.backend} storage infrastructure...")
        if self.backend == "gdrive":
            self.authorize_writes()
            self.resolve_root(create=True)
            zone_ids = {
                "/".join(path): self.resolve_zone(alias, create=True)
                for alias, path in self.zone_paths.items()
            }
            print("✅ Infrastructure sync complete! Storage is ready.")
            return zone_ids
        return None

    def get_or_create_nested_folder(self, path_segments: list, root_id: str) -> str:
        """Create nested folders safely under ``root_id`` and return the leaf ID."""
        segments = self._validate_path_segments(path_segments, "path_segments")
        if not root_id or not isinstance(root_id, str):
            raise ValueError("root_id must be a non-empty Drive folder ID.")
        self._validate_drive_id(root_id, "root_id")
        self._assert_parent_within_selected_root(root_id)
        self.authorize_writes()
        current_id = root_id
        for segment in segments:
            current_id = self._get_or_create_folder(segment, parent_id=current_id)
        return current_id

    def get_path(self, zone_name: str, filename: str = "") -> str:
        """Return the universal URI for a configured zone and optional file."""
        segments = self._zone_segments(zone_name)
        base_uri = f"{self.backend}://{self.master_folder_name}/{'/'.join(segments)}"
        return f"{base_uri}/{filename}" if filename else base_uri

    def _get_duckdb_credentials_path(self):
        """Creates an ephemeral temp file for DuckDB to authenticate."""
        creds_json = os.environ.get("GCP_SERVICE_ACCOUNT_JSON")
        if not creds_json:
            raise ValueError("Secret GCP_SERVICE_ACCOUNT_JSON not found in environment!")
        temp = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json")
        temp.write(creds_json)
        temp.close()
        return temp.name

    def setup_duckdb(self):
        """Initialize an in-memory DuckDB connection for the configured protocol."""
        import duckdb
        con = duckdb.connect()
        if self.backend == "gdrive":
            print("🔧 Configuring DuckDB Virtual File System for Google Drive...")
            con.execute("INSTALL gdrive FROM community;")
            con.execute("LOAD gdrive;")
            key_path = self._get_duckdb_credentials_path()
            con.execute(f"""
                CREATE SECRET IF NOT EXISTS gdrive_secret (
                    TYPE gdrive,
                    PROVIDER service_account,
                    KEY_FILE '{key_path}',
                    SCOPES 'https://www.googleapis.com/auth/drive'
                );
            """)
        return con


if __name__ == "__main__":
    storage = StorageManager(backend="gdrive")
    storage.init_infrastructure()
