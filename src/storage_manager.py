import os
import json
import tempfile
import httplib2
from google.auth import load_credentials_from_dict
from google.auth.exceptions import DefaultCredentialsError
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from google.oauth2.credentials import Credentials as UserCredentials
from google_auth_httplib2 import Request as HttpLib2Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

class StorageManager:
    def __init__(self, backend="gdrive"):
        self.backend = backend
        self.master_folder_name = "zohelo-data"
        self.zones = [
            "01_landing", 
            "02_bronze", 
            "03_silver", 
            "04_gold", 
            "05_archive"
        ]
        
        if self.backend == "gdrive":
            self.drive_service = self._authenticate_gdrive()

    def _authenticate_gdrive(self):
        """Authenticates using service account JSON or OAuth credentials from env."""
        scopes = ['https://www.googleapis.com/auth/drive']
        is_github_actions = os.environ.get("GITHUB_ACTIONS", "").lower() == "true"
        creds_json = os.environ.get("GCP_SERVICE_ACCOUNT_JSON")
        creds_dict = json.loads(creds_json) if creds_json else None

        if creds_dict and creds_dict.get('type') == 'service_account':
            credentials = ServiceAccountCredentials.from_service_account_info(
                creds_dict, scopes=scopes
            )
            return build('drive', 'v3', credentials=credentials)

        if creds_dict and creds_dict.get("type"):
            try:
                credentials, _ = load_credentials_from_dict(creds_dict, scopes=scopes)
            except (DefaultCredentialsError, ValueError):
                pass
            else:
                return build("drive", "v3", credentials=credentials)

        # Preferred OAuth path when service account JSON is not available.
        oauth_client_id = os.environ.get("GOOGLE_OAUTH_CLIENT_ID")
        oauth_client_secret = os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET")
        oauth_refresh_token = os.environ.get("GOOGLE_OAUTH_REFRESH_TOKEN")

        if oauth_client_id and oauth_client_secret and oauth_refresh_token:
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

        if is_github_actions:
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
                raise RuntimeError("Failed to refresh Google OAuth access token in GitHub Actions.") from exc
        else:
            # Local development flow (interactive browser auth)
            flow = InstalledAppFlow.from_client_config(creds_dict, scopes=scopes)
            credentials = flow.run_local_server(port=0)
        
        return build('drive', 'v3', credentials=credentials)

    def _get_or_create_folder(self, folder_name, parent_id=None):
        """Finds a folder by name, or creates it if it doesn't exist."""
        query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        if parent_id:
            query += f" and '{parent_id}' in parents"

        results = self.drive_service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
        files = results.get('files', [])

        if files:
            return files[0]['id']
        else:
            print(f"🏗️ Creating folder '{folder_name}'...")
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            if parent_id:
                file_metadata['parents'] = [parent_id]
                
            folder = self.drive_service.files().create(body=file_metadata, fields='id').execute()
            return folder.get('id')

    def init_infrastructure(self):
        """The core product: Deploys the logical folder structure to the storage backend."""
        print(f"Initializing {self.backend} storage infrastructure...")
        
        if self.backend == "gdrive":
            master_id = self._get_or_create_folder(self.master_folder_name)
            zone_ids = {}
            for zone in self.zones:
                zone_ids[zone] = self._get_or_create_folder(zone, parent_id=master_id)
            
            print("✅ Infrastructure sync complete! Storage is ready.")
            return zone_ids

    def get_or_create_nested_folder(self, path_segments: list, root_id: str) -> str:
        """Creates nested folders from path_segments under root_id, returning the deepest folder ID."""
        if not path_segments:
            raise ValueError("path_segments must not be empty — files must land in a named source folder.")
        current_id = root_id
        for segment in path_segments:
            current_id = self._get_or_create_folder(segment, parent_id=current_id)
        return current_id

    def get_path(self, zone_name: str, filename: str = "") -> str:
        """
        The Abstraction Gateway: Returns the universal URI for any file.
        Example: get_path("01_landing", "data.json") -> "gdrive://zohelo-data/01_landing/data.json"
        """
        if zone_name not in self.zones:
            raise ValueError(f"Zone '{zone_name}' is not a valid Medallion zone.")
        
        base_uri = f"{self.backend}://{self.master_folder_name}/{zone_name}"
        
        if filename:
            return f"{base_uri}/{filename}"
        return base_uri

    def _get_duckdb_credentials_path(self):
        """Creates an ephemeral temp file for DuckDB to authenticate."""
        creds_json = os.environ.get("GCP_SERVICE_ACCOUNT_JSON")
        if not creds_json:
            raise ValueError("Secret GCP_SERVICE_ACCOUNT_JSON not found in environment!")
            
        temp = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json')
        temp.write(creds_json)
        temp.close()
        return temp.name 

    def setup_duckdb(self):
        """
        Initializes an in-memory DuckDB connection with the necessary extensions 
        to read the configured storage protocol (e.g., gdrive://).
        """
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

# --- Deployment Execution ---
if __name__ == "__main__":
    # Strictly initializes the declarative folder infrastructure
    storage = StorageManager(backend="gdrive")
    storage.init_infrastructure()
