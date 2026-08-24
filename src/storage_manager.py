import os
import json
import tempfile
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from google.oauth2.credentials import Credentials as UserCredentials
from google.auth.transport.requests import Request
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
        """Authenticates using OAuth Client credentials from environment"""
        creds_json = os.environ.get("GCP_SERVICE_ACCOUNT_JSON")
        if not creds_json:
            raise ValueError("Secret GCP_SERVICE_ACCOUNT_JSON not found in environment!")
        
        creds_dict = json.loads(creds_json)
        
        # Check if it's an OAuth Client ID (installed app) or service account
        if creds_dict.get('type') == 'service_account':
            # Service Account flow (kept for backward compatibility if needed)
            credentials = ServiceAccountCredentials.from_service_account_info(
                creds_dict, scopes=['https://www.googleapis.com/auth/drive']
            )
        else:
            # OAuth Client ID (installed app) flow - for your Google AI Pro account
            # This is a desktop/CLI flow that works with GitHub Actions
            flow = InstalledAppFlow.from_client_secrets_string(
                json.dumps(creds_dict),
                scopes=['https://www.googleapis.com/auth/drive']
            )
            # For headless (GitHub Actions), use run_local_server with port 0
            credentials = flow.run_local_server(port=0, open_browser=False)
        
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
