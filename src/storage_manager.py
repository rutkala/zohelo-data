import os
import json
import tempfile
from google.oauth2 import service_account
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
        """Authenticates using the GitHub Codespace Secret"""
        creds_json = os.environ.get("GCP_SERVICE_ACCOUNT_JSON")
        if not creds_json:
            raise ValueError("Secret GCP_SERVICE_ACCOUNT_JSON not found in environment!")
        
        creds_dict = json.loads(creds_json)
        credentials = service_account.Credentials.from_service_account_info(
            creds_dict, scopes=['https://www.googleapis.com/auth/drive']
        )
        return build('drive', 'v3', credentials=credentials)

    def _get_or_create_folder(self, folder_name, parent_id=None):
        """Finds a folder by name, or creates it if it doesn't exist."""
        query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        if parent_id:
            query += f" and '{parent_id}' in parents"

        results = self.drive_service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
        files = results.get('files', [])

        if files:
            print(f"✅ Folder '{folder_name}' already exists (ID: {files[0]['id']})")
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
            # 1. Create the Master Folder
            master_id = self._get_or_create_folder(self.master_folder_name)
            
            # 2. Create the Medallion Zones inside the Master Folder
            zone_ids = {}
            for zone in self.zones:
                zone_ids[zone] = self._get_or_create_folder(zone, parent_id=master_id)
            
            print("\n🚀 Infrastructure deployment complete! Storage is ready.")
            return zone_ids

    def get_duckdb_credentials_path(self):
        """Creates an ephemeral temp file for DuckDB to authenticate."""
        creds_json = os.environ.get("GCP_SERVICE_ACCOUNT_JSON")
        temp = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json')
        temp.write(creds_json)
        temp.close()
        return temp.name # Returns e.g. /tmp/tmpxyz123.json

# --- Deployment Execution ---
if __name__ == "__main__":
    storage = StorageManager(backend="gdrive")
    storage.init_infrastructure()
