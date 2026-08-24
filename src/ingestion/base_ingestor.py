import yaml
import requests
from datetime import datetime, timedelta
from io import BytesIO
import sys
import os

# Ensure Python can find the storage_manager module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from storage_manager import StorageManager

class BaseIngestor:
    def __init__(self, config_path="config/sources.yaml"):
        with open(config_path, "r") as f:
            self.catalog = yaml.safe_load(f).get("sources", {})
            
        self.storage = StorageManager(backend="gdrive")
        # Securely fetch exact folder IDs for the current run
        self.zone_ids = self.storage.init_infrastructure()

    def run_extraction(self, source_id: str, mode: str = "incremental"):
        if source_id not in self.catalog:
            raise ValueError(f"Source {source_id} missing from catalog.")
            
        config = self.catalog[source_id]
        tech = config["technical"]
        load_config = config["load_methods"].get(mode)
        
        print(f"📥 Starting [{mode}] load for {source_id}")

        if mode == "incremental":
            self._fetch_and_store(
                url=load_config["endpoint"],
                params=load_config.get("params", {}),
                source_id=source_id,
                zone=tech["target_zone"],
                ext=tech["file_extension"],
                mode=mode
            )
            
        elif mode == "full" and load_config.get("pagination_strategy") == "date_chunking":
            self._run_date_chunking(source_id, tech, load_config)

    def _run_date_chunking(self, source_id, tech, load_config):
        """Autonomously paginates through historical timelines in NBP-compliant chunks."""
        start_date = datetime.strptime(load_config["historical_start_date"], "%Y-%m-%d")
        end_date = datetime.now()
        max_days = load_config["max_chunk_days"]
        
        current_start = start_date
        while current_start < end_date:
            current_end = min(current_start + timedelta(days=max_days - 1), end_date)
            url = load_config["endpoint_template"].format(
                start_date=current_start.strftime("%Y-%m-%d"),
                end_date=current_end.strftime("%Y-%m-%d")
            )
            
            suffix = f"full_{current_start.strftime('%Y%m%d')}_{current_end.strftime('%Y%m%d')}"
            
            self._fetch_and_store(
                url=url,
                params=load_config.get("params", {}),
                source_id=source_id,
                zone=tech["target_zone"],
                ext=tech["file_extension"],
                mode=suffix
            )
            current_start = current_end + timedelta(days=1)

    def _fetch_and_store(self, url, params, source_id, zone, ext, mode):
        print(f"Fetching: {url}")
        response = requests.get(url, params=params)
        
        if response.status_code == 404:
            print("No data available for this range.")
            return
            
        response.raise_for_status()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{source_id}_{mode}_{timestamp}.{ext}"
        
        from googleapiclient.http import MediaIoBaseUpload
        file_metadata = {'name': filename, 'parents': [self.zone_ids[zone]]}
        media = MediaIoBaseUpload(BytesIO(response.content), mimetype="application/octet-stream", resumable=True)
        
        self.storage.drive_service.files().create(
            body=file_metadata, 
            media_body=media, 
            fields='id'
        ).execute()
        print(f"✅ Saved {filename} to {zone}")

if __name__ == "__main__":
    ingestor = BaseIngestor()
    # Target execution for testing
    ingestor.run_extraction("nbp_exchange_rates_table_a", mode="incremental")
