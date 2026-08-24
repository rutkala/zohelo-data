import os
import sys
import yaml
import time
import argparse
import requests
from datetime import datetime, timedelta
from io import BytesIO
from googleapiclient.http import MediaIoBaseUpload

# Ensure Python locates storage_manager from src
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from storage_manager import StorageManager


class BaseIngestor:
    def __init__(self, config_path="config/sources.yaml"):
        with open(config_path, "r", encoding="utf-8") as f:
            self.catalog = yaml.safe_load(f).get("sources", {})
            
        self.storage = StorageManager(backend="gdrive")
        self.zone_ids = self.storage.init_infrastructure()

    def run(self, target_source: str = "all", mode: str = "incremental"):
        """
        Executes ingestion for 'all' sources or a specific 'source_id'.
        """
        if target_source.lower() == "all":
            print(f"🚀 Running [{mode.upper()}] ingestion across ALL {len(self.catalog)} sources...")
            for source_id in self.catalog.keys():
                self.extract_and_load(source_id, mode=mode)
        else:
            if target_source not in self.catalog:
                raise ValueError(f"Source '{target_source}' not found in catalog.")
            self.extract_and_load(target_source, mode=mode)

    def extract_and_load(self, source_id: str, mode: str = "incremental"):
        config = self.catalog[source_id]
        tech = config["technical"]
        load_config = config.get("load_methods", {}).get(mode)

        if not load_config:
            print(f"⚠️ Mode '{mode}' not configured for '{source_id}'. Skipping.")
            return

        print(f"\n==================================================")
        print(f"📥 Processing [{source_id}] | Mode: {mode.upper()}")
        print(f"==================================================")

        if mode == "incremental":
            self._fetch_and_store(
                url=load_config["endpoint"],
                params=load_config.get("params", {}),
                source_id=source_id,
                zone=tech["target_zone"],
                ext=tech["file_extension"],
                mode_tag=mode,
                tech=tech
            )
        elif mode == "full":
            strategy = load_config.get("pagination_strategy")
            if strategy == "date_chunking":
                self._run_date_chunking(source_id, tech, load_config)
            elif strategy == "none" or not strategy:
                self._fetch_and_store(
                    url=load_config["endpoint"],
                    params=load_config.get("params", {}),
                    source_id=source_id,
                    zone=tech["target_zone"],
                    ext=tech["file_extension"],
                    mode_tag=mode,
                    tech=tech
                )
            else:
                raise NotImplementedError(f"Pagination strategy '{strategy}' is not supported.")

    def _run_date_chunking(self, source_id: str, tech: dict, load_config: dict):
        """Paginates historical data in compliant date blocks (e.g., 93 days for NBP)."""
        start_date = datetime.strptime(load_config["historical_start_date"], "%Y-%m-%d")
        end_date = datetime.now()
        max_days = load_config.get("max_chunk_days", 90)

        current_start = start_date
        while current_start < end_date:
            current_end = min(current_start + timedelta(days=max_days - 1), end_date)
            url = load_config["endpoint_template"].format(
                start_date=current_start.strftime("%Y-%m-%d"),
                end_date=current_end.strftime("%Y-%m-%d")
            )

            date_tag = f"full_{current_start.strftime('%Y%m%d')}_{current_end.strftime('%Y%m%d')}"
            self._fetch_and_store(
                url=url,
                params=load_config.get("params", {}),
                source_id=source_id,
                zone=tech["target_zone"],
                ext=tech["file_extension"],
                mode_tag=date_tag,
                tech=tech
            )

            current_start = current_end + timedelta(days=1)
            time.sleep(0.1)  # Polite pacing to respect public API rate limits

    def _fetch_and_store(self, url: str, params: dict, source_id: str, zone: str, ext: str, mode_tag: str, tech: dict = None):
        print(f"🌐 Requesting: {url} | Params: {params}")
        response = requests.get(url, params=params)

        if response.status_code == 404:
            print(f"ℹ️ No data available (404 Not Found) for this range/endpoint.")
            return

        response.raise_for_status()

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{source_id}_{mode_tag}_{timestamp}.{ext}"

        if self.storage.backend == "gdrive":
            zone_id = self.zone_ids[zone]
            if tech is None:
                raise ValueError(f"'tech' config is required for GDrive backend (source: {source_id}).")
            source_system = tech["source_system"]
            landing_subpath = tech.get("landing_subpath", [])
            path_segments = [source_system] + landing_subpath
            target_folder_id = self.storage.get_or_create_nested_folder(path_segments, zone_id)
            file_metadata = {'name': filename, 'parents': [target_folder_id]}
            media = MediaIoBaseUpload(
                BytesIO(response.content),
                mimetype="application/json" if ext == "json" else "application/octet-stream",
                resumable=True
            )

            self.storage.drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            path_display = "/".join(path_segments)
            print(f"✅ Saved to Drive: {zone}/{path_display}/{filename}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Universal Data Ingestion Engine")
    parser.add_argument(
        "--source",
        type=str,
        default="all",
        help="Specific source_id from sources.yaml or 'all' (default: all)"
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["incremental", "full"],
        default="incremental",
        help="Extraction mode: 'incremental' or 'full' (default: incremental)"
    )
    args = parser.parse_args()

    ingestor = BaseIngestor()
    ingestor.run(target_source=args.source, mode=args.mode)
