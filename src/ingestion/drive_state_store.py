"""Drive transport whose only mutable ingestion object is the selected pointer."""
import io

from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

from drive_release_store import DriveReleaseStore


class DriveStateStore(DriveReleaseStore):
    def __init__(self, storage, platform_root_id, control_root_id):
        super().__init__(storage, platform_root_id)
        storage._assert_parent_within_selected_root(control_root_id)
        self.control_root_id = control_root_id

    def read(self, file_id):
        metadata = self.files.get(fileId=file_id, fields="id,size,trashed").execute(num_retries=2)
        size = int(metadata.get("size", -1))
        if metadata.get("id") != file_id or metadata.get("trashed") is not False or not 0 < size <= 12_000_000:
            raise ValueError("Ingestion object has missing or excessive size metadata")
        buffer = io.BytesIO()
        reader = MediaIoBaseDownload(buffer, self.files.get_media(fileId=file_id), chunksize=512 * 1024)
        done = False
        while not done:
            _, done = reader.next_chunk(num_retries=2)
            if buffer.tell() > size:
                raise ValueError("Ingestion object grew during transfer")
        if buffer.tell() != size:
            raise ValueError("Ingestion object size changed during transfer")
        return buffer.getvalue()

    def replace(self, file_id, data):
        self.storage.authorize_writes()
        self.storage._assert_parent_within_selected_root(self.control_root_id)
        metadata = self.files.get(
            fileId=file_id, fields="id,name,mimeType,parents,ownedByMe,trashed",
        ).execute(num_retries=2)
        if not (
            metadata.get("id") == file_id
            and metadata.get("name") == "current-ingestion-state.json"
            and metadata.get("mimeType") == "application/json"
            and metadata.get("parents") == [self.control_root_id]
            and metadata.get("ownedByMe") is True
            and metadata.get("trashed") is False
        ):
            raise ValueError("Only the owned ingestion-state pointer may be replaced")
        media = MediaIoBaseUpload(io.BytesIO(data), mimetype="application/json", resumable=False)
        self.files.update(fileId=file_id, media_body=media, fields="id").execute(num_retries=2)
