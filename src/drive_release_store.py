"""Google Drive transport for immutable release objects and one mutable pointer."""

import io

from googleapiclient.http import MediaIoBaseUpload


class DriveReleaseStore:
    def __init__(self, storage, root_id):
        self.storage = storage
        self.root_id = root_id
        self.files = storage.drive_service.files()

    @staticmethod
    def _quote(value):
        return value.replace("\\", "\\\\").replace("'", "\\'")

    def find(self, name, parent_id):
        result = []
        token = None
        while True:
            response = self.files.list(
                q=f"name='{self._quote(name)}' and '{self._quote(parent_id)}' in parents and trashed=false",
                fields="nextPageToken,files(id,mimeType)",
                spaces="drive", pageSize=100, pageToken=token,
            ).execute(num_retries=2)
            for item in response.get("files", []):
                result.append(item["id"])
            token = response.get("nextPageToken")
            if not token:
                return result

    def read(self, file_id):
        data = self.files.get_media(fileId=file_id).execute(num_retries=2)
        if not isinstance(data, bytes):
            raise ValueError("Drive did not return binary release contents")
        return data

    def mkdir(self, name, parent_id):
        self.storage.authorize_writes()
        return self.storage.get_or_create_nested_folder([name], root_id=parent_id)

    def create(self, name, data, parent_id):
        self.storage.authorize_writes()
        if not isinstance(data, bytes) or not data:
            raise ValueError("Release objects must contain bytes")
        allocated = self.files.generateIds(count=1, space="drive", type="files").execute()
        ids = allocated.get("ids", [])
        if len(ids) != 1 or not ids[0]:
            raise ValueError("Drive did not allocate a release object ID")
        file_id = ids[0]
        mime = "application/json" if name.endswith(".json") else "application/octet-stream"
        media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime, resumable=True)
        try:
            created = self.files.create(
                body={"id": file_id, "name": name, "parents": [parent_id], "mimeType": mime},
                media_body=media, fields="id",
            ).execute(num_retries=2)
            if created.get("id") != file_id:
                raise ValueError("Drive returned an unexpected release object ID")
        except Exception:
            # A timed-out request may already have created the reserved object.
            # Resolve only that identity; never search/delete by filename.
            if self.read(file_id) != data:
                raise
        return file_id

    def replace(self, file_id, data):
        self.storage.authorize_writes()
        metadata = self.files.get(
            fileId=file_id, fields="id,name,mimeType,parents,ownedByMe,trashed",
        ).execute(num_retries=2)
        if not (
            metadata.get("id") == file_id
            and metadata.get("name") == "current-release.json"
            and metadata.get("mimeType") == "application/json"
            and metadata.get("parents") == [self.root_id]
            and metadata.get("ownedByMe") is True
            and metadata.get("trashed") is False
        ):
            raise ValueError("Only the owned current-release pointer may be replaced")
        media = MediaIoBaseUpload(io.BytesIO(data), mimetype="application/json", resumable=False)
        self.files.update(fileId=file_id, media_body=media, fields="id").execute(num_retries=2)
