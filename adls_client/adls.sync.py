"""
High-level sync ADLS operations, inherits from ADLSBaseClientSync.
"""
import os
from .client_sync import ADLSBaseClientSync

class ADLSClientSync(ADLSBaseClientSync):
    """
    Sync Azure Data Lake Storage Gen2 client
    exposing file/folder operations using requests.
    """
    def create_file(self, path: str):
        self.put(path, params={"resource": "file"}, headers={"Content-Length": "0"})

    def create_folder(self, path: str):
        self.put(path, params={"resource": "directory"}, headers={"Content-Length": "0"})

    def list_directory(self, path: str = ""):
        params = {"resource": "filesystem", "recursive": "false"}
        if path and path not in ["/", ""]:
            if path.startswith("/"):
                path = path[1:]
            params["directory"] = path

        resp = self.get("", params=params)
        try:
            result = resp.json()
            return result.get("paths", [])
        except ValueError:
            raise RuntimeError("Invalid JSON response in list_directory")

    def read_file(self, path: str) -> bytes:
        resp = self.get(path)
        return resp.content

    def delete(self, path: str, recursive: bool = False):
        params = {}
        if recursive:
            params["recursive"] = "true"
        self.delete_request(path, params=params)

    def upload_large_file(self, local_path: str, remote_path: str, chunk_size: int = 4 * 1024 * 1024):
        file_size = os.path.getsize(local_path)
        # Create file
        self.create_file(remote_path)

        position = 0
        with open(local_path, "rb") as f:
            while True:
                data = f.read(chunk_size)
                if not data:
                    break
                # Append chunk
                self.patch(
                    remote_path,
                    params={"action": "append", "position": str(position)},
                    data=data
                )
                position += len(data)

        # Flush data
        self.patch(remote_path, params={"action": "flush", "position": str(position), "close": "true"})
