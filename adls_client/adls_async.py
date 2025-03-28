"""
High-level async ADLS operations, inherits from ADLSBaseClientAsync.
"""
import os
from .client_async import ADLSBaseClientAsync

class ADLSClientAsync(ADLSBaseClientAsync):
    """
    Async Azure Data Lake Storage Gen2 client
    exposing file/folder operations using httpx.
    """
    async def create_file(self, path: str):
        await self.put(path, params={"resource": "file"}, headers={"Content-Length": "0"})

    async def create_folder(self, path: str):
        await self.put(path, params={"resource": "directory"}, headers={"Content-Length": "0"})

    async def list_directory(self, path: str = ""):
        params = {"resource": "filesystem", "recursive": "false"}
        if path and path not in ["/", ""]:
            if path.startswith("/"):
                path = path[1:]
            params["directory"] = path

        resp = await self.get("", params=params)
        try:
            result = resp.json()
            return result.get("paths", [])
        except ValueError:
            raise RuntimeError("Invalid JSON response in list_directory")

    async def read_file(self, path: str) -> bytes:
        resp = await self.get(path)
        return resp.content

    async def delete(self, path: str, recursive: bool = False):
        params = {}
        if recursive:
            params["recursive"] = "true"
        await self.delete_request(path, params=params)

    async def upload_large_file(self, local_path: str, remote_path: str, chunk_size: int = 4 * 1024 * 1024):
        """
        Upload large files using the create/append/flush sequence.
        """
        file_size = os.path.getsize(local_path)
        # Create file
        await self.create_file(remote_path)

        position = 0
        with open(local_path, "rb") as f:
            while True:
                data = f.read(chunk_size)
                if not data:
                    break
                # Append chunk
                await self.patch(
                    remote_path,
                    params={"action": "append", "position": str(position)},
                    data=data
                )
                position += len(data)

        # Flush data
        await self.patch(remote_path, params={"action": "flush", "position": str(position), "close": "true"})
