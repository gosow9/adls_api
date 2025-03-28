"""
Async base ADLS client: wraps low-level HTTP methods via httpx.
"""
import httpx
from .client_base import ADLSBaseClient

class ADLSBaseClientAsync(ADLSBaseClient):
    def __init__(self, account_name: str, filesystem: str, auth):
        super().__init__(account_name, filesystem, auth)
        self._session = httpx.AsyncClient()

    async def _request(self, method: str, path: str = "", params=None, headers=None, data=None):
        # Ensure we have an up-to-date token
        token = await self.auth.get_token()
        url = f"{self.base_url}/{self.filesystem}"
        if path.startswith("/"):
            path = path[1:]
        if path:
            url = f"{url}/{path}"

        req_headers = {
            "Authorization": f"Bearer {token}",
            "x-ms-version": self.api_version
        }
        if headers:
            req_headers.update(headers)

        resp = await self._session.request(
            method, url, params=params, headers=req_headers, content=data
        )
        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError as ex:
            raise RuntimeError(f"ADLS (async) request failed [{resp.status_code}]: {resp.text}") from ex
        return resp

    async def get(self, path="", params=None, headers=None):
        return await self._request("GET", path, params=params, headers=headers)

    async def put(self, path="", params=None, headers=None, data=None):
        return await self._request("PUT", path, params=params, headers=headers, data=data)

    async def patch(self, path="", params=None, headers=None, data=None):
        return await self._request("PATCH", path, params=params, headers=headers, data=data)

    async def delete_request(self, path="", params=None, headers=None):
        return await self._request("DELETE", path, params=params, headers=headers)

    async def close(self):
        await self._session.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
