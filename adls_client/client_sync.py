"""
Sync base ADLS client: wraps low-level HTTP methods via requests.
"""
import requests
from .client_base import ADLSBaseClient

class ADLSBaseClientSync(ADLSBaseClient):
    def __init__(self, account_name: str, filesystem: str, auth):
        super().__init__(account_name, filesystem, auth)
        self._session = requests.Session()

    def _request(self, method: str, path: str = "", params=None, headers=None, data=None):
        # Ensure we have an up-to-date token
        token = self.auth.get_token()
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

        resp = self._session.request(
            method, url, params=params, headers=req_headers, data=data
        )
        try:
            resp.raise_for_status()
        except requests.HTTPError as ex:
            raise RuntimeError(f"ADLS (sync) request failed [{resp.status_code}]: {resp.text}") from ex
        return resp

    def get(self, path="", params=None, headers=None):
        return self._request("GET", path, params=params, headers=headers)

    def put(self, path="", params=None, headers=None, data=None):
        return self._request("PUT", path, params=params, headers=headers, data=data)

    def patch(self, path="", params=None, headers=None, data=None):
        return self._request("PATCH", path, params=params, headers=headers, data=data)

    def delete_request(self, path="", params=None, headers=None):
        return self._request("DELETE", path, params=params, headers=headers)

    def close(self):
        self._session.close()
