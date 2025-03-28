"""
Sync service principal auth using requests.
Automatically fetches and refreshes an OAuth2 token for Azure Storage (https://storage.azure.com/.default).
"""
import time
import requests
from .auth_base import ServicePrincipalAuthBase

class ServicePrincipalAuthSync(ServicePrincipalAuthBase):
    def __init__(self, tenant_id: str, client_id: str, client_secret: str):
        super().__init__(tenant_id, client_id, client_secret)

    def fetch_token(self):
        """Fetch a new OAuth2 token from Azure AD using client credentials (sync)."""
        token_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        data = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'scope': 'https://storage.azure.com/.default'
        }
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}

        resp = requests.post(token_url, data=data, headers=headers)
        resp.raise_for_status()
        json_body = resp.json()
        self.token = json_body.get('access_token')
        expires_in = json_body.get('expires_in', 3600)
        self.token_expires_at = time.time() + float(expires_in) - 5

    def get_token(self) -> str:
        """Return a valid token, refreshing if needed."""
        if not self.token or time.time() >= self.token_expires_at:
            self.fetch_token()
        return self.token
