"""
Base (abstract) class for authentication (optional).
You can skip this if you prefer.
"""
import abc

class ServicePrincipalAuthBase(abc.ABC):
    """Abstract definition of service principal auth interface."""

    def __init__(self, tenant_id: str, client_id: str, client_secret: str):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = None
        self.token_expires_at = 0

    @abc.abstractmethod
    def get_token(self):
        """Return a valid access token. Should refresh automatically if needed."""
        pass