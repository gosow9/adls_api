"""
An optional base/abstract for ADLS client to share logic across sync/async.
You can skip this if you prefer more direct classes.
"""
import abc

class ADLSBaseClient(abc.ABC):
    def __init__(self, account_name: str, filesystem: str, auth):
        self.account_name = account_name
        self.filesystem = filesystem
        self.auth = auth
        self.api_version = "2023-08-03"
        self.base_url = f"https://{account_name}.dfs.core.windows.net"

    @abc.abstractmethod
    def close(self):
        """Close underlying sessions/resources if needed."""
        pass
