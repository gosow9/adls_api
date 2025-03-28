"""
ADLS Client package initialization.
Exposes a convenience function to create either async or sync clients.
"""
from .auth_async import ServicePrincipalAuthAsync
from .auth_sync import ServicePrincipalAuthSync
from .adls_async import ADLSClientAsync
from .adls_sync import ADLSClientSync

def create_adls_client(
    account_name: str,
    filesystem: str,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    use_async: bool = True
):
    """
    Create either an async or sync ADLS client based on 'use_async'.

    Args:
        account_name (str): The storage account name, e.g. 'mystorageaccount'
        filesystem (str): The container/filesystem name.
        tenant_id (str): Azure tenant ID for service principal authentication.
        client_id (str): Azure client ID for service principal authentication.
        client_secret (str): Azure client secret for service principal authentication.
        use_async (bool): If True, returns an async client (ADLSClientAsync).
                          If False, returns a sync client (ADLSClientSync).

    Returns:
        ADLSClientAsync or ADLSClientSync
    """
    if use_async:
        auth = ServicePrincipalAuthAsync(tenant_id, client_id, client_secret)
        return ADLSClientAsync(account_name, filesystem, auth)
    else:
        auth = ServicePrincipalAuthSync(tenant_id, client_id, client_secret)
        return ADLSClientSync(account_name, filesystem, auth)