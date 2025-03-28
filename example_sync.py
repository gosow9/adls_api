"""
Example of using the SYNC ADLS client.
"""
from adls_client import create_adls_client

def main():
    tenant_id = "YOUR_TENANT_ID"
    client_id = "YOUR_CLIENT_ID"
    client_secret = "YOUR_CLIENT_SECRET"
    account_name = "yourstorageaccount"
    filesystem = "myfilesystem"

    # We want a sync client, so use_async=False
    client = create_adls_client(
        account_name,
        filesystem,
        tenant_id,
        client_id,
        client_secret,
        use_async=False
    )

    client.create_folder("my-sync-folder")
    print("Created folder 'my-sync-folder'.")

    client.create_file("my-sync-folder/example.txt")
    print("Created file 'example.txt' in 'my-sync-folder'.")

    items = client.list_directory("my-sync-folder")
    print("List of items in 'my-sync-folder':", items)

    # Clean up
    client.delete("my-sync-folder", recursive=True)
    print("Deleted 'my-sync-folder' and its contents.")

    # Close the session
    client.close()

if __name__ == "__main__":
    main()
