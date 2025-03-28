"""
Example of using the ASYNC ADLS client.
"""
import asyncio
from adls_client import create_adls_client

async def main():
    tenant_id = "YOUR_TENANT_ID"
    client_id = "YOUR_CLIENT_ID"
    client_secret = "YOUR_CLIENT_SECRET"
    account_name = "yourstorageaccount"
    filesystem = "myfilesystem"

    # We want an async client, so use_async=True
    client = create_adls_client(
        account_name,
        filesystem,
        tenant_id,
        client_id,
        client_secret,
        use_async=True
    )

    # It's recommended to use the async client as a context manager
    async with client:
        await client.create_folder("my-async-folder")
        print("Created folder 'my-async-folder'.")

        await client.create_file("my-async-folder/example.txt")
        print("Created file 'example.txt' in 'my-async-folder'.")

        items = await client.list_directory("my-async-folder")
        print("List of items in 'my-async-folder':", items)

        # Clean up
        await client.delete("my-async-folder", recursive=True)
        print("Deleted 'my-async-folder' and its contents.")

# Run the async example
asyncio.run(main())
