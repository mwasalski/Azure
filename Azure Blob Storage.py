class AzureBlobStorage:
    def __init__(self, connection_string, container_name):
        from azure.storage.blob import BlobServiceClient
        import os
        from dotenv import load_dotenv

        """
        Connect to Azure Blob Storage using credentials from a .env file.

        Environment Variables Required in .env:
        - connection_string: Azure Storage Account connection string.
        - container: Name of the container to access.

        Raises:
            ValueError: If required environment variables are missing.
        """
        # Load environment variables
        load_dotenv()

        # Get credentials from the environment variables
        connection_string = os.getenv('connection_string')
        container_name = os.getenv('container')

        # Validate required environment variables
        if not connection_string or not container_name:
            raise ValueError(
                "Missing required environment variables. Ensure 'connection_string' "
                "and 'container' are defined in the .env file."
            )

        # Initialize BlobServiceClient and ContainerClient
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self.container_client = self.blob_service_client.get_container_client(container_name)

    def list_blobs(self):
        blob_list = self.container_client.list_blobs()
        for blob in blob_list:
            print(blob.name)