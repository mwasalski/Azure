class AzureBlobStorage:
    def __init__(self, connection_string=None, container_name=None): # both are optional, if empty then it will use data the .env file
        # libraries for connecting to Azure Blob Storage and loading environment variables
        """_summary_

        Args:
            connection_string (_type_, optional): _description_. Defaults to None.
            container_name (_type_, optional): _description_. Defaults to None.

        Raises:
            ValueError: _description_
            
        Methods:
            list_blobs(self, prefix=None): _summary_
        """
        from azure.storage.blob import BlobServiceClient as bsc
        import os
        from dotenv import load_dotenv
        
        # now you dont have to import these libraries in the methods
        self.bsc = bsc
        self.os = os
        self.load_dotenv = load_dotenv
        
        # Load environment variables
        self.load_dotenv()

        # you will connect to Storage Account that is in your .env file or you paste the connection string in the parameter
        self.connection_string = connection_string or os.getenv('connection_string')
        self.container_name = container_name or os.getenv('container')

        # this line checks if there is anything in the parameter or in the .env file
        # if not then it will raise an error
        if not self.connection_string or not self.container_name:
            raise ValueError(
                "Missing required credentials. Provide either parameters or "
                "ensure 'connection_string' and 'container' are defined in the .env file."
            )

        # initializing connection to the storage account and the container
        self.blob_service_client = bsc.from_connection_string(self.connection_string)
        self.container_client = self.blob_service_client.get_container_client(self.container_name)

    def list_blobs(self, prefix=None, suffix=None):
        """List all blobs in the container, optionally filtered by prefix.

        Args:
            prefix (str, optional): Filter results to blobs with this prefix.

        Returns:
            list: List of blob names.
        """
        self.prefix = prefix or self.os.getenv('prefix')
        self.suffix = suffix or self.os.getenv('suffix')
        
        blob_list = self.container_client.list_blobs(name_starts_with=prefix, name_ends_with=suffix)
        return [blob.name for blob in blob_list]