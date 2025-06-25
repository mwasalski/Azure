from azure.storage.blob import BlobServiceClient
import os
import pandas as pd
import io
from dotenv import load_dotenv
from typing import Optional, List, Union


class AzureBlobStorage:
    def __init__(self, connection_string: Optional[str] = None, container_name: Optional[str] = None):
        """
        Initializes connection to Azure Blob Storage
        Args:
            connection_string (str, optional): Azure storage connection string, recommended to use .env file
            container_name (str, optional): Container name, recommended to use .env file
        """
        load_dotenv()
        
        self.connection_string = connection_string or os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        self.container_name = container_name or os.getenv('CONTAINER_NAME')
        
        if not self.connection_string:
            raise ValueError("Missing connection string. Check .env file or parameters")
        if not self.container_name:
            raise ValueError("Missing container name. Check .env file or parameters")
            
        try:
            self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
            self.container_client = self.blob_service_client.get_container_client(self.container_name)
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Azure Storage: {str(e)}")

    def blob_list(self) -> List[str]:
        """
        Lists all blobs in the container
        Returns:
            list: List of blob names in the container
        """
        try:
            blobs = self.container_client.list_blobs()
            blob_names = [blob.name for blob in blobs]
            return blob_names
        except Exception as e:
            raise RuntimeError(f"Error listing blobs: {str(e)}")

    def create_container(self, new_container: str) -> str:
        """
        Creates a new container in Azure Blob Storage
        Args:
            new_container (str): Name of the container to create
        Returns:
            str: Message indicating result
        """
        try:
            containers = self.blob_service_client.list_containers()
            existing_containers = [container.name for container in containers]
            
            if new_container not in existing_containers:
                self.blob_service_client.create_container(new_container)
                return f"Container '{new_container}' created successfully"
            else:
                return f"Container '{new_container}' already exists"
        except Exception as e:
            raise RuntimeError(f"Error creating container: {str(e)}")

    def delete_container(self, container_to_delete: str) -> str:
        """
        Deletes a container in Azure Blob Storage
        Args:
            container_to_delete (str): Name of the container to delete
        Returns:
            str: Message indicating result
        """
        try:
            self.blob_service_client.delete_container(container_to_delete)
            return f"Container '{container_to_delete}' deleted successfully"
        except Exception as e:
            raise RuntimeError(f"Error deleting container: {str(e)}")

    def list_containers(self) -> List[str]:
        """
        Lists all containers in the storage account
        Returns:
            list: List of container names
        """
        try:
            containers = self.blob_service_client.list_containers()
            container_names = [container.name for container in containers]
            return container_names
        except Exception as e:
            raise RuntimeError(f"Error listing containers: {str(e)}")

    def upload_blob(self, blob_name: str, data: Union[str, bytes, io.IOBase], overwrite: bool = False) -> str:
        """
        Uploads data to a blob
        Args:
            blob_name (str): Name of the blob
            data: Data to upload (string, bytes, or file-like object)
            overwrite (bool): Whether to overwrite existing blob
        Returns:
            str: Message indicating result
        """
        try:
            blob_client = self.container_client.get_blob_client(blob_name)
            blob_client.upload_blob(data, overwrite=overwrite)
            return f"Blob '{blob_name}' uploaded successfully"
        except Exception as e:
            raise RuntimeError(f"Error uploading blob: {str(e)}")

    def download_blob(self, blob_name: str) -> bytes:
        """
        Downloads a blob's content
        Args:
            blob_name (str): Name of the blob to download
        Returns:
            bytes: Blob content
        """
        try:
            blob_client = self.container_client.get_blob_client(blob_name)
            return blob_client.download_blob().readall()
        except Exception as e:
            raise RuntimeError(f"Error downloading blob: {str(e)}")

    def delete_blob(self, blob_name: str) -> str:
        """
        Deletes a blob
        Args:
            blob_name (str): Name of the blob to delete
        Returns:
            str: Message indicating result
        """
        try:
            blob_client = self.container_client.get_blob_client(blob_name)
            blob_client.delete_blob()
            return f"Blob '{blob_name}' deleted successfully"
        except Exception as e:
            raise RuntimeError(f"Error deleting blob: {str(e)}")

    def parquet_to_df(self, blob_name: str) -> pd.DataFrame:
        """
        Downloads a parquet file from blob storage and converts it to a pandas DataFrame
        Args:
            blob_name (str): Name of the parquet blob
        Returns:
            pd.DataFrame: DataFrame containing the parquet data
        """
        try:
            blob_data = self.download_blob(blob_name)
            return pd.read_parquet(io.BytesIO(blob_data))
        except Exception as e:
            raise RuntimeError(f"Error converting parquet to DataFrame: {str(e)}")

    def df_to_excel(self, dataframe: pd.DataFrame, blob_name: str) -> str:
        """
        Converts a DataFrame to Excel format and uploads it as a blob
        Args:
            dataframe (pd.DataFrame): DataFrame to convert
            blob_name (str): Name for the Excel blob (should end with .xlsx)
        Returns:
            str: Message indicating result
        """
        try:
            buffer = io.BytesIO()
            dataframe.to_excel(buffer, index=False)
            buffer.seek(0)
            
            return self.upload_blob(blob_name, buffer.getvalue(), overwrite=True)
        except Exception as e:
            raise RuntimeError(f"Error converting DataFrame to Excel: {str(e)}")

    def df_to_parquet(self, dataframe: pd.DataFrame, blob_name: str) -> str:
        """
        Converts a DataFrame to Parquet format and uploads it as a blob
        Args:
            dataframe (pd.DataFrame): DataFrame to convert
            blob_name (str): Name for the Parquet blob (should end with .parquet)
        Returns:
            str: Message indicating result
        """
        try:
            buffer = io.BytesIO()
            dataframe.to_parquet(buffer, index=False)
            buffer.seek(0)
            
            return self.upload_blob(blob_name, buffer.getvalue(), overwrite=True)
        except Exception as e:
            raise RuntimeError(f"Error converting DataFrame to Parquet: {str(e)}")

    def csv_to_df(self, blob_name: str) -> pd.DataFrame:
        """
        Downloads a CSV file from blob storage and converts it to a pandas DataFrame
        Args:
            blob_name (str): Name of the CSV blob
        Returns:
            pd.DataFrame: DataFrame containing the CSV data
        """
        try:
            blob_data = self.download_blob(blob_name)
            return pd.read_csv(io.BytesIO(blob_data))
        except Exception as e:
            raise RuntimeError(f"Error converting CSV to DataFrame: {str(e)}")

    def df_to_csv(self, dataframe: pd.DataFrame, blob_name: str) -> str:
        """
        Converts a DataFrame to CSV format and uploads it as a blob
        Args:
            dataframe (pd.DataFrame): DataFrame to convert
            blob_name (str): Name for the CSV blob (should end with .csv)
        Returns:
            str: Message indicating result
        """
        try:
            csv_data = dataframe.to_csv(index=False)
            return self.upload_blob(blob_name, csv_data, overwrite=True)
        except Exception as e:
            raise RuntimeError(f"Error converting DataFrame to CSV: {str(e)}")