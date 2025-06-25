# Import all required libraries
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
        # Load environment variables from .env file
        # This allows us to store sensitive connection strings securely
        load_dotenv()
        
        # Get connection details
        # Can provide data in parameters or use .env file
        # Priority: parameter value > environment variable > None
        self.connection_string = connection_string or os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        self.container_name = container_name or os.getenv('CONTAINER_NAME')
        
        # Data validation
        # If something is wrong with connection string or container name, return error
        if not self.connection_string:
            raise ValueError("Missing connection string. Check .env file or parameters")
        if not self.container_name:
            raise ValueError("Missing container name. Check .env file or parameters")
            
        # Attempt to open the door (try)
        # If something goes wrong (except):
        # Key doesn't fit
        # Door is locked
        # No permissions
        # We inform the user about the problem (raise) with exact description of what happened
        
        # This is a security mechanism that:
        # Catches all possible errors during connection
        # Informs user in a readable way what went wrong
        # Allows for appropriate error response
        try:
            # Create client (connection) to Azure Storage Account using connection string
            # This is the main access point to our Azure Storage account
            # Allows for:
            # Browsing all containers
            # Creating new containers
            # Managing permissions
            # Executing operations at the account level
            self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
            
            # Create client (connection) to specific container in Azure Storage
            # Allows for:
            # Browsing all objects (blobs) in the container
            # Creating new objects
            # Managing permissions
            # Executing operations at the specific container level
            self.container_client = self.blob_service_client.get_container_client(self.container_name)
        except Exception as e:
            # If connection fails, report error with information about what went wrong
            # str(e) shows details of the original error
            raise ConnectionError(f"Failed to connect to Azure Storage: {str(e)}")

# ======================================================================================================================================
    # Listing blobs - shows all files in our container
    
    def blob_list(self) -> List[str]:
        """
        Lists all blobs in the container
        Returns:
            list: List of blob names in the container
        """
        try:
            # Get list of all blobs (files) in the container
            # This is like asking "what files do you have in this folder?"
            blobs = self.container_client.list_blobs()
            
            # Extract just the names from blob objects
            # Convert from complex blob objects to simple list of names
            blob_names = [blob.name for blob in blobs]
            
            return blob_names
        except Exception as e:
            # If something goes wrong, inform user with clear error message
            raise RuntimeError(f"Error listing blobs: {str(e)}")

# ======================================================================================================================================
    # Creating container - makes new storage folders in Azure
    
    def create_container(self, new_container: str) -> str:
        """
        Creates a new container in Azure Blob Storage
        Args:
            new_container (str): Name of the container to create
        Returns:
            str: Message indicating result
        """
        try:
            # Get list of all existing containers in the storage account
            # This is like checking what folders already exist
            containers = self.blob_service_client.list_containers()
            existing_containers = [container.name for container in containers]
            
            # Check if the container we want to create already exists
            # Prevent creating duplicate containers
            if new_container not in existing_containers:
                # Create the new container since it doesn't exist
                # This is like creating a new folder in the cloud
                self.blob_service_client.create_container(new_container)
                return f"Container '{new_container}' created successfully"
            else:
                # Container already exists, inform user
                return f"Container '{new_container}' already exists"
        except Exception as e:
            # If creation fails, provide clear error message
            raise RuntimeError(f"Error creating container: {str(e)}")

# ======================================================================================================================================
    # Delete container - removes storage folders from Azure
    
    def delete_container(self, container_to_delete: str) -> str:
        """
        Deletes a container in Azure Blob Storage
        Args:
            container_to_delete (str): Name of the container to delete
        Returns:
            str: Message indicating result
        """
        try:
            # Delete the specified container from Azure Storage
            # WARNING: This will delete ALL files inside the container too!
            # This is like deleting an entire folder with all its contents
            self.blob_service_client.delete_container(container_to_delete)
            return f"Container '{container_to_delete}' deleted successfully"
        except Exception as e:
            # If deletion fails, provide clear error message
            raise RuntimeError(f"Error deleting container: {str(e)}")

# ======================================================================================================================================
    # List containers - shows all storage folders in Azure account
    
    def list_containers(self) -> List[str]:
        """
        Lists all containers in the storage account
        Returns:
            list: List of container names
        """
        try:
            # Get all containers from the storage account
            # This is like listing all folders in your cloud storage
            containers = self.blob_service_client.list_containers()
            
            # Extract just the names from container objects
            # Convert from complex objects to simple list of names
            container_names = [container.name for container in containers]
            
            return container_names
        except Exception as e:
            # If listing fails, provide clear error message
            raise RuntimeError(f"Error listing containers: {str(e)}")

# ======================================================================================================================================
    # Upload blob - sends files to Azure storage
    
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
            # Get a client for the specific blob we want to upload
            # This is like selecting the file location where we want to save
            blob_client = self.container_client.get_blob_client(blob_name)
            
            # Upload the data to Azure blob storage
            # overwrite parameter controls if we replace existing files
            # This is like copying a file to the cloud
            blob_client.upload_blob(data, overwrite=overwrite)
            return f"Blob '{blob_name}' uploaded successfully"
        except Exception as e:
            # If upload fails, provide clear error message
            raise RuntimeError(f"Error uploading blob: {str(e)}")

# ======================================================================================================================================
    # Download blob - gets files from Azure storage
    
    def download_blob(self, blob_name: str) -> bytes:
        """
        Downloads a blob's content
        Args:
            blob_name (str): Name of the blob to download
        Returns:
            bytes: Blob content
        """
        try:
            # Get a client for the specific blob we want to download
            # This is like selecting which file we want to retrieve
            blob_client = self.container_client.get_blob_client(blob_name)
            
            # Download the blob content as binary data
            # readall() gets the entire file content at once
            # This is like downloading a file from cloud to memory
            return blob_client.download_blob().readall()
        except Exception as e:
            # If download fails, provide clear error message
            raise RuntimeError(f"Error downloading blob: {str(e)}")

# ======================================================================================================================================
    # Delete blob - removes files from Azure storage
    
    def delete_blob(self, blob_name: str) -> str:
        """
        Deletes a blob
        Args:
            blob_name (str): Name of the blob to delete
        Returns:
            str: Message indicating result
        """
        try:
            # Get a client for the specific blob we want to delete
            # This is like selecting which file we want to remove
            blob_client = self.container_client.get_blob_client(blob_name)
            
            # Delete the blob from Azure storage
            # This permanently removes the file from the cloud
            blob_client.delete_blob()
            return f"Blob '{blob_name}' deleted successfully"
        except Exception as e:
            # If deletion fails, provide clear error message
            raise RuntimeError(f"Error deleting blob: {str(e)}")

# ======================================================================================================================================
    # Create DataFrames from parquet files - converts Azure files to pandas tables
    
    def parquet_to_df(self, blob_name: str) -> pd.DataFrame:
        """
        Downloads a parquet file from blob storage and converts it to a pandas DataFrame
        Args:
            blob_name (str): Name of the parquet blob
        Returns:
            pd.DataFrame: DataFrame containing the parquet data
        """
        try:
            # Download the parquet file from Azure as binary data
            # Think of it as downloading a file from cloud to memory
            blob_data = self.download_blob(blob_name)
            
            # Convert binary data to pandas DataFrame
            # io.BytesIO creates a file-like object from bytes in memory
            # pd.read_parquet reads the parquet format and creates DataFrame
            return pd.read_parquet(io.BytesIO(blob_data))
        except Exception as e:
            # If conversion fails, provide clear error message
            raise RuntimeError(f"Error converting parquet to DataFrame: {str(e)}")

# ======================================================================================================================================
    # Create Excel files from DataFrames - converts pandas tables to Excel format and uploads to Azure
    
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
            # Create a memory buffer to store Excel file
            # Think of it as creating a temporary file in memory
            buffer = io.BytesIO()
            
            # Convert DataFrame to Excel format and write to buffer
            # index=False means don't include row numbers in Excel
            dataframe.to_excel(buffer, index=False)
            
            # Reset buffer position to beginning
            # This is like rewinding a tape to the start
            buffer.seek(0)
            
            # Upload the Excel file to Azure blob storage
            # overwrite=True means replace file if it already exists
            return self.upload_blob(blob_name, buffer.getvalue(), overwrite=True)
        except Exception as e:
            # If conversion or upload fails, provide clear error message
            raise RuntimeError(f"Error converting DataFrame to Excel: {str(e)}")

# ======================================================================================================================================
    # Create Parquet files from DataFrames - converts pandas tables to efficient Parquet format
    
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
            # Create a memory buffer to store Parquet file
            # Parquet is a compressed, efficient format for data storage
            buffer = io.BytesIO()
            
            # Convert DataFrame to Parquet format and write to buffer
            # index=False means don't include row numbers in Parquet
            # Parquet files are smaller and faster to read than Excel
            dataframe.to_parquet(buffer, index=False)
            
            # Reset buffer position to beginning
            # Prepare the data for upload
            buffer.seek(0)
            
            # Upload the Parquet file to Azure blob storage
            # overwrite=True means replace file if it already exists
            return self.upload_blob(blob_name, buffer.getvalue(), overwrite=True)
        except Exception as e:
            # If conversion or upload fails, provide clear error message
            raise RuntimeError(f"Error converting DataFrame to Parquet: {str(e)}")

# ======================================================================================================================================
    # Create DataFrames from CSV files - converts CSV files from Azure to pandas tables
    
    def csv_to_df(self, blob_name: str) -> pd.DataFrame:
        """
        Downloads a CSV file from blob storage and converts it to a pandas DataFrame
        Args:
            blob_name (str): Name of the CSV blob
        Returns:
            pd.DataFrame: DataFrame containing the CSV data
        """
        try:
            # Download the CSV file from Azure as binary data
            # CSV is a common text format for data storage
            blob_data = self.download_blob(blob_name)
            
            # Convert binary data to pandas DataFrame
            # io.BytesIO creates a file-like object from bytes in memory
            # pd.read_csv reads comma-separated values and creates DataFrame
            return pd.read_csv(io.BytesIO(blob_data))
        except Exception as e:
            # If conversion fails, provide clear error message
            raise RuntimeError(f"Error converting CSV to DataFrame: {str(e)}")

# ======================================================================================================================================
    # Create CSV files from DataFrames - converts pandas tables to CSV format and uploads to Azure
    
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
            # Convert DataFrame to CSV format as string
            # index=False means don't include row numbers in CSV
            # CSV is human-readable text format with comma-separated values
            csv_data = dataframe.to_csv(index=False)
            
            # Upload the CSV data to Azure blob storage
            # overwrite=True means replace file if it already exists
            return self.upload_blob(blob_name, csv_data, overwrite=True)
        except Exception as e:
            # If conversion or upload fails, provide clear error message
            raise RuntimeError(f"Error converting DataFrame to CSV: {str(e)}")

# ======================================================================================================================================

# ======================================================================================================================================

# ======================================================================================================================================