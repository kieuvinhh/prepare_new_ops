import os
from azure.storage.blob import BlobServiceClient, ContainerClient

# Replace these variables with your own
connection_string = "your_connection_string"
container_name = "your_container_name"
local_folder_path = "/opt/airflow/gold"
destination_path_in_blob = "final_test"


def upload_files_to_adls_gen2(connection_string, container_name, local_folder_path, destination_path_in_blob):
    try:
        # Create the BlobServiceClient object
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Get a container client
        container_client = blob_service_client.get_container_client(container_name)

        # Check if the container exists
        if not container_client.exists():
            # Create the container if it does not exist
            container_client.create_container()
            print(f"Container '{container_name}' created.")
        else:
            print(f"Container '{container_name}' already exists.")

        # Upload each file in the local folder
        for root, dirs, files in os.walk(local_folder_path):
            for file in files:
                local_file_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_file_path, local_folder_path)
                blob_path = os.path.join(destination_path_in_blob, relative_path)

                blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)

                print(f"Uploading {local_file_path} to Azure Storage as blob {blob_path}")

                with open(local_file_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)

        print("All files uploaded successfully.")

    except Exception as ex:
        print(f"Exception occurred: {ex}")
