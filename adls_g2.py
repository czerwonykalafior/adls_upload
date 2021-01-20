import os

from azure.storage.filedatalake import DataLakeServiceClient


def upload_to_adls(directory, filename):
    service_client = DataLakeServiceClient.from_connection_string(os.environ['ADLS_CONNECTION_STRING'])
    file_system_client = service_client.get_file_system_client(file_system=os.environ['ADLS_FILE_SYSTEM_NAME'])
    directory_client = file_system_client.get_directory_client(directory)
    file_client = directory_client.create_file(filename)

    with open(filename, 'rb') as local_file:
        offset, current_size = 0, 0

        for file_chunk in iter(lambda: local_file.read(4096), b""):
            chunk_size = len(file_chunk)
            current_size += chunk_size
            file_client.append_data(file_chunk, offset=offset, length=chunk_size, validate_content=True)
            file_client.flush_data(current_size)
            offset += len(file_chunk)


upload_to_adls("my-directory", "data.csv")
