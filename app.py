import os

try:
    source_adls_credentials=os.environ['<SOURCE_ADLS_CREDENTIALS>']
except:
    pass
source_adls_credentials='<SOURCE_ADLS_CREDENTIALS>'
# account name Azure Date Lake Storage (Gen2)
source_storage_account_name='<SOURCE_STORAGE_ACCOUNT_NAME>'
# file system is the ADLS container 
source_filesystem_name='<SOURCE_FILESYSTEM_NAME>'
file_name='<FILE_NAME>'
datalake_service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format("https",source_storage_account_name),credential=<SOURCE_ADLS_CREDENTIALS>)


filesystem_client = datalake_service_client.get_file_system_client(source_filesystem_name)
try:
    filesystem_client.create_file_system()
except:
    pass

errors = []
def on_error(error):
    errors.append(error)

file_client = datalake_service_client.get_file_client(source_filesystem_name, file_name)

import pandas as pd

query_expression = "SELECT * from DataLakeStorage"
input_format = DelimitedTextDialect(delimiter=',', quotechar='"', lineterminator='\n', escapechar="", has_header=True)
output_format = DelimitedJsonDialect(delimiter='\n')
reader = file_client.query_file(query_expression, on_error=on_error, file_format=input_format, output_format=output_format)
content = reader.readall()
print(content)

import pandas as pd
historical_df = pd.read_json(content,lines=True)
historical_df

payload_df_csv=historical_df.to_csv(index=False)
print(payload_df_csv)

def write_response(file_name, requests_response, file_system, storage_folder_name, storage_account_name, adls_credentials):
    try:
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format("https", storage_account_name), credential=adls_credentials)
        file_system_client = service_client.get_file_system_client(file_system=file_system)
        directory_client = file_system_client.get_directory_client(storage_folder_name)
        file_client = directory_client.get_file_client(file_name)
        try:
            file_client.create_file()
            file_client.append_data(requests_response, offset=0, length=len(requests_response))
            file_client.flush_data(len(requests_response))
            # logging.info("the file '" + file_name + "' was read from 'avralphaessausteast' ADLS and successfully written to 'agvicenergysmartfarming' ADLS.")
            print("the file '"+file_name+"' was read from 'avralphaessausteast' ADLS and successfully written to 'agvicenergysmartfarming' ADLS.")
        except:
            # logging.info("the file " + file_name + " already exists or an error occured")
            print("the file '"+file_name+"' already exists or more likely an error occured.")
    except Exception as e:
        # logging.info(e)
        print(e)

# account name Azure Date Lake Storage (Gen2)
destination_storage_account_name='<DESTINATION_STORAGE_ACCOUNT_NAME>'
# storage_folder_name is the folder in the ADLS container 
destination_storage_folder_name='<DESTINATION_STORAGE_FOLDER_NAME>'
# file system is the ADLS container 
destination_file_system='<DESTINATION_FILE_SYSTEM>'


try:
    adls_agvicenergysmartfarming_key=os.environ['<DESTINATION_CREDENTIALS>']
except:
    pass
adls_agvicenergysmartfarming_key='<DESTINATION_ADLS_CREDENTIALS>'


write_response(file_name,payload_df_csv,destination_file_system,destination_storage_folder_name,destination_storage_account_name, adls_agvicenergysmartfarming_key)


