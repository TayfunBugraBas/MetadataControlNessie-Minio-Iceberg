import requests
from minio import Minio
from minio.error import S3Error

NESSIE_API = "http://localhost:19120/api/v2/trees/main/entries?content=true&filter=entry.contentType=='ICEBERG_TABLE'"

all_tables = []

client = Minio("localhost:9000",
        access_key="admin",
        secret_key="password",
        secure=False
    )

response = requests.get(NESSIE_API)
response.raise_for_status()
data = response.json()
bucket_name = "warehouse"
warehouseArr = []

try:
    objects = client.list_objects(bucket_name, recursive=True)

    latest_metadata_file = None
    latest_last_modified = None
    
    for obj in objects:

            metadata = client.stat_object(bucket_name, obj.object_name)
            last_modified = metadata.last_modified
            print(str(last_modified) + " " + obj.object_name)

            
            if latest_last_modified is None or last_modified > latest_last_modified and obj.object_name.endswith(".metadata.json") :
                latest_last_modified = last_modified
                latest_metadata_file = obj.object_name

    warehouseArr.append(latest_metadata_file)

except S3Error as e:
    print(f"Error: {e}")

for metastore in data["entries"]:
    entry = metastore["content"]
    print(entry["metadataLocation"])
    location = entry["metadataLocation"]
    #found = client.stat_object("warehouse", location.replace("s3a://warehouse",""))
    #if not found:
    #    print("bulunamadi")
    #else:
    #    print("bu dosya mevcut")
   
    if  location.replace("s3a://warehouse/","") in warehouseArr:
        print(location + " bulduuu")
    else:
        print(location + "---bulamadii---" )


