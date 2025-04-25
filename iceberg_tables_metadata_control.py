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

from minio import Minio

client = Minio(
    "localhost:9000",
    access_key="admin",
    secret_key="password",
    secure=False
)

bucket_name = "warehouse"
warehouseArr = []

try:
    objects = client.list_objects(bucket_name, recursive=True)

    latest_metadata_per_table = {}

    for obj in objects:
        if not obj.object_name.endswith(".metadata.json"):
            continue

        metadata = client.stat_object(bucket_name, obj.object_name)
        last_modified = metadata.last_modified

       
        base_name = obj.object_name.replace(".metadata.json", "")
        table_key = base_name.split("/")[1]  

        
        if table_key not in latest_metadata_per_table or last_modified > latest_metadata_per_table[table_key]["last_modified"]:
            latest_metadata_per_table[table_key] = {
                "object_name": obj.object_name,
                "last_modified": last_modified
            }

    warehouseArr = [entry["object_name"] for entry in latest_metadata_per_table.values()]

    print("Bulunan en güncel metadata dosyaları:")
    for file in warehouseArr:
        print(file)

except Exception as e:
    print("Hata:", e)

for metastore in data["entries"]:
    entry = metastore["content"]
   
    location = entry["metadataLocation"]
  
   
    if  location.replace(f"s3a://{bucket_name}/","") in warehouseArr:
        print(location + " bulundu dosyalar silinmedi")
        warehouseArr.remove(location.replace("s3a://warehouse/",""))

#for item in warehouseArr:
#    i = item.split('/')
#    base_dir.append(i[0]+ '/' + i[1])

print(warehouseArr)

for item in warehouseArr:
    i = item.split('/')
    x = client.list_objects(bucket_name, i[0]+ '/' + i[1], recursive=True)
    for item2 in x:
        #delete here
        print("deleting  "+ item2.object_name)
        client.remove_object(bucket_name, item2.object_name)
        pass


#print(base_dir)

#for_delete = []
#for item in base_dir:
#    x = client.list_objects(bucket_name, item, recursive=True)
#    for item2 in x:
#        for_delete.append(item2.object_name)

#print(for_delete)
