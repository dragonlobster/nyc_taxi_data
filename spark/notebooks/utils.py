# Ingestion Step - Landing Zone
from urllib.request import urlretrieve 
import pyspark.sql.functions as f
import os

def ingest_landing(src, local_dest, minio_dest, minio_client):
    """
    A function that uses a GET request to download the file to minio landing zone
    """
    urlretrieve(src, local_dest) # GET request to download the file locally 
    
    # check if landing bucket exists, if not create it
    found = minio_client.bucket_exists("landing") 
    
    if not found: 
        minio_client.make_bucket("landing")  
        
    minio_client.fput_object("landing", minio_dest, local_dest) # PUT file into minio object
    os.remove(local_dest) # remove file from local
    print(f"Ingestion {src} Successful") # log success

def load_bronze(local_src, minio_src, dest_table, minio_client, spark, file_type="parquet"):
    """
    Load landing data into the bronze database
    """
    minio_client.fget_object("landing", minio_src, local_src) # get landing file object from minio
    
    # add ingest time
    if file_type == "parquet":
        df = spark.read.parquet(local_src)
    elif file_type == "csv":
        df = spark.read.option("header", "true").csv(local_src)
    df = df.withColumn("ingest_time", f.current_timestamp())
    
    # if the table doesn't exist create the table, otherwise append
    exists = spark.catalog.tableExists(dest_table)
    if not exists:
        df.write.saveAsTable(dest_table)
        spark.sql(f"ALTER TABLE {dest_table} SET TBLPROPERTIES ('write.spark.accept-any-schema'='true')")
    else:
        df.writeTo(dest_table).option("mergeSchema","true").append()
    print(f"Load {minio_src} to Bronze Success")

def sanitize_columns(df, columns_to_rename):
    """
    A function that renames columns to cleaner/descriptive column names in silver zone
    """
    for old_name, new_name in columns_to_rename.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df
    