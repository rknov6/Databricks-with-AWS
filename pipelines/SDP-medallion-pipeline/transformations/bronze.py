from pyspark import pipelines as dp
from pyspark.sql.functions import col,current_timestamp,lit

schema_location = "/Volumes/dev/dbx_course/landing/_schema"
checkpoint_location = "/Volumes/dev/dbx_course/landing/_checkpoint"
data_location = "/Volumes/dev/dbx_course/landing"

@dp.table(name = 'dev.bronze.orders',
          comment = "this is bronze orders table")
def bronze_orders():
    return(
        spark.readStream.format("cloudFiles")
             .option("cloudFiles.format","csv")
             .option("cloudFiles.schemalocation",f"{schema_location}/orders")
             .option("cloudFiles.schemaEvolutionMode","addNewColumns")
             .load(f"{data_location}/orders/")
             .withColumn("file_name",col("_metadata.file_name"))
             .withColumn("ingest_timestamp",current_timestamp())
             .withColumn("_source",lit("sdp_load"))
)
    
@dp.table(name = 'dev.bronze.customers',
          comment = "this is bronze customers table")
def bronze_customers():
    return(
        spark.readStream.format("cloudFiles")
             .option("cloudFiles.format","csv")
             .option("cloudFiles.schemalocation",f"{schema_location}/customers")
             .option("cloudFiles.schemaEvolutionMode","addNewColumns")
             .load(f"{data_location}/customers/")
             .withColumn("file_name",col("_metadata.file_name"))
             .withColumn("ingest_timestamp",current_timestamp())
             .withColumn("_source",lit("sdp_load"))
)