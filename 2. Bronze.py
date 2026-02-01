from pyspark.sql.types import *
from pyspark.sql.functions import *

# Schema
schema = StructType([
    StructField('order_id',StringType()),
    StructField('timestamp',StringType()),
    StructField('customer_id',StringType()),
    StructField('product_id',StringType()),
    StructField('category',StringType()),
    StructField('price',DoubleType()),
    StructField('quantity',IntegerType()),
    StructField('total_amount',DoubleType()),
    StructField('city',StringType()),
    StructField('state',StringType()),
    StructField('country',StringType()),
    StructField('latitude',StringType()),
    StructField('longitude',StringType()),
    StructField('delivery_status',StringType()),
])

# Azure Event Hub Configuration
event_hub_namespace = "<<namespace>>"
event_hub_name = "<<eventhub_name>>"
event_hub_conn_str = "<<Conn. String>>"

config = {
    'kafka.bootstrap.servers': f"{event_hub_namespace}:9093",
    'subscribe': event_hub_name,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{event_hub_conn_str}";',
    'startingOffsets': 'latest',
    'failOnDataLoss': 'false'
}

# Read Streaming data from Event Hub
df_raw = (
    spark.readStream
    .format('kafka')
    .options(**config)
    .load()
)

# Parse JSON from kafka stream
df_bronze = (
    df_raw.selectExpr("Cast(value AS STRING) as json")
    .select(from_json("json",schema).alias("data"))
    .select("data.*")
)

# Write data to bronze layer

spark.conf.set(
  "fs.azure.account.key.1adls.dfs.core.windows.net",
  "<<access_key>>"
)


bronze_path = "abfss://bronze@1adls.dfs.core.windows.net"

(
df_bronze.writeStream
    .format('delta')
    .outputMode("append")
    .option("checkpointLocation",bronze_path+"/checkpoint")
    .start(bronze_path)
)
