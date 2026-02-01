# Imports
from pyspark.sql.functions import *

# Access ADLS

spark.conf.set(
  "fs.azure.account.key.1adls.dfs.core.windows.net",
  "<<access_key>>"
)


bronze_path = "abfss://bronze@1adls.dfs.core.windows.net/"
silver_path = "abfss://silver@1adls.dfs.core.windows.net/"

# Reading raw data from bronze
df_bronze=(
  spark.readStream
  .format("delta")
  .load(bronze_path)
)

df_silver=(
  df_bronze
  .withColumn("timestamp",to_timestamp("timestamp"))
  .withColumn("price",when(col("price").isNull(),0.0).otherwise(col("price")))
  .withColumn("quantity",when(col("quantity").isNull(),1).otherwise(col("quantity")))
  .withColumn("total_amount",col("price")*col("quantity"))
  .withWatermark("timestamp","1 minute")
  .dropDuplicates(["order_id"])
  .filter(col("country")=="India")
  .filter(col("state").isNotNull())
)

# Write to Silver Layer
(
  df_silver.writeStream
 .format("delta")
 .outputMode("append")
 .option("checkpointLocation",silver_path+"checkpoint")
 .start(silver_path)
 )




