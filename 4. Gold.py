from pyspark.sql.functions import *

# Access ADLS
spark.conf.set(
  "fs.azure.account.key.1adls.dfs.core.windows.net",
  "<<access_key>>"
)



gold_path = "abfss://gold@1adls.dfs.core.windows.net/"
silver_path = "abfss://silver@1adls.dfs.core.windows.net/"

# Read from silver
df_silver=(
    spark.readStream
    .format("delta")
    .load(silver_path)
)


# Aggregation
df_gold=(
    df_silver
    .withWatermark("timestamp","1 minute")
    .groupby(
        window("timestamp","1 minute"),"state"
    )
    .agg(
        sum("total_amount").alias("total_sales"),
        sum("quantity").alias("total_items")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "state",
        "total_sales",
        "total_items"
    )
)

# Write to external delta table
(
    df_gold.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation",gold_path+"checkpoint")
    .start(gold_path) 
)


