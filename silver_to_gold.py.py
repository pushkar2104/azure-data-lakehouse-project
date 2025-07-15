from pyspark.sql.functions import year, lit, round, sum, avg, count, rank
from pyspark.sql.window import Window

# Gold Layer Transformations
gold_df = silver_df \
    .withColumn("ActualValue", col("Quantity") * col("Price")) \
    .withColumn("OrderYear", year(col("OrderDate"))) \
    .withColumn("DeliveryFlag", when(col("DeliveryStatus") == "Delivered", "Yes").otherwise("No")) \
    .withColumn("PaymentType", when(col("PaymentMethod").isin("COD", "UPI", "upi"), "Digital").otherwise("Card"))

# Customer spend ranking within each region
windowSpec = Window.partitionBy("Region").orderBy(col("ActualValue").desc())
ranked_df = gold_df.withColumn("CustomerRank", rank().over(windowSpec))

# Aggregated insights
summary_df = gold_df.groupBy("Region", "Category") \
    .agg(
        round(sum("ActualValue"), 2).alias("TotalRevenue"),
        count("*").alias("NumOrders"),
        round(avg("Quantity"), 2).alias("AvgItemsPerOrder")
    )

# Save to Gold Layer
gold_df.write.mode("overwrite").format("parquet").save("/mnt/gold/ecomm_curated")

