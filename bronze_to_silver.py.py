from pyspark.sql.functions import *

# Load raw data
raw_df = spark.read.option("header", True).csv("/mnt/bronze/ecommerce_sales_raw.csv")

# Silver Layer Transformations
silver_df = raw_df \
    .withColumn("OrderDate", 
        when(col("OrderDate").rlike(r"\d{2}/\d{2}/\d{4}"), to_date(col("OrderDate"), "dd/MM/yyyy"))
        .when(col("OrderDate").rlike(r"\d{4}\.\d{2}\.\d{2}"), to_date(col("OrderDate"), "yyyy.MM.dd"))
        .when(col("OrderDate").rlike(r"\d{2}-\d{2}-\d{4}"), to_date(col("OrderDate"), "dd-MM-yyyy"))
        .otherwise(to_date(col("OrderDate"), "yyyy-MM-dd"))
    ) \
    .withColumn("CustomerID", regexp_replace(col("CustomerID"), "[^A-Z0-9]", "")) \
    .withColumn("ProductID", upper(regexp_replace(col("ProductID"), "[^A-Z0-9]", ""))) \
    .withColumn("Category", when(col("Category").rlike("(?i)elctronics|electrioncs"), "Electronics")
                          .otherwise(initcap(col("Category")))) \
    .withColumn("Region", upper(regexp_replace(col("Region"), "[^A-Z]", ""))) \
    .withColumn("PaymentMethod", regexp_replace(upper(col("PaymentMethod")), "CASH ONDELVRY", "COD")) \
    .withColumn("DeliveryStatus", initcap(col("DeliveryStatus"))) \
    .withColumn("Quantity", col("Quantity").cast("int")) \
    .withColumn("Price", col("Price").cast("double")) \
    .withColumn("TotalValue", col("TotalValue").cast("double")) \
    .na.fill({"Quantity": 0, "Price": 0}) \
    .dropna(subset=["OrderID", "CustomerID", "ProductID", "Price"])

# Save to Silver Layer
silver_df.write.mode("overwrite").format("parquet").save("/mnt/silver/ecomm_cleaned")
