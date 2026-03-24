# =====================================
# ANALYTICS + SERVING LAYER
# =====================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, avg, desc
import os
import time

start_time = time.time()

print("========================================")
print("       ANALYTICS LAYER STARTED          ")
print("========================================")

spark = SparkSession.builder \
    .appName("AnalyticsLayer") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Buat folder serving
os.makedirs("data/serving", exist_ok=True)

# Load clean data
print("Loading Clean Parquet Data...")
df_clean = spark.read.parquet("data/clean/parquet/")
total_records = df_clean.count()
print(f"Total Records: {total_records}")
print("----------------------------------------")

# KPI 1: TOTAL REVENUE
print("Calculating Total Revenue...")
total_revenue = df_clean.agg(
    _sum("total_amount").alias("total_revenue")
)
total_revenue.show()
total_revenue.write.mode("overwrite") \
    .option("header", True) \
    .csv("data/serving/total_revenue")
print("Total Revenue saved!")
print("----------------------------------------")

# KPI 2: TOP 10 PRODUCTS
print("Calculating Top 10 Products...")
top_products = df_clean.groupBy("product") \
    .agg(_sum("quantity").alias("total_quantity")) \
    .orderBy(desc("total_quantity")) \
    .limit(10)
top_products.show()
top_products.write.mode("overwrite") \
    .option("header", True) \
    .csv("data/serving/top_products")
print("Top Products saved!")
print("----------------------------------------")

# KPI 3: REVENUE PER CATEGORY
print("Calculating Revenue per Category...")
category_revenue = df_clean.groupBy("category") \
    .agg(_sum("total_amount").alias("category_revenue")) \
    .orderBy(desc("category_revenue"))
category_revenue.show()
category_revenue.write.mode("overwrite") \
    .option("header", True) \
    .csv("data/serving/category_revenue")
print("Category Revenue saved!")
print("----------------------------------------")

# KPI 4: AVERAGE TRANSACTION VALUE
print("Calculating Average Transaction Value...")
avg_transaction = df_clean.groupBy("customer_id") \
    .agg(avg("total_amount").alias("avg_transaction_value"))
avg_transaction.show(5)
avg_transaction.write.mode("overwrite") \
    .option("header", True) \
    .csv("data/serving/avg_transaction")
print("Average Transaction saved!")
print("----------------------------------------")

spark.stop()

end_time = time.time()
execution_time = round(end_time - start_time, 2)

print("========================================")
print("   ANALYTICS LAYER COMPLETED SUCCESS    ")
print(f"   Execution Time: {execution_time} sec")
print("========================================")