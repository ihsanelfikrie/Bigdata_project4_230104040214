# =====================================
# VISUALIZATION LAYER
# =====================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, desc
import pandas as pd
import matplotlib.pyplot as plt
import os

os.makedirs("reports", exist_ok=True)

spark = SparkSession.builder \
    .appName("VisualizationLayer") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("========================================")
print("     VISUALIZATION LAYER STARTED        ")
print("========================================")

df = spark.read.parquet("data/clean/parquet/")

# Chart 1: Revenue per Category
print("Membuat chart Revenue per Category...")
category_df = df.groupBy("category") \
    .agg(_sum("total_amount").alias("total_revenue")) \
    .orderBy(desc("total_revenue")) \
    .toPandas()

plt.figure(figsize=(8, 5))
plt.bar(category_df["category"],
        category_df["total_revenue"],
        color=["#2196F3", "#4CAF50", "#FF9800"])
plt.title("Revenue per Category", fontsize=14)
plt.ylabel("Total Revenue (Rp)")
plt.xlabel("Category")
plt.tight_layout()
plt.savefig("reports/category_revenue.png")
plt.close()
print("Saved: reports/category_revenue.png")

# Chart 2: Top Products
print("Membuat chart Top Products...")
top_df = df.groupBy("product") \
    .agg(_sum("quantity").alias("total_quantity")) \
    .orderBy(desc("total_quantity")) \
    .limit(10) \
    .toPandas()

plt.figure(figsize=(10, 6))
plt.barh(top_df["product"],
         top_df["total_quantity"],
         color="#2196F3")
plt.title("Top 10 Products by Quantity", fontsize=14)
plt.xlabel("Total Quantity")
plt.tight_layout()
plt.savefig("reports/top_products.png")
plt.close()
print("Saved: reports/top_products.png")

spark.stop()

print("========================================")
print("   VISUALIZATION COMPLETED SUCCESS      ")
print("========================================")