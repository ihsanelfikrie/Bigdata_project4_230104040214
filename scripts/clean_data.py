# =====================================
# CLEAN DATA LAYER (SILVER)
# =====================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

spark = SparkSession.builder \
    .appName("CleanData") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("========================================")
print("       CLEAN DATA LAYER STARTED         ")
print("========================================")

print("Loading raw data...")
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("data/raw/ecommerce.csv")

print(f"Raw records: {df.count()}")

# Buang data kosong
df = df.dropna()

# Filter data tidak valid
df = df.filter(col("total_amount") > 0)
df = df.filter(col("quantity") > 0)

print(f"Clean records: {df.count()}")
df.show(5)

# Simpan ke Parquet
os.makedirs("data/clean/parquet", exist_ok=True)
df.write.mode("overwrite").parquet("data/clean/parquet/")

print("========================================")
print("       CLEAN DATA SAVED SUCCESSFULLY    ")
print("========================================")

spark.stop()