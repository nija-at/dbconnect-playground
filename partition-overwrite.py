# https://subhamkharwal.medium.com/pyspark-dynamic-partition-overwrite-a6e987192df9

from databricks.connect import DatabricksSession
from random import randint
from pyspark.sql.functions import to_date
from pyspark.dbutils import DBUtils

path = "dbfs:/tmp/dbc_qa_file{}".format(randint(0, 1000))

spark = DatabricksSession.Builder().getOrCreate()
dbutils = DBUtils(spark)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
print(spark.conf.get("spark.sql.sources.partitionOverwriteMode"))

print ("creating original data file")

# spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")

_data = [
    ["ORD1001", "P003", 70, "01-21-2022"],
    ["ORD1004", "P033", 12, "01-24-2022"],
    ["ORD1005", "P036", 10, "01-20-2022"],
    ["ORD1002", "P016", 2, "01-10-2022"],
    ["ORD1003", "P012", 6, "01-10-2022"],
]
_cols = ["order_id", "prod_id", "qty", "order_date"]
df = spark.createDataFrame(data=_data, schema=_cols)
df = df.withColumn("order_date", to_date("order_date" ,"MM-dd-yyyy"))

df.repartition("order_date") \
    .write \
    .format("parquet") \
    .partitionBy("order_date") \
    .mode("overwrite") \
    .save(path)

spark.read.parquet(path).show()

print("\n\ncreating delta data\n\n")

_data = [
    ["ORD1010", "P053", 78, "01-24-2022"],
    ["ORD1011", "P076", 21, "01-20-2022"],
]
_cols = ["order_id", "prod_id", "qty", "order_date"]
delta_df = spark.createDataFrame(data=_data, schema=_cols)
delta_df = delta_df.withColumn("order_date", to_date("order_date" ,"MM-dd-yyyy"))

delta_df.repartition("order_date") \
    .write \
    .format("parquet") \
    .partitionBy("order_date") \
    .mode("overwrite") \
    .save(path)

spark.read.parquet(path).show()
