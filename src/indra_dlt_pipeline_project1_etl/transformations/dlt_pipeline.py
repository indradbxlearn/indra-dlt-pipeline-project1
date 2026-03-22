import dlt
from pyspark.sql.functions import *

# Bronze table (raw data)
@dlt.table(
    name="bronze_table"
)
def bronze_table():
    data = [
        (1, "Alice", 100),
        (2, "Bob", 200),
        (3, "Charlie", 300)
    ]
    columns = ["id", "name", "amount"]
    return spark.createDataFrame(data, columns)


# Silver table (transformation)
@dlt.table(
    name="silver_table"
)
def silver_table():
    df = dlt.read("bronze_table")
    return df.withColumn("amount_with_tax", col("amount") * 1.1)


# Gold table (aggregated)
@dlt.table(
    name="gold_table"
)
def gold_table():
    df = dlt.read("silver_table")
    return df.groupBy("name").agg(sum("amount_with_tax").alias("total_amount"))