from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = SparkSession.builder.appName("MiniETL").getOrCreate()

data = [
    ("Alice", 100),
    ("Bob", 200),
    ("Alice", 300)
]

df = spark.createDataFrame(data, ["name", "salary"])

result = df.groupBy("name").agg(avg("salary").alias("avg_salary"))

result.show()

spark.stop()
