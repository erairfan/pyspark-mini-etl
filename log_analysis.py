from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LogAnalysis").getOrCreate()

logs = [
    ("INFO",),
    ("ERROR",),
    ("INFO",),
    ("ERROR",),
    ("WARN",)
]

df = spark.createDataFrame(logs, ["level"])

df.groupBy("level").count().show()

spark.stop()
