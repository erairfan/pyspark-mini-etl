You are given a user watch details csv file with the following columns:
user_id,date,watch_duration_secs
1, 2025-01-01, 160
1, 2025-01-03, 20
2, 2025-01-01, 100
3, 2025-01-01, 150
4, 2025-01-02, 210
1, 2025-01-04, 200

Write Pyspark code to find the month wise total number of unique users who watched the content for at least 1 minute cumulatively in a month
Expected Output
Month, Unique_user_count
2025-01, 2345
2025-02, 1256


                                                                                      df= df.withColumn("watch_duration”, col(“watch_duration_sec”).cast(“int”))

usr_month=df.withColumn("month”, concat(“-“, year(“date”, month(“date”))
user_month=df.groupBy("month”, “user_id”).agg(_sum(“watch_duration”).alias(“total_watch_time”)

unique_user=user_month.filter(“total_watch_time”)>=60)



                                                                                      
