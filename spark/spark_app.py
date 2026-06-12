from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, current_timestamp, window, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder \
    .appName("FraudDetectionProject") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("Time", DoubleType(), True),
    StructField("Amount", DoubleType(), True),
    StructField("Class", DoubleType(), True),  
    StructField("User_ID", StringType(), True)  
])

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "bank_transactions") \
    .option("startingOffsets", "earliest") \
    .load()

transactions = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", current_timestamp())

processed_df = transactions.withColumn("Category", 
    when(col("Amount") > 500, "Macro")
    .when(col("Amount") < 20, "Micro")
    .otherwise("Normal"))

blacklist_data = [("User_Hacker",)]
blacklist_df = spark.createDataFrame(blacklist_data, ["User_ID"])

final_transactions = processed_df.join(
    blacklist_df,
    on="User_ID",
    how="left"
).withColumn(
    "Status",
    when(col("User_ID") == "User_Hacker", "BLOCKED")
    .otherwise("OK")
)

bot_alerts = processed_df \
    .withWatermark("event_time", "30 seconds") \
    .groupBy(window(col("event_time"), "10 seconds", "10 seconds"), col("User_ID")) \
    .count() \
    .filter("count > 4") \
    .select("User_ID", "count", "window") \
    .withColumn("Alert", when(col("count") > 4, "BOT DETECTED"))

query1 = final_transactions.select(
    "User_ID", "Amount", "Category", "Status"
).writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

blocked_query = final_transactions.filter(col("Status") == "BLOCKED") \
    .select("User_ID", "Amount", "Category", "Status") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()


query2 = bot_alerts.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

query1.awaitTermination()