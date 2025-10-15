from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import socket, time

HOST, PORT = "127.0.0.1", 9999

# Preflight
for _ in range(20):
    try:
        s = socket.create_connection((HOST, PORT), timeout=1)
        s.close()
        break
    except OSError:
        time.sleep(0.25)

spark = SparkSession.builder.appName("RideShare-Task3").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.shuffle.partitions", "1")

schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", IntegerType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True),
])

raw = (spark.readStream
             .format("socket")
             .option("host", HOST)
             .option("port", PORT)
             .load())

parsed = raw.select(from_json(col("value"), schema).alias("j")).select("j.*")
with_time = parsed.withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

windowed = (with_time
            .withWatermark("event_time", "1 minute")
            .groupBy(window(col("event_time"), "5 minutes", "1 minute"))
            .agg(_sum("fare_amount").alias("total_fare"))
            .select(col("window.start").alias("window_start"),
                    col("window.end").alias("window_end"),
                    col("total_fare")))

# Console (append)
console_q = (windowed.writeStream
             .format("console")
             .outputMode("append")
             .trigger(processingTime="10 seconds")
             .option("truncate", "false")
             .start())

# CSV (append) — rows appear when each window finalizes
csv_q = (windowed.writeStream
         .format("csv")
         .outputMode("append")
         .trigger(processingTime="10 seconds")
         .option("header", "true")
         .option("checkpointLocation", "outputs/task_3/_chk")
         .option("path", "outputs/task_3")
         .start())

spark.streams.awaitAnyTermination()
