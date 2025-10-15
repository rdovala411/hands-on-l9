from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import socket, time

HOST, PORT = "127.0.0.1", 9999

# Preflight: wait for the generator to be reachable (up to ~5s)
for _ in range(20):
    try:
        s = socket.create_connection((HOST, PORT), timeout=1)
        s.close()
        break
    except OSError:
        time.sleep(0.25)

spark = SparkSession.builder.appName("RideShare-Task1").getOrCreate()
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

parsed = (
    raw.select(from_json(col("value"), schema).alias("json"))
       .select("json.*")
       .na.drop(subset=["trip_id"])
)

# Console (append)
console_q = (parsed.writeStream
             .format("console")
             .outputMode("append")
             .option("truncate", "false")
             .start())

# CSV sink (append)
csv_q = (parsed.writeStream
         .format("csv")
         .outputMode("append")
         .trigger(processingTime="10 seconds")
         .option("header", "true")
         .option("checkpointLocation", "outputs/task_1/_chk")
         .option("path", "outputs/task_1")
         .start())

spark.streams.awaitAnyTermination()
