from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, avg
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

spark = SparkSession.builder.appName("RideShare-Task2").getOrCreate()
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

parsed = raw.select(from_json(col("value"), schema).alias("j")).select("j.*").na.drop(subset=["driver_id"])

agg = (parsed
       .groupBy("driver_id")
       .agg(_sum("fare_amount").alias("total_fare"),
            avg("distance_km").alias("avg_distance")))

# Console in COMPLETE mode (running totals)
console_q = (agg.writeStream
             .format("console")
             .outputMode("complete")
             .option("truncate", "false")
             .start())

# Write each micro-batch as one tidy CSV in epoch folders
def write_epoch(df, epoch_id):
    (df.orderBy(col("total_fare").desc())
       .coalesce(1)
       .write.mode("overwrite")
       .option("header", "true")
       .csv(f"outputs/task_2/epoch={epoch_id}"))

csv_q = (agg.writeStream
         .outputMode("complete")
         .trigger(processingTime="10 seconds")
         .foreachBatch(write_epoch)
         .start())

spark.streams.awaitAnyTermination()
