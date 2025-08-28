from config import configuration

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.functions import col

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType

def main():
    s3_url: str = configuration.get('S3_ENDPOINT_URL')
    s3_access_key: str = configuration.get('AWS_ACCESS_KEY_ID')
    s3_secret_key: str = configuration.get('AWS_SECRET_ACCESS_KEY')

    conf = SparkConf()
    conf.set('spark.hadoop.fs.s3a.endpoint', s3_url)
    conf.set('spark.hadoop.fs.s3a.access.key', s3_access_key)
    conf.set('spark.hadoop.fs.s3a.secret.key', s3_secret_key)
    conf.set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    conf.set("spark.hadoop.fs.s3a.aws.credendials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    conf.set('spark.hadoop.fs.s3a.path.style.access', 'true')
    conf.set("spark.jar.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,"
            "org.apache.hadoop:hadoop-aws:4.0.0,"
            "software.amazon.awssdk:aws-java-sdk:2.32.0,"
            "software.amazon.awssdk:auth:2.32.0")

    spark = (
        SparkSession.builder.appName("SparkCitySteaming")
            .config(conf=conf)
            .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    vehicleSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuelType", StringType(), True),
    ])
    
    gpsSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicleType", StringType(), True)
    ])

    trafficSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("cameraId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("snapshot", StringType(), True)
    ])

    weatherSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("windSpeed", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("airQualityIndex", DoubleType(), True)
    ])

    emergencySchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("incidentId", StringType(), True),
        StructField("type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True)
    ])

    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', 'broker:9092')
                .option('subscribe', topic)
                .option('startingOffset', 'earliest')
                .load()
                .selectExpr('CAST(value as STRING)')
                .select(from_json(col('value'), schema).alias('data'))
                .select('data.*')
                # .withWatermark('timestamp')
                )

    def streamWriter(input, checkpointFolder, output):
        return (input.writeStream
                .format('parquet')
                .option('checkpointlocation', checkpointFolder)
                .option('path', output)
                .outputMode('append')
                .start()
                )

    vehicleDF = read_kafka_topic('vehicle_data', vehicleSchema).alias('vehicle')
    
    # gpsDF = read_kafka_topic('gps_data', gpsSchema).alias('gps')
    # trafficDF = read_kafka_topic('traffic_data', trafficSchema).alias('traffic')
    # weatherDF = read_kafka_topic('weather_data', weatherSchema).alias('weather')
    # emergencyDF = read_kafka_topic('emergency_data', emergencySchema).alias('emergency')

    query1 = streamWriter(vehicleDF, 
                "s3a://spark-streaming-data/checkpoints/vehicle_data",
                "s3a://spark-streaming-data/data/vehicle_data")

    # query1 = vehicleDF \
    #     .writeStream \
    #     .outputMode("update") \
    #     .format("console") \
    #     .start()

    query1.awaitTermination()

    

if __name__ == "__main__":
    main()