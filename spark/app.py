import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructType, FloatType, StringType
from pyspark.sql.functions import from_json, col


# Configuración de PySpark
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

KAFKA_TOPIC = "weather_data"
KAFKA_BROKER = "localhost:9092"

MONGO_URI = "mongodb://username:password@localhost:27017"
MONGO_DATABASE = "weather_db"
MONGO_COLLECTION = "weather_data"

# Definir el esquema de los datos del clima
schema = StructType() \
    .add("main", StructType() \
         .add("temp", FloatType())
         .add("humidity", FloatType())) \
    .add("weather", StringType())

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("WeatherStreamingToMongoDB") \
    .config("spark.mongodb.output.uri", f"{MONGO_URI}/{MONGO_DATABASE}.{MONGO_COLLECTION}") \
    .getOrCreate()

# Leer el stream de Kafka
stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

# Procesar los datos de Kafka
weather_df = stream_df.select(from_json(col("value").cast("string"), schema).alias("data"))

# Extraer las columnas relevantes
processed_df = weather_df.select(
    col("data.main.temp").alias("temperature"),
    col("data.main.humidity").alias("humidity"),
    col("data.weather").alias("description")
)

# Guardar los datos en MongoDB

query = processed_df.writeStream \
    .format("mongodb") \
    .option("uri", f"{MONGO_URI}/{MONGO_DATABASE}.{MONGO_COLLECTION}") \
    .outputMode("append") \
    .start()

query.awaitTermination()
