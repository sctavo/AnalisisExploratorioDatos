from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.appName("ETL_Vuelos").getOrCreate()

# Leer CSV
df = spark.read.csv("/opt/spark/data/flight_data_2024.csv", header=True, inferSchema=True)

# Renombrar columnas para trabajar uniforme
df = df.select(
    col("fl_date").alias("FlightDate"),
    col("op_unique_carrier").alias("Airline"),
    col("origin").alias("Origin"),
    col("dest").alias("Dest"),
    col("dep_delay").alias("DepDelay"),
    col("arr_delay").alias("ArrDelay"),
    col("distance").alias("Distance")
)

# Limpiar valores nulos
df = df.dropna(subset=["ArrDelay", "DepDelay"])

# Variable objetivo: retraso mayor a 15 minutos
df = df.withColumn("Retrasado", when(col("ArrDelay") > 15, 1).otherwise(0))

# Guardar resultado
df.write.mode("overwrite").parquet("/opt/spark/work-dir/cleaned_flights")


print("ETL COMPLETADO EXITOSAMENTE")
spark.stop()
