import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, split, trim, when, count, concat_ws
from pyspark.sql.functions import regexp_replace

# Obtener argumentos del Job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Parámetros de conexión a RDS
RDS_HOST = "database-rds.c9ke6aw2ksad.us-east-1.rds.amazonaws.com"
RDS_PORT = "5432"
RDS_USER = "postgres"
RDS_PASSWORD = "Recaudos123#"
RDS_DATABASE = "Nequi"

# Cargar datos desde S3
print("Cargando datos desde S3...")
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://analitica-ti-bucket/911_calls_for_service.csv"], "recurse": True},
    transformation_ctx="AmazonS3_node1736566340413"
)

# Convertir DynamicFrame a DataFrame de Spark
print("Convirtiendo DynamicFrame a DataFrame de Spark...")
spark_df = dynamic_frame.toDF()

# ----------------- Exploración y Análisis de Datos (EDA) ----------------

# Mostrar número de filas y columnas
num_rows = spark_df.count()
num_columns = len(spark_df.columns)
print(f"El dataset tiene {num_rows} filas y {num_columns} columnas.")

# Mostrar estadísticas descriptivas
print("Estadísticas descriptivas:")
spark_df.describe().show()


# Identificar duplicados
print("Número de filas duplicadas:")
duplicates = spark_df.count() - spark_df.dropDuplicates().count()
print(f"Filas duplicadas: {duplicates}")

# ---------------- Extracción y Transformación de los datos (ETL) ----------------

# Rellenar valores nulos o vacíos en 'location' con "0,0"
spark_df = spark_df.withColumn(
    "location",
    when(col("location").isNull() | (col("location") == ""), "0,0").otherwise(col("location"))
)

# Eliminar caracteres innecesarios como paréntesis y espacios en la columna 'location'
spark_df = spark_df.withColumn("location", regexp_replace(col("location"), "[()\\s]", ""))

# Dividir la columna 'location' en 'latitude' y 'longitude'
spark_df = spark_df.withColumn("latitude", split(col("location"), ",").getItem(0).cast("double")) \
                   .withColumn("longitude", split(col("location"), ",").getItem(1).cast("double"))
                   
# Rellenar valores nulos en 'latitude' y 'longitude' con 0
spark_df = spark_df.fillna({"latitude": 0, "longitude": 0})

# Eliminar la columna 'location'
spark_df = spark_df.drop("location")

# Mostrar esquema y primeras filas
print("Esquema del DataFrame:")
spark_df.printSchema()

print("Primeras filas:")
spark_df.show(5)

# Eliminar la primera columna
print("Eliminando la primera columna...")
first_column_name = spark_df.columns[0]  # Obtener el nombre de la primera columna
spark_df = spark_df.drop(first_column_name)

# Mostrar esquema después de eliminar la primera columna
print("Esquema después de eliminar la primera columna:")
spark_df.printSchema()

# Concatenar todas las columnas en una nueva columna
print("Concatenando todas las columnas en una nueva columna...")
spark_df = spark_df.withColumn("concat_column", concat_ws("_", *spark_df.columns))

# Eliminar duplicados basados en la columna concatenada
print("Eliminando duplicados basados en la columna concatenada...")
spark_df = spark_df.dropDuplicates(["concat_column"])

# Eliminar la columna concatenada
print("Eliminando la columna concatenada...")
spark_df = spark_df.drop("concat_column")

# Mostrar datos después de las transformaciones
print("Datos después de las transformaciones:")
spark_df.show(5)

# Configuración de conexión JDBC a RDS
jdbc_url = f"jdbc:postgresql://{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}"
jdbc_properties = {
    "user": RDS_USER,
    "password": RDS_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# Guardar datos limpios en Amazon RDS
print("Cargando datos limpios en Amazon RDS...")
try:
    spark_df.write.jdbc(
        url=jdbc_url,
        table="call911",  # Nombre de la tabla
        mode="overwrite",      # Modo: 'overwrite' para reemplazar datos, 'append' para agregar
        properties=jdbc_properties
    )
    print("Datos cargados exitosamente en la tabla 'call911' de RDS.")
except Exception as e:
    print(f"Error al cargar datos en RDS: {e}")

# Finalizar el Job
print("Finalizando el Job...")
job.commit()