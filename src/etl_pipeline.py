import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, lit, expr
from pyspark.sql.types import IntegerType, FloatType, StringType, DoubleType

args = getResolvedOptions(sys.argv, ['JOB_NAME'])


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

input_path = "s3://nqi-tech-raw/PS_20174392719_1491204439457_log.csv"
output_path = "s3://nqi-tech-processed/data_cleaned.parquet"

datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [input_path]},
    format="csv",
    format_options={"withHeader": True, "separator": ","}
)


df = datasource.toDF()

#### Procesado de data ####

# 0. establecer tipado de columnas
df = df \
    .withColumn("step", col("step").cast(IntegerType())) \
    .withColumn("type", col("type").cast(StringType())) \
    .withColumn("amount", col("amount").cast(DoubleType())) \
    .withColumn("nameOrig", col("nameOrig").cast(StringType())) \
    .withColumn("oldbalanceOrg", col("oldbalanceOrg").cast(FloatType())) \
    .withColumn("newbalanceOrig", col("newbalanceOrig").cast(FloatType())) \
    .withColumn("nameDest", col("nameDest").cast(StringType())) \
    .withColumn("oldbalanceDest", col("oldbalanceDest").cast(FloatType())) \
    .withColumn("newbalanceDest", col("newbalanceDest").cast(FloatType())) \
    .withColumn("isFraud", col("isFraud").cast(IntegerType())) \
    .withColumn("isFlaggedFraud", col("isFlaggedFraud").cast(IntegerType()))



# 1. Eliminar duplicados
df_clean = df.dropDuplicates()

# 2. Crear columna de fecha basada en la columna 'step'
start_date = "2025-01-01 00:00:00"
df_clean = df_clean.withColumn(
    "date",
    expr(f"timestamp('{start_date}') + (step * INTERVAL '1' HOUR)")
)

# 3. Normalizar la columna 'amount' usando log1p
df_clean = df_clean.withColumn("amount_log", expr("log1p(amount)"))

# 4. Rellenar valores nulos en la columna 'amount' con la mediana
median_amount = df_clean.filter(col("amount").isNotNull()).approxQuantile("amount", [0.5], 0.01)[0]
df_clean = df_clean.fillna({"amount": median_amount})

############################


df_dynamic = DynamicFrame.fromDF(df_clean, glueContext, "df_dynamic")

glueContext.write_dynamic_frame.from_options(
    frame=df_dynamic,
    connection_type="s3",
    connection_options={"path": output_path},
    format="parquet",
    format_options={"compression": "gzip"}
)

job.commit()