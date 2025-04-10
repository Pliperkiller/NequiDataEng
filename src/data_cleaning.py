from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, isnan, count, lit, monotonically_increasing_id
from pyspark.sql.types import FloatType, IntegerType, BooleanType, DateType

# Iniciar SparkSession
spark = SparkSession.builder.appName("FraudDetectionETL").getOrCreate()

# 1. Cargar datos CSV
df = spark.read.csv("ruta_datos.csv", header=True, inferSchema=True)

# 2. Eliminar vacíos de columnas prioritarias
columnas_prioritarias = ["step", "type", "amount", "nameOrig", "oldbalanceOrg", 
                         "newbalanceOrig", "nameDest", "isFraud"]

df_sin_nulos = df
for col_name in columnas_prioritarias:
    df_sin_nulos = df_sin_nulos.filter(~(col(col_name).isNull() | 
                                        (col(col_name) == "") | 
                                        isnan(col(col_name))))

# 3. Eliminar duplicados general
df_sin_duplicados = df_sin_nulos.dropDuplicates()

# 4. Normalización de campos tipo texto
# Asumimos que queremos estandarizar los valores en columnas tipo texto
df_normalizado = (df_sin_duplicados.withColumn(
    "type", when(col("type").isNotNull(), col("type")).otherwise(lit("UNKNOWN"))
).withColumn(
    "nameOrig", when(col("nameOrig").isNotNull(), col("nameOrig")).otherwise(lit("UNKNOWN"))
).withColumn(
    "nameDest", when(col("nameDest").isNotNull(), col("nameDest")).otherwise(lit("UNKNOWN"))
))

# 5. Creación de columna fecha
# Convertimos step a fecha y eliminamos la columna original
df_con_fecha = (df_normalizado.withColumn(
    "date", to_date(col("step").cast("string"), "yyyyMMdd")
))
df_con_fecha = df_con_fecha.drop("step")

# 6. Convertir tipo de columna
df_tipado = (df_con_fecha.withColumn("type", col("type").cast("string"))
                         .withColumn("amount", col("amount").cast(FloatType()))
                         .withColumn("nameOrig", col("nameOrig").cast("string"))
                         .withColumn("oldbalanceOrg", col("oldbalanceOrg").cast(FloatType()))
                         .withColumn("newbalanceOrig", col("newbalanceOrig").cast(FloatType()))
                         .withColumn("nameDest", col("nameDest").cast("string"))
                         .withColumn("oldbalanceDest", col("oldbalanceDest").cast(FloatType()))
                         .withColumn("newbalanceDest", col("newbalanceDest").cast(FloatType()))
                         .withColumn("isFraud", col("isFraud").cast(BooleanType()))
                         .withColumn("isFlaggedFraud", col("isFlaggedFraud").cast(BooleanType()))
                         .withColumn("date", col("date").cast(DateType()))
)

# 7. Renombrar columnas
df_renombrado = (df_tipado.withColumnRenamed("type", "type")
                         .withColumnRenamed("amount", "amount")
                         .withColumnRenamed("nameOrig", "name_orig")
                         .withColumnRenamed("oldbalanceOrg", "orig_balance_orig")
                         .withColumnRenamed("newbalanceOrig", "new_balance_orig")
                         .withColumnRenamed("nameDest", "name_dest")
                         .withColumnRenamed("oldbalanceDest", "old_balance_dest")
                         .withColumnRenamed("newbalanceDest", "new_balance_dest")
                         .withColumnRenamed("isFraud", "is_fraud")
                         .withColumnRenamed("isFlaggedFraud", "is_flagged_fraud")
                         .withColumnRenamed("date", "datetime")
)

# 8. Preparar las tablas según el modelo relacional
# Tabla transaction_type
transaction_types_df = df_renombrado.select("type").distinct()
transaction_types_df = transaction_types_df.withColumn("type_id", monotonically_increasing_id())
transaction_types_df = (transaction_types_df.select(
    col("type_id").cast("int"),
    col("type").alias("transaction_type")
))

# Tabla account (combinando las cuentas de origen y destino)
accounts_orig_df = (df_renombrado.select(
    col("name_orig").alias("account_code")
).distinct())

accounts_dest_df = (df_renombrado.select(
    col("name_dest").alias("account_code")
).distinct())

accounts_df = accounts_orig_df.union(accounts_dest_df).distinct()
accounts_df = accounts_df.withColumn("account_id", monotonically_increasing_id())

# Tabla transaction
df_with_type_id = (df_renombrado.join(
    transaction_types_df.select(col("type_id"), col("transaction_type").alias("type")),
    on="type"
))

df_with_orig_id = (df_with_type_id.join(
    accounts_df.select(col("account_id").alias("orig_account_id"), col("account_code").alias("name_orig")),
    on="name_orig"
))

df_with_both_ids = (df_with_orig_id.join(
    accounts_df.select(col("account_id").alias("dest_account_id"), col("account_code").alias("name_dest")),
    on="name_dest"
))

transactions_df = (df_with_both_ids.select(
    monotonically_increasing_id().alias("transaction_id"),
    col("type_id").cast("int"),
    col("orig_account_id").cast("bigint"),
    col("dest_account_id").cast("bigint"),
    col("orig_balance_orig").alias("old_balance_orig").cast("decimal(18,2)"),
    col("new_balance_orig").cast("decimal(18,2)"),
    col("old_balance_dest").cast("decimal(18,2)"),
    col("new_balance_dest").cast("decimal(18,2)"),
    col("amount").cast("decimal(18,2)"),
    col("is_fraud").cast("boolean"),
    col("is_flagged_fraud").cast("boolean"),
    col("datetime")
))

# 9. Guardar las tablas en formato Parquet
transaction_types_df.write.mode("overwrite").parquet("ruta/transaction_type.parquet")
accounts_df.write.mode("overwrite").parquet("ruta/account.parquet")
transactions_df.write.mode("overwrite").parquet("ruta/transaction.parquet")

# 10. Mostrar un resumen del procesamiento
print(f"Registros originales: {df.count()}")
print(f"Registros después de eliminar nulos: {df_sin_nulos.count()}")
print(f"Registros después de eliminar duplicados: {df_sin_duplicados.count()}")
print(f"Tipos de transacción: {transaction_types_df.count()}")
print(f"Cuentas únicas: {accounts_df.count()}")
print(f"Transacciones finales: {transactions_df.count()}")