from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, monotonically_increasing_id

spark = (SparkSession.builder
        .appName("Data Transformation for Relational Model")
        .getOrCreate()
        )

input_path = "s3://nqi-tech-raw/PS_20174392719_1491204439457_log.csv"
output_path = "s3://nqi-tech-processed/data/relational_model/"

df = spark.read.csv(input_path, header=True, inferSchema=True)

# 1. Crear tabla transaction_type
# Extraer tipos únicos de transacción
transaction_types_df = df.select("type").distinct()
transaction_types_df = transaction_types_df.withColumn("type_id", monotonically_increasing_id())
transaction_types_df = transaction_types_df.select(
                                                    col("type_id").cast("int"),
                                                    col("type").alias("transaction_type")
                                                    )

transaction_types_df.write.mode("overwrite").parquet(f"{output_path}transaction_type.parquet")


# 2. Crear tabla account (combinando las cuentas de origen y destino)
# Extraer cuentas únicas (origen)
accounts_orig_df = df.select(
                            col("name_orig").alias("account_code")
                            ).distinct()

# Extraer cuentas únicas (destino)
accounts_dest_df = df.select(
                            col("name_dest").alias("account_code")
                            ).distinct()

# Unir cuentas y asignar IDs
accounts_df = accounts_orig_df.union(accounts_dest_df).distinct()
accounts_df = accounts_df.withColumn("account_id", monotonically_increasing_id())

accounts_df.write.mode("overwrite").parquet(f"{output_path}transactions.parquet")


# 3. Crear tabla transaction
# Primero, unir con la tabla transaction_type para obtener type_id
df_with_type_id = df.join(
    transaction_types_df.select(col("type_id"), col("transaction_type").alias("type")),
    on="type"
)

# Unir con la tabla account para obtener IDs de cuenta origen
df_with_orig_id = df_with_type_id.join(
    accounts_df.select(col("account_id").alias("orig_account_id"), col("account_code").alias("name_orig")),
    on="name_orig"
)

# Unir con la tabla account para obtener IDs de cuenta destino
df_with_both_ids = df_with_orig_id.join(
    accounts_df.select(col("account_id").alias("dest_account_id"), col("account_code").alias("name_dest")),
    on="name_dest"
)

# Crear la tabla final de transacciones
transactions_df = df_with_both_ids.select(
    monotonically_increasing_id().alias("transaction_id"),
    col("type_id").cast("int"),
    col("orig_account_id").cast("bigint"),
    col("dest_account_id").cast("bigint"),
    col("orld_balance_orig").alias("old_balance_orig").cast("decimal(18,2)"),
    col("new_balance_orig").alias("new_balance_orig").cast("decimal(18,2)"),
    col("old_balance_dest").alias("old_balance_dest").cast("decimal(18,2)"),
    col("new_balance_dest").alias("new_balance_dest").cast("decimal(18,2)"),
    col("amount").cast("decimal(18,2)"),
    col("is_fraud").alias("is_fraud").cast("boolean"),
    col("is_flagged_fraud").alias("is_flagged_fraud").cast("boolean"),
    col("datetime").alias("transaction_date_time")
)
transactions_df.write.mode("overwrite").parquet(f"{output_path}transaction.parquet")


spark.stop()