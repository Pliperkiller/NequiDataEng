from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date, when, isnan, count, lit, monotonically_increasing_id
from pyspark.sql.types import FloatType, IntegerType, BooleanType, DateType
import os
from pathlib import Path

class ETLPipeline:
    def __init__(self, spark=None):
        """Inicializar el pipeline con una sesión Spark opcional."""
        if not spark:
            # Configuracion de Spark para operar usando 4GB de memoria ram
            self.spark = (SparkSession.builder
                .appName("FraudDetectionETL")
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "4g")
                .getOrCreate())
        else:
            self.spark = spark
    
    def load_data(self, input_path):
        """Cargar datos desde un archivo Parquet."""
        return self.spark.read.parquet(input_path)
    
    def create_transaction_type_table(self, df):
        """Crear tabla de tipos de transacción."""
        types_df = df.select("type").distinct()
        types_df = types_df.withColumn("type_id", monotonically_increasing_id())
        return (types_df.select(
            col("type_id").cast("int"),
            col("type").alias("transaction_type")
        ))
    
    def create_account_table(self, df):
        """Crear tabla de cuentas."""
        # Cuentas origen
        accounts_orig_df = (df.select(
            col("name_orig").alias("account_code")
        ).distinct())
        
        # Cuentas destino
        accounts_dest_df = (df.select(
            col("name_dest").alias("account_code")
        ).distinct())
        
        # Unir y asignar IDs
        accounts_df = accounts_orig_df.union(accounts_dest_df).distinct()
        return accounts_df.withColumn("account_id", monotonically_increasing_id())
    
    def create_transaction_table(self, df, transaction_types_df, accounts_df):
        """Crear tabla de transacciones."""
        # Unir con tipos de transacción
        df_with_type_id = (df.join(
            transaction_types_df.select(col("type_id"), col("transaction_type").alias("type")),
            on="type"
        ))
        
        # Unir con cuentas origen
        df_with_orig_id = (df_with_type_id.join(
            accounts_df.select(col("account_id").alias("orig_account_id"), col("account_code").alias("name_orig")),
            on="name_orig"
        ))
        
        # Unir con cuentas destino
        df_with_both_ids = (df_with_orig_id.join(
            accounts_df.select(col("account_id").alias("dest_account_id"), col("account_code").alias("name_dest")),
            on="name_dest"
        ))
        
        # Seleccionar y transformar columnas finales
        return (df_with_both_ids.select(
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
    
    def save_parquet(self, df, output_path):
        """Guardar DataFrame en formato Parquet usando pandas."""
        # Convertir a pandas y guardar
        pandas_df = df.toPandas()
        pandas_df.to_parquet(str(output_path))
        
    def run_pipeline(self, input_path, output_dir):
        """Ejecutar el pipeline completo."""
        # Cargar datos
        df = self.load_data(input_path)
        
        # Crear tablas relacionales
        transaction_types_df = self.create_transaction_type_table(df)
        accounts_df = self.create_account_table(df)
        transactions_df = self.create_transaction_table(df, transaction_types_df, accounts_df)
        
        # Guardar tablas
        self.save_parquet(transaction_types_df, os.path.join(output_dir, "transaction_type.parquet"))
        self.save_parquet(accounts_df, os.path.join(output_dir, "account.parquet"))
        self.save_parquet(transactions_df, os.path.join(output_dir, "transaction.parquet"))
        

# Para usar el pipeline
if __name__ == "__main__":
    base_dir = Path().resolve().parent

    dataset_path = str(base_dir / 'data' / 'processed' / 'data_cleaned.parquet')
    output_path = str(base_dir / 'data' / 'modeled' )

    pipeline = ETLPipeline()
    summary = pipeline.run_pipeline(dataset_path, output_path)
