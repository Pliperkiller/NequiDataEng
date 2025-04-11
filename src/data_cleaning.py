from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, lit
from pyspark.sql.types import FloatType, BooleanType, DateType
import os
from pathlib import Path

import logging
logging.getLogger("py4j").setLevel(logging.ERROR)


class DataCleaningPipeline:
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
        """Cargar datos desde un archivo CSV."""
        return self.spark.read.csv(input_path, header=True, inferSchema=False)
    
    def remove_nulls(self, df, priority_columns):
        """Eliminar filas con valores nulos en columnas prioritarias."""
        result_df = df
        for col_name in priority_columns:
            result_df = result_df.filter(~(col(col_name).isNull() | (col(col_name) == "")))
        return result_df
    
    def remove_duplicates(self, df):
        """Eliminar filas duplicadas."""
        return df.dropDuplicates()
    
    def normalize_text_fields(self, df):
        """Normalizar campos de texto."""
        return (df
        .withColumn(
            "type", when(col("type").isNotNull(), col("type")).otherwise(lit("UNKNOWN"))
        ).withColumn(
            "nameOrig", when(col("nameOrig").isNotNull(), col("nameOrig")).otherwise(lit("UNKNOWN"))
        ).withColumn(
            "nameDest", when(col("nameDest").isNotNull(), col("nameDest")).otherwise(lit("UNKNOWN"))
        ))
    
    def create_date_column(self, df):
        """Crear columna de fecha a partir de step tomamos fecha inicial este año (tomado como una referencia)."""
        start_date = "2025-01-01 00:00:00"
        df_with_date = (df.withColumn(
            "date", expr(f"timestamp('{start_date}') + (step * INTERVAL '1' HOUR)")
        ))
        return df_with_date.drop("step")
    
    def convert_column_types(self, df):
        """Convertir columnas a los tipos de datos correctos."""
        return (df.withColumn("type", col("type").cast("string"))
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
    
    def rename_columns(self, df):
        """Renombrar columnas según el modelo de datos."""
        return (df.withColumnRenamed("type", "type")
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
    
    
    def save_parquet(self, df, output_path):
        """Guardar DataFrame en formato Parquet usando pandas."""
        # Convertir a pandas y guardar
        pandas_df = df.toPandas()
        pandas_df.to_parquet(str(output_path))
        
    def run_pipeline(self, input_path, output_dir):
        """Ejecutar el pipeline completo."""
        # Cargar datos
        df = self.load_data(input_path)
        original_count = df.count()
        
        # Paso 1: Eliminar nulos
        priority_columns = ["step", "type", "amount", "nameOrig", "oldbalanceOrg", 
                           "newbalanceOrig", "nameDest", "isFraud"]
        
        df_no_nulls = self.remove_nulls(df, priority_columns)
        no_nulls_count = df_no_nulls.count()
        
        # Paso 2: Eliminar duplicados
        df_no_duplicates = self.remove_duplicates(df_no_nulls)
        no_duplicates_count = df_no_duplicates.count()
        
        # Paso 3: Normalizar texto
        df_normalized = self.normalize_text_fields(df_no_duplicates)
        
        # Paso 4: Crear columna fecha
        df_with_date = self.create_date_column(df_normalized)
        
        # Paso 5: Convertir tipos
        df_typed = self.convert_column_types(df_with_date)
        
        # Paso 6: Renombrar columnas
        df_renamed = self.rename_columns(df_typed)
        
        
        # Paso 7: Guardar en Parquet
        self.save_parquet(df_renamed, os.path.join(output_dir))

        summary = {
        "registros_originales": original_count,
        "registros_sin_nulos": no_nulls_count,
        "registros_sin_duplicados": no_duplicates_count
        }

        return summary


if __name__ == "__main__":
    base_dir = Path().resolve().parent

    dataset_path = str(base_dir / 'data' / 'raw' / 'PS_20174392719_1491204439457_log.csv')
    output_path = str(base_dir / 'data' / 'processed' / 'data_cleaned.parquet')

    pipeline = DataCleaningPipeline()
    summary = pipeline.run_pipeline(dataset_path, output_path)
    for key, value in summary.items():
        print(f"{key}: {value}")