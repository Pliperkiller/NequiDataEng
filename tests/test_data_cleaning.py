import os
import tempfile
import pytest
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, DateType

from src.data_cleaning import DataCleaningPipeline


@pytest.fixture(scope="session")
def spark():
    """Crear una sesión Spark para pruebas."""
    return SparkSession.builder \
        .appName("TestDataCleaningPipeline") \
        .master("local[1]") \
        .getOrCreate()


@pytest.fixture
def pipeline(spark):
    """Instanciar el pipeline usando la sesión Spark de prueba."""
    return DataCleaningPipeline(spark)


@pytest.fixture
def sample_df(spark):
    """Crear un DataFrame básico para pruebas de varios métodos."""
    schema = StructType([
        StructField("step", IntegerType(), True),
        StructField("type", StringType(), True),
        StructField("amount", FloatType(), True),
        StructField("nameOrig", StringType(), True),
        StructField("oldbalanceOrg", FloatType(), True),
        StructField("newbalanceOrig", FloatType(), True),
        StructField("nameDest", StringType(), True),
        StructField("oldbalanceDest", FloatType(), True),
        StructField("newbalanceDest", FloatType(), True),
        StructField("isFraud", IntegerType(), True),
        StructField("isFlaggedFraud", IntegerType(), True)
    ])
    
    # Incluye filas válidas, filas con nulos y filas duplicadas
    data = [
        (1, "PAYMENT", 9839.64, "C123", 170136.0, 160296.36, "M197", 0.0, 0.0, 0, 0),
        (1, "TRANSFER", 200.0, "C456", 5000.0, 4800.0, "C789", 300.0, 100.0, 1, 0),
        (2, None, 500.0, "C001", 1000.0, 500.0, "C002", 200.0, 300.0, 0, 0),    # nulo en 'type'
        (2, "PAYMENT", None, "C003", 1500.0, 1500.0, "C004", 0.0, 1500.0, 0, 0), # nulo en 'amount'
        (1, "PAYMENT", 9839.64, "C123", 170136.0, 160296.36, "M197", 0.0, 0.0, 0, 0) # fila duplicada de la 1ª
    ]
    
    return spark.createDataFrame(data, schema)


def test_remove_nulls(pipeline, sample_df):
    """
    Prueba que la función remove_nulls elimina filas con nulos en las columnas especificadas.
    """
    # Se definen las columnas prioritarias para quitar nulos
    priority_columns = ["type", "amount"]
    
    df_clean = pipeline.remove_nulls(sample_df, priority_columns)
    
    # Se esperaban eliminar las filas 3 y 4 (los índices en Python empezando en 0)
    expected_count = sample_df.count() - 2
    actual_count = df_clean.count()
    assert actual_count == expected_count, f"Se esperaban {expected_count} registros, se obtuvieron {actual_count}"


def test_remove_duplicates(pipeline, sample_df):
    """
    Prueba que la función remove_duplicates elimina filas duplicadas.
    """
    # Antes de quitar duplicados: contar registros
    count_before = sample_df.count()
    df_dedup = pipeline.remove_duplicates(sample_df)
    count_after = df_dedup.count()
    
    # Sabemos que se duplicó la primera fila, por lo que debería haber un registro menos
    assert count_after == count_before - 1, f"Se esperaba {count_before - 1} registros, se obtuvo {count_after}"


def test_normalize_text_fields(pipeline, spark):
    """
    Prueba que normalize_text_fields reemplaza nulos en campos de texto por 'UNKNOWN'
    y no afecta los registros que ya tienen valor.
    """
    # Crear DataFrame de prueba específico para campos de texto
    schema = StructType([
        StructField("type", StringType(), True),
        StructField("nameOrig", StringType(), True),
        StructField("nameDest", StringType(), True)
    ])
    data = [
        ("PAYMENT", "C123", "M001"),
        (None, "C456", "M002"),
        ("TRANSFER", None, "M003"),
        ("CASH_OUT", "C789", None)
    ]
    df_text = spark.createDataFrame(data, schema)
    
    df_norm = pipeline.normalize_text_fields(df_text)
    rows = df_norm.collect()
    
    # Verificamos que los valores nulos se reemplazaron por "UNKNOWN"
    assert rows[1]["type"] == "UNKNOWN", "El valor nulo en 'type' debe haber sido reemplazado por 'UNKNOWN'"
    assert rows[2]["nameOrig"] == "UNKNOWN", "El valor nulo en 'nameOrig' debe haber sido reemplazado por 'UNKNOWN'"
    assert rows[3]["nameDest"] == "UNKNOWN", "El valor nulo en 'nameDest' debe haber sido reemplazado por 'UNKNOWN'"
    
    # Verificamos que los valores que ya tenían texto no se modificaron
    assert rows[0]["type"] == "PAYMENT", "El valor 'PAYMENT' no debe haberse modificado"


def test_create_date_column(pipeline, spark):
    """
    Prueba que create_date_column crea la columna 'date' a partir de 'step'
    y elimina la columna 'step'. Además, verifica que la fecha calculada sea correcta.
    """
    # Crear DataFrame con columna "step"
    schema = StructType([
        StructField("step", IntegerType(), True),
        StructField("type", StringType(), True)
    ])
    data = [
        (0, "PAYMENT"),
        (10, "TRANSFER")
    ]
    df_steps = spark.createDataFrame(data, schema)
    
    df_date = pipeline.create_date_column(df_steps)
    
    # Comprobar que la columna 'step' fue eliminada y 'date' existe
    assert "step" not in df_date.columns, "La columna 'step' debe ser eliminada después de crear 'date'"
    assert "date" in df_date.columns, "La columna 'date' debe existir en el DataFrame"
    
    # Validar el valor de la fecha con la lógica aplicada:
    # Según el método, se usa start_date = "2025-01-01 00:00:00" y se suma step * 1 hora.
    base_date = datetime.strptime("2025-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
    results = df_date.select("date").collect()
    
    # Verificar para ambas filas
    for row, (step, _) in zip(results, data):
        expected_date = (base_date + timedelta(hours=step)).date()
        actual_date = row["date"]
        # Como convertimos a DateType, comparamos solo la parte de la fecha.
        assert actual_date == expected_date, f"Para step {step}: se esperaba {expected_date}, se obtuvo {actual_date}"


def test_convert_column_types(pipeline, spark):
    """
    Prueba que convert_column_types realiza correctamente la conversión de tipos de
    columnas, incluyendo la conversión de la columna 'date' a DateType.
    """
    # Crear DataFrame que simule la salida de create_date_column
    schema = StructType([
        StructField("type", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("nameOrig", StringType(), True),
        StructField("oldbalanceOrg", StringType(), True),
        StructField("newbalanceOrig", StringType(), True),
        StructField("nameDest", StringType(), True),
        StructField("oldbalanceDest", StringType(), True),
        StructField("newbalanceDest", StringType(), True),
        StructField("isFraud", StringType(), True),
        StructField("isFlaggedFraud", StringType(), True),
        StructField("date", StringType(), True)
    ])
    data = [
        ("PAYMENT", "1000.5", "C123", "2000.0", "1000.0", "M001", "0", "1000.0", "0", "0", "2025-01-01"),
        ("TRANSFER", "500.0", "C456", "1500.0", "1000.0", "M002", "100.0", "900.0", "1", "0", "2025-01-02")
    ]
    df_in = spark.createDataFrame(data, schema)
    
    df_out = pipeline.convert_column_types(df_in)
    
    # Comprobar los tipos de los campos después de la conversión
    schema_out = df_out.schema
    
    # Buscar cada campo en el esquema y validar el tipo
    field_types = {
        "type": "string",
        "amount": FloatType().simpleString(),
        "nameOrig": "string",
        "oldbalanceOrg": FloatType().simpleString(),
        "newbalanceOrig": FloatType().simpleString(),
        "nameDest": "string",
        "oldbalanceDest": FloatType().simpleString(),
        "newbalanceDest": FloatType().simpleString(),
        "isFraud": BooleanType().simpleString(),
        "isFlaggedFraud": BooleanType().simpleString(),
        "date": DateType().simpleString()
    }
    for field_name, expected_type in field_types.items():
        # Buscar el campo en el esquema
        field = schema_out[field_name]
        actual_type = field.dataType.simpleString()
        assert actual_type == expected_type, f"El campo '{field_name}' debe ser de tipo {expected_type}, pero es {actual_type}"
