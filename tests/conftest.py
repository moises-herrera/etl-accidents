import pytest
import os
import sys
from pyspark.sql import SparkSession

# Agregar el directorio padre al path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture(scope="session")
def spark():
    """Fixture de sesión de Spark compartida para todos los tests."""
    # Detener cualquier sesión de Spark existente
    SparkSession.builder.getOrCreate().stop()
    
    spark = (
        SparkSession.builder
        .appName("ETL_Tests")
        .master("local[2]")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "localhost")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    
    yield spark
    
    # Limpiar al finalizar todos los tests
    spark.stop()


def pytest_configure(config):
    """Configuración inicial de pytest."""
    # Suprimir warnings de Spark
    import warnings
    warnings.filterwarnings("ignore", category=ResourceWarning)
    warnings.filterwarnings("ignore", category=DeprecationWarning)
