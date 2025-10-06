import pytest
import os
import sys
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)
from pyspark.sql.functions import col

# Agregar el directorio padre al path para importar el módulo etl
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from etl_accidents import etl


@pytest.fixture
def sample_accidents_data(spark):
    """Fixture que crea un DataFrame de ejemplo con datos de accidentes."""
    schema = StructType(
        [
            StructField("ID", StringType(), True),
            StructField("Severity", IntegerType(), True),
            StructField("Start_Time", StringType(), True),
            StructField("End_Time", StringType(), True),
            StructField("State", StringType(), True),
            StructField("City", StringType(), True),
            StructField("County", StringType(), True),
            StructField("Weather_Condition", StringType(), True),
            StructField("Temperature(F)", DoubleType(), True),
            StructField("Humidity(%)", DoubleType(), True),
            StructField("Pressure(in)", DoubleType(), True),
            StructField("Visibility(mi)", DoubleType(), True),
            StructField("Wind_Speed(mph)", DoubleType(), True),
            StructField("Precipitation(in)", DoubleType(), True),
            StructField("Sunrise_Sunset", StringType(), True),
            StructField("Civil_Twilight", StringType(), True),
            StructField("Nautical_Twilight", StringType(), True),
        ]
    )

    data = [
        (
            "A-1",
            2,
            "2021-01-15 08:30:00",
            "2021-01-15 09:00:00",
            "CA",
            "Los Angeles",
            "Los Angeles County",
            "Clear",
            65.5,
            45.0,
            29.8,
            10.0,
            5.5,
            0.0,
            "Day",
            "Day",
            "Day",
        ),
        (
            "A-2",
            3,
            "2021-02-20 14:15:00",
            "2021-02-20 15:30:00",
            "TX",
            "Houston",
            "Harris County",
            "Rain",
            55.0,
            85.0,
            29.5,
            2.0,
            15.0,
            0.5,
            "Day",
            "Day",
            "Day",
        ),
        (
            "A-3",
            1,
            "2021-03-10 22:45:00",
            "2021-03-10 23:00:00",
            "NY",
            "New York",
            "New York County",
            "Fog",
            45.0,
            90.0,
            30.1,
            0.5,
            3.0,
            0.0,
            "Night",
            "Night",
            "Night",
        ),
        (
            "A-4",
            4,
            "2022-06-05 18:30:00",
            "2022-06-05 20:00:00",
            "CA",
            "San Francisco",
            "San Francisco County",
            "Clear",
            70.0,
            50.0,
            29.9,
            10.0,
            8.0,
            0.0,
            "Day",
            "Day",
            "Day",
        ),
        (
            "A-5",
            2,
            "2022-12-25 06:00:00",
            "2022-12-25 06:30:00",
            "FL",
            "Miami",
            "Miami-Dade County",
            "Snow",
            None,
            None,
            None,
            5.0,
            10.0,
            1.0,
            "Day",
            "Day",
            "Day",
        ),
        # Registro con valores nulos para probar la limpieza
        (
            "A-6",
            None,
            None,
            "2023-01-01 10:00:00",
            None,
            "Chicago",
            "Cook County",
            "  rain  ",
            60.0,
            70.0,
            29.7,
            8.0,
            12.0,
            0.2,
            "Day",
            "Day",
            "Day",
        ),
    ]

    return spark.createDataFrame(data, schema)


class TestCreateSparkSession:
    """Tests para la función create_spark_session."""

    def test_spark_session_exists(self, spark):
        """Verifica que existe una sesión de Spark válida."""
        assert spark is not None
        assert spark.sparkContext is not None

    def test_spark_session_has_configurations(self, spark):
        """Verifica que las configuraciones de Spark están aplicadas."""
        config = spark.sparkContext.getConf()
        
        # Verifica que al menos algunas configuraciones importantes están presentes
        assert config.get("spark.master") == "local[2]"
        assert spark.sparkContext.appName == "ETL_Tests"


class TestExtractData:
    """Tests para la función extract_data."""

    def test_extract_data_creates_dataframe(self, spark, tmp_path):
        """Verifica que extract_data crea un DataFrame correctamente."""
        # Crear archivo CSV temporal
        csv_file = tmp_path / "test_data.csv"
        csv_file.write_text(
            "ID,Severity,Start_Time\n"
            "A-1,2,2025-01-15 08:30:00\n"
            "A-2,3,2025-02-20 14:15:00\n"
        )

        df = etl.extract_data(spark, str(csv_file))

        assert df is not None
        assert df.count() == 2
        assert "ID" in df.columns
        assert "Severity" in df.columns
        assert "Start_Time" in df.columns

    def test_extract_data_repartitions(self, spark, tmp_path):
        """Verifica que los datos se reparticionen correctamente."""
        csv_file = tmp_path / "test_data.csv"
        csv_file.write_text("ID,Severity\n" "A-1,2\n")

        df = etl.extract_data(spark, str(csv_file))

        # Verifica que tiene 16 particiones
        assert df.rdd.getNumPartitions() == 16


class TestTransformData:
    """Tests para la función transform_data."""

    def test_transform_data_adds_temporal_columns(self, sample_accidents_data):
        """Verifica que se agregan las columnas temporales correctamente."""
        df_transformed = etl.transform_data(sample_accidents_data)

        expected_columns = ["Year", "Month", "Hour", "DayOfWeek"]
        for col_name in expected_columns:
            assert col_name in df_transformed.columns

    def test_transform_data_adds_time_period(self, sample_accidents_data):
        """Verifica que se agrega la columna Time_Period correctamente."""
        df_transformed = etl.transform_data(sample_accidents_data)

        assert "Time_Period" in df_transformed.columns

        time_periods = (
            df_transformed.select("Time_Period")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        valid_periods = {"MORNING", "AFTERNOON", "EVENING", "NIGHT"}

        for period in time_periods:
            assert period in valid_periods

    def test_transform_data_adds_severity_category(self, sample_accidents_data):
        """Verifica que se agrega la columna Severity_Category correctamente."""
        df_transformed = etl.transform_data(sample_accidents_data)

        assert "Severity_Category" in df_transformed.columns

        categories = (
            df_transformed.select("Severity_Category")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        valid_categories = {"LOW", "MODERATE", "HIGH", "SEVERE", "UNKNOWN"}

        for category in categories:
            assert category in valid_categories

    def test_transform_data_filters_null_values(self, sample_accidents_data):
        """Verifica que se filtran correctamente los valores nulos."""
        df_transformed = etl.transform_data(sample_accidents_data)

        # Verifica que no hay nulos en Start_Time, State, City, Severity
        assert df_transformed.filter(col("Start_Time").isNull()).count() == 0
        assert df_transformed.filter(col("State").isNull()).count() == 0
        assert df_transformed.filter(col("City").isNull()).count() == 0
        assert df_transformed.filter(col("Severity").isNull()).count() == 0

    def test_transform_data_cleans_text_columns(self, sample_accidents_data):
        """Verifica que se limpian y normalizan las columnas de texto."""
        df_transformed = etl.transform_data(sample_accidents_data)

        # Verifica que los valores de texto estén en mayúsculas y sin espacios extras
        weather_values = (
            df_transformed.select("Weather_Condition")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )

        for value in weather_values:
            if value:
                assert value == value.upper()
                assert value == value.strip()

    def test_transform_data_handles_null_numeric_values(self, sample_accidents_data):
        """Verifica que se manejan correctamente los valores nulos en columnas numéricas."""
        df_transformed = etl.transform_data(sample_accidents_data)

        # Verifica que se reemplazan los nulos con medianas
        assert df_transformed.filter(col("Temperature(F)").isNull()).count() == 0
        assert df_transformed.filter(col("Humidity(%)").isNull()).count() == 0
        assert df_transformed.filter(col("Pressure(in)").isNull()).count() == 0

    def test_transform_data_converts_timestamp(self, sample_accidents_data):
        """Verifica que se convierten correctamente las columnas de timestamp."""
        df_transformed = etl.transform_data(sample_accidents_data)

        # Verifica que Start_Time y End_Time son del tipo timestamp
        assert dict(df_transformed.dtypes)["Start_Time"] == "timestamp"
        assert dict(df_transformed.dtypes)["End_Time"] == "timestamp"


class TestGenerateTemporalMetrics:
    """Tests para la función generate_temporal_metrics."""

    def test_generate_temporal_metrics_returns_dict(self, sample_accidents_data):
        """Verifica que se retorna un diccionario con las métricas temporales."""
        df_transformed = etl.transform_data(sample_accidents_data)
        metrics = etl.generate_temporal_metrics(df_transformed)

        assert isinstance(metrics, dict)
        assert "by_year" in metrics
        assert "by_month" in metrics
        assert "by_dayofweek" in metrics

    def test_generate_temporal_metrics_by_year(self, sample_accidents_data):
        """Verifica que se generan correctamente las métricas por año."""
        df_transformed = etl.transform_data(sample_accidents_data)
        metrics = etl.generate_temporal_metrics(df_transformed)

        by_year = metrics["by_year"]
        assert "Year" in by_year.columns
        assert "Total_Accidents" in by_year.columns
        assert by_year.count() > 0

    def test_generate_temporal_metrics_by_month(self, sample_accidents_data):
        """Verifica que se generan correctamente las métricas por mes."""
        df_transformed = etl.transform_data(sample_accidents_data)
        metrics = etl.generate_temporal_metrics(df_transformed)

        by_month = metrics["by_month"]
        assert "Month" in by_month.columns
        assert "Total_Accidents" in by_month.columns
        assert by_month.count() > 0

    def test_generate_temporal_metrics_by_dayofweek(self, sample_accidents_data):
        """Verifica que se generan correctamente las métricas por día de la semana."""
        df_transformed = etl.transform_data(sample_accidents_data)
        metrics = etl.generate_temporal_metrics(df_transformed)

        by_dayofweek = metrics["by_dayofweek"]
        assert "DayOfWeek" in by_dayofweek.columns
        assert "Total_Accidents" in by_dayofweek.columns
        assert by_dayofweek.count() > 0


class TestGenerateCategoricalMetrics:
    """Tests para la función generate_categorical_metrics."""

    def test_generate_categorical_metrics_returns_dict(self, sample_accidents_data):
        """Verifica que se retorna un diccionario con las métricas categóricas."""
        df_transformed = etl.transform_data(sample_accidents_data)
        metrics = etl.generate_categorical_metrics(df_transformed)

        assert isinstance(metrics, dict)
        assert "top_states" in metrics
        assert "top_cities" in metrics
        assert "top_weather" in metrics
        assert "severity_distribution" in metrics
        assert "time_period_distribution" in metrics

    def test_generate_categorical_metrics_top_states(self, sample_accidents_data):
        """Verifica que se generan correctamente las métricas de estados."""
        df_transformed = etl.transform_data(sample_accidents_data)
        metrics = etl.generate_categorical_metrics(df_transformed)

        top_states = metrics["top_states"]
        assert "State" in top_states.columns
        assert "Total_Accidents" in top_states.columns
        assert top_states.count() <= 10

    def test_generate_categorical_metrics_top_cities(self, sample_accidents_data):
        """Verifica que se generan correctamente las métricas de ciudades."""
        df_transformed = etl.transform_data(sample_accidents_data)
        metrics = etl.generate_categorical_metrics(df_transformed)

        top_cities = metrics["top_cities"]
        assert "City" in top_cities.columns
        assert "Total_Accidents" in top_cities.columns
        assert top_cities.count() <= 10

    def test_generate_categorical_metrics_severity_distribution(
        self, sample_accidents_data
    ):
        """Verifica que se genera correctamente la distribución de severidad."""
        df_transformed = etl.transform_data(sample_accidents_data)
        metrics = etl.generate_categorical_metrics(df_transformed)

        severity_distribution = metrics["severity_distribution"]
        assert "Severity_Category" in severity_distribution.columns
        assert "Total_Accidents" in severity_distribution.columns
        assert severity_distribution.count() > 0

    def test_generate_categorical_metrics_time_period_distribution(
        self, sample_accidents_data
    ):
        """Verifica que se genera correctamente la distribución por período del día."""
        df_transformed = etl.transform_data(sample_accidents_data)
        metrics = etl.generate_categorical_metrics(df_transformed)

        time_period_distribution = metrics["time_period_distribution"]
        assert "Time_Period" in time_period_distribution.columns
        assert "Total_Accidents" in time_period_distribution.columns
        assert time_period_distribution.count() > 0

    def test_generate_categorical_metrics_top_weather(self, sample_accidents_data):
        """Verifica que se generan correctamente las métricas de clima."""
        df_transformed = etl.transform_data(sample_accidents_data)
        metrics = etl.generate_categorical_metrics(df_transformed)

        top_weather = metrics["top_weather"]
        assert "Weather_Condition" in top_weather.columns
        assert "Total_Accidents" in top_weather.columns
        assert top_weather.count() <= 15


class TestLoadData:
    """Tests para la función load_data."""

    def test_load_data_creates_parquet_files(self, sample_accidents_data, tmp_path):
        """Verifica que se crean los archivos parquet correctamente."""
        df_transformed = etl.transform_data(sample_accidents_data)
        temporal_metrics = etl.generate_temporal_metrics(df_transformed)
        categorical_metrics = etl.generate_categorical_metrics(df_transformed)

        output_dir = str(tmp_path / "output")
        os.makedirs(output_dir, exist_ok=True)

        etl.load_data(temporal_metrics, categorical_metrics, output_dir)

        # Verifica que se crearon los directorios de parquet
        assert os.path.exists(os.path.join(output_dir, "accidents_by_year.parquet"))
        assert os.path.exists(os.path.join(output_dir, "accidents_by_month.parquet"))
        assert os.path.exists(os.path.join(output_dir, "top_states.parquet"))

    def test_load_data_files_are_readable(self, sample_accidents_data, tmp_path, spark):
        """Verifica que los archivos parquet creados son legibles."""
        df_transformed = etl.transform_data(sample_accidents_data)
        temporal_metrics = etl.generate_temporal_metrics(df_transformed)
        categorical_metrics = etl.generate_categorical_metrics(df_transformed)

        output_dir = str(tmp_path / "output")
        os.makedirs(output_dir, exist_ok=True)

        etl.load_data(temporal_metrics, categorical_metrics, output_dir)

        parquet_path = os.path.join(output_dir, "accidents_by_year.parquet")
        df_read = spark.read.parquet(f"file:///{parquet_path}")

        assert df_read is not None
        assert df_read.count() > 0


class TestMainFunction:
    """Tests para la función main."""

    def test_main_returns_zero_on_success(self, tmp_path, monkeypatch):
        """Verifica que main retorna 0 cuando se ejecuta exitosamente."""
        # Crear archivo CSV de prueba
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        csv_file = input_dir / "US_Accidents_March23.csv"
        csv_file.write_text(
            "ID,Severity,Start_Time,End_Time,State,City,County,Weather_Condition,"
            "Temperature(F),Humidity(%),Pressure(in),Visibility(mi),Wind_Speed(mph),"
            "Precipitation(in),Sunrise_Sunset,Civil_Twilight,Nautical_Twilight\n"
            "A-1,2,2021-01-15 08:30:00,2021-01-15 09:00:00,CA,Los Angeles,"
            "Los Angeles County,Clear,65.5,45.0,29.8,10.0,5.5,0.0,Day,Day,Day\n"
        )

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        # Simular argumentos de línea de comandos
        test_args = [
            "etl.py",
            "--input-dir",
            str(input_dir),
            "--output-dir",
            str(output_dir),
        ]
        monkeypatch.setattr(sys, "argv", test_args)

        result = etl.main()
        assert result == 0
