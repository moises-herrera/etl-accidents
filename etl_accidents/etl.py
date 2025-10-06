import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    to_timestamp,
    year,
    month,
    dayofweek,
    hour,
    when,
    trim,
    upper,
    desc,
)
import kagglehub


def create_spark_session(app_name="US_Accidents_ETL"):
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.files.maxPartitionBytes", "128MB")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.default.parallelism", "8")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.memory.fraction", "0.8")
        .config("spark.memory.storageFraction", "0.3")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "512m")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def extract_data(spark, input_path):
    print("\n" + "=" * 80)
    print("FASE 1: EXTRACCIÓN DE DATOS")
    print("=" * 80)
    print(f"Leyendo datos desde: {input_path}")

    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
    )

    df = df.repartition(16)
    df.cache()

    print(f"Número de particiones: {df.rdd.getNumPartitions()}")

    total_rows = df.count()
    total_cols = len(df.columns)
    print(f"Datos extraídos exitosamente")
    print(f"Filas: {total_rows:,}")
    print(f"Columnas: {total_cols}")

    return df


def transform_data(df):
    print("\n" + "=" * 80)
    print("FASE 2: TRANSFORMACIÓN DE DATOS")
    print("=" * 80)

    # Conversion de tipos de datos
    df_transformed = df.withColumn(
        "Start_Time", to_timestamp(col("Start_Time"))
    ).withColumn("End_Time", to_timestamp(col("End_Time")))

    # Limpieza de nulos y valores inconsistentes
    df_transformed = df_transformed.filter(col("Start_Time").isNotNull())
    df_transformed = df_transformed.filter(
        col("State").isNotNull() & col("City").isNotNull()
    )
    df_transformed = df_transformed.filter(col("Severity").isNotNull())

    # Limpiar valores de texto (trim y uppercase para consistencia)
    text_columns = [
        "State",
        "City",
        "County",
        "Weather_Condition",
        "Sunrise_Sunset",
        "Civil_Twilight",
        "Nautical_Twilight",
    ]

    for col_name in text_columns:
        if col_name in df_transformed.columns:
            df_transformed = df_transformed.withColumn(
                col_name, trim(upper(col(col_name)))
            )

    # Reemplazar valores nulos en columnas numéricas con la mediana/valor por defecto
    number_columns = df_transformed.select(
        [
            col_name
            for col_name, dtype in df_transformed.dtypes
            if dtype in ("double", "int", "float")
        ]
    ).columns

    for col_name in number_columns:
        median_value = df_transformed.approxQuantile(col_name, [0.5], 0.01)[0]
        df_transformed = df_transformed.withColumn(
            col_name,
            when(col(col_name).isNull(), median_value).otherwise(col(col_name)),
        )

    df_transformed = (
        df_transformed.withColumn("Year", year(col("Start_Time")))
        .withColumn("Month", month(col("Start_Time")))
        .withColumn("Hour", hour(col("Start_Time")))
        .withColumn("DayOfWeek", dayofweek(col("Start_Time")))
    )

    df_transformed = df_transformed.withColumn(
        "Time_Period",
        when((col("Hour") >= 6) & (col("Hour") < 12), "MORNING")
        .when((col("Hour") >= 12) & (col("Hour") < 18), "AFTERNOON")
        .when((col("Hour") >= 18) & (col("Hour") < 22), "EVENING")
        .otherwise("NIGHT"),
    )

    df_transformed = df_transformed.withColumn(
        "Severity_Category",
        when(col("Severity") == 1, "LOW")
        .when(col("Severity") == 2, "MODERATE")
        .when(col("Severity") == 3, "HIGH")
        .when(col("Severity") == 4, "SEVERE")
        .otherwise("UNKNOWN"),
    )

    df_transformed = df_transformed.repartition(16, "Year", "State")
    df_transformed.cache()

    rows_after_cleaning = df_transformed.count()
    print(f"Filas después de limpieza: {rows_after_cleaning:,}")

    return df_transformed


def generate_temporal_metrics(df):
    accidents_by_year = (
        df.groupBy("Year").agg(count("*").alias("Total_Accidents")).orderBy("Year")
    )

    accidents_by_month = (
        df.groupBy("Month").agg(count("*").alias("Total_Accidents")).orderBy("Month")
    )

    accidents_by_dayofweek = (
        df.groupBy("DayOfWeek")
        .agg(count("*").alias("Total_Accidents"))
        .orderBy("DayOfWeek")
    )

    return {
        "by_year": accidents_by_year,
        "by_month": accidents_by_month,
        "by_dayofweek": accidents_by_dayofweek,
    }


def generate_categorical_metrics(df):
    # Top 10 estados con más accidentes
    top_states = (
        df.groupBy("State")
        .agg(
            count("*").alias("Total_Accidents"),
        )
        .orderBy(desc("Total_Accidents"))
        .limit(10)
    )

    # Top 10 ciudades con más accidentes
    top_cities = (
        df.groupBy("City")
        .agg(
            count("*").alias("Total_Accidents"),
        )
        .orderBy(desc("Total_Accidents"))
        .limit(10)
    )

    # Top condiciones climáticas
    top_weather = (
        df.filter(col("Weather_Condition").isNotNull())
        .groupBy("Weather_Condition")
        .agg(count("*").alias("Total_Accidents"))
        .orderBy(desc("Total_Accidents"))
        .limit(15)
    )

    # Distribución por severidad
    severity_distribution = (
        df.groupBy("Severity_Category")
        .agg(count("*").alias("Total_Accidents"))
        .orderBy(desc("Total_Accidents"))
    )

    # Accidentes por período del día
    time_period_distribution = (
        df.groupBy("Time_Period")
        .agg(count("*").alias("Total_Accidents"))
        .orderBy(desc("Total_Accidents"))
    )

    return {
        "top_states": top_states,
        "top_cities": top_cities,
        "top_weather": top_weather,
        "severity_distribution": severity_distribution,
        "time_period_distribution": time_period_distribution,
    }



def load_data(temporal_metrics, categorical_metrics, output_dir):
    print("\n" + "=" * 80)
    print("FASE 3: CARGA DE RESULTADOS")
    print("=" * 80)
    path = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(path, output_dir)

    for key, df in temporal_metrics.items():
        output_path = os.path.join(output_dir, f"accidents_{key}.parquet")
        df.write.mode("overwrite").parquet(f"file:///{output_path}")

    for key, df in categorical_metrics.items():
        output_path = os.path.join(output_dir, f"{key}.parquet")
        df.write.mode("overwrite").parquet(f"file:///{output_path}")

    print(f"\nTodos los archivos guardados en: {output_dir}")


def main():
    parser = argparse.ArgumentParser(
        description="ETL para análisis de accidentes de tráfico en EE.UU."
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        default="input/",
        help="Directorio con el archivo CSV de entrada",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="output/",
        help="Directorio para guardar los resultados",
    )

    args = parser.parse_args()

    input_path = os.path.join(args.input_dir, "US_Accidents_March23.csv")
    
    if not os.path.exists(input_path) or os.path.getsize(input_path) == 0:
        path = kagglehub.dataset_download("sobhanmoosavi/us-accidents", "US_Accidents_March23.csv")
        input_path = os.path.dirname(path)

    print("\n" + "=" * 80)
    print("ETL - ANÁLISIS DE ACCIDENTES DE TRÁFICO EN EE.UU.")
    print("=" * 80)

    try:
        spark = create_spark_session()

        df_raw = extract_data(spark, input_path)

        df_transformed = transform_data(df_raw)

        print("\nGENERANDO MÉTRICAS...")

        temporal_metrics = generate_temporal_metrics(df_transformed)

        categorical_metrics = generate_categorical_metrics(df_transformed)

        print("\nMÉTRICAS GENERADAS")

        output_abs_path = os.path.abspath(args.output_dir)
        load_data(temporal_metrics, categorical_metrics, output_abs_path)

        print("\n" + "=" * 80)
        print("ETL COMPLETADO EXITOSAMENTE")
        print("=" * 80)

        df_transformed.unpersist()
        df_raw.unpersist()
        spark.stop()

        return 0

    except Exception as e:
        print(f"\nError durante la ejecución del ETL: {str(e)}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
