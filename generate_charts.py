from pyspark.sql import SparkSession
import os
import matplotlib.pyplot as plt
import seaborn as sns


def generate_charts(output_dir="output"):
    if not os.path.exists(output_dir):
        print(f"Error: El directorio {output_dir} no existe")
        return

    spark = (
        SparkSession.builder.appName("Results Analysis")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )

    print("\n" + "=" * 80)
    print("GENERANDO GRÁFICOS")
    print("=" * 80)

    for item in os.listdir(output_dir):
        item_path = os.path.join(output_dir, item)

        if not os.path.isdir(item_path) or not item.endswith(".parquet"):
            continue

        print(f"\nProcesando: {item}")

        try:
            df = spark.read.parquet(item_path)
            dataset = df.toPandas()

            if dataset.empty:
                print(f"El archivo {item} está vacío")
                continue

            plt.figure(figsize=(12, 6))

            x_col = dataset.columns[0]
            y_col = dataset.columns[1]

            sns.barplot(data=dataset, x=x_col, y=y_col, color="steelblue")
            title = item.replace("_", " ").replace(".parquet", "").title()
            plt.title(title, fontsize=14, fontweight="bold")
            plt.xlabel(x_col.replace("_", " ").title(), fontsize=12)
            plt.ylabel(y_col.replace("_", " ").title(), fontsize=12)
            plt.xticks(rotation=45, ha="right")
            plt.grid(axis="y", alpha=0.3)
            plt.tight_layout()

            output_path = os.path.join(output_dir, item.replace(".parquet", ".png"))
            plt.savefig(output_path, dpi=150, bbox_inches="tight")
            plt.close()

            print(f"Gráfico guardado: {item.replace('.parquet', '.png')}")

        except Exception as e:
            print(f"Error procesando {item}: {str(e)}")
            continue

    spark.stop()


if __name__ == "__main__":
    generate_charts()
