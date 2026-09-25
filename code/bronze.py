from pyspark.sql import SparkSession
import bronze_config


def bronze_process(spark):

    for item in bronze_config.ingestion_configuration:
        print(f"Ingesting {item["source"]} | bronze.{item["target_table"]}")
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(item["file_path"])
        )

        (
            df.write
            .mode("overwrite")
            .saveAsTable(f"workspace.bronze.{item["target_table"]}")
        )


