from pyspark.sql import SparkSession
from log import logs_summary
from datetime import datetime
import bronze_config
import uuid


def bronze_process(spark):

    for item in bronze_config.ingestion_configuration:
        
        run_id = str(uuid.uuid4())
        start_time = datetime.now()

        try:

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

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            print(f"PROCESS: INGESTION | TABLE NAME: {item["target_table"]} | STATUSL: SUCCESS")
            logs_summary(spark, run_id, item["source"], "BRONZE", item["target_table"], "EXTRACT", "SUCCESS", start_time, end_time, duration, None)

        except Exception as e:

            print(f"PROCESS: INGESTION | TABLE NAME: {item["target_table"]} | STATUSL: FAILED | ERROR: {str(e)}")
            logs_summary(spark, run_id, item["source"], "BRONZE", item["target_table"], "EXTRACT", "FAILED", start_time, end_time, duration, str(e))
            raise

