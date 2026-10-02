from pyspark.sql import SparkSession
from log import logs_summary, logs_detailed
from datetime import datetime
import bronze_config
import uuid


def bronze_process(spark):

    for item in bronze_config.ingestion_configuration:
        
        run_id = str(uuid.uuid4())
        start_time = datetime.now()
        end_time = None
        duration = None
        try:

            try:
                df = (
                    spark.read
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv(item["file_path"])
                )
                logs_detailed(spark, run_id, "EXTRACT", df.count(), "SUCCESS", f"Extract from source {item["source"]} to target table {item["target_table"]}")
            except Exception as e:
                logs_detailed(spark, run_id, "EXTRACT", df.count(), "FAILED", f"ERROR: {str(e)}")
                raise
            
            try:
                (
                    df.write
                    .mode("overwrite")
                    .saveAsTable(f"workspace.bronze.{item["target_table"]}")
                )
                logs_detailed(spark, run_id, "LOAD", df.count(), "SUCCESS", f"Load from source {item["source"]} to target table {item["target_table"]}")
            except Exception as e:
                logs_detailed(spark, run_id, "LOAD", df.count(), "FAILED", f"ERROR: {str(e)}")
                raise

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            print(f"PROCESS: INGESTION | TABLE NAME: {item["target_table"]} | STATUSL: SUCCESS")
            logs_summary(spark, run_id, item["source"], "BRONZE", item["target_table"], "EXTRACT", "SUCCESS", start_time, end_time, duration, None)

        except Exception as e:

            print(f"PROCESS: INGESTION | TABLE NAME: {item["target_table"]} | STATUSL: FAILED | ERROR: {str(e)}")
            logs_summary(spark, run_id, item["source"], "BRONZE", item["target_table"], "EXTRACT", "FAILED", start_time, end_time, duration, str(e))
            raise

