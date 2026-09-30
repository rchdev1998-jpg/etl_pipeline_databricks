import gold_config
import uuid
from datetime import datetime
from log import logs_summary

def gold_process(spark):

    for item in gold_config.business_configuration:

        try:

            run_id = str(uuid.uuid4())
            start_time = datetime.now()

            spark.sql(
                f"""
                CREATE OR REPLACE TABLE {item['table_name']}
                AS
                SELECT * FROM (
                    {item['transformation']}
                )
                """
            )

            end_time = datetime.now()
            durations = (end_time - start_time).total_seconds()

            print(f"PROCESS: TRANSFORMATION | TABLE NAME: {item["table_name"]} | STATUSL: SUCCESS")
            logs_summary(spark, run_id, None, "GOLD", item["table_name"], "TRANSFORMATION", "SUCCESS", start_time, end_time, durations, None)

        except Exception as e:

            print(f"PROCESS: TRANSFORMATION | TABLE NAME: {item["table_name"]} | STATUSL: FAILED | ERROR: {str(e)}")
            logs_summary(spark, run_id, None, "GOLD", item["table_name"], "TRANSFORMATION", "FAILED", start_time, end_time, durations, str(e))
            raise

