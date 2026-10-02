def logs_summary(spark, run_id, source, schema, table_name, process, status, start_time, end_time, duration, message):
    try:
        spark.sql(
            f"""
                INSERT INTO workspace.logs.etl_logs_summary (
                    log_run_id,
                    log_source,
                    log_schema,
                    log_table_name,
                    log_process,
                    log_status,
                    log_start_time,
                    log_end_time,
                    log_duration,
                    log_message
                )
                VALUES (
                    :run_id,
                    :log_source,
                    :log_schema,
                    :table_name,
                    :process,
                    :status,
                    :start_time,
                    :end_time,
                    :duration,
                    :message
                )
                """, args = {
                    "run_id": run_id,
                    "log_source": source,
                    "log_schema": schema,
                    "table_name": table_name,
                    "process": process,
                    "status": status,
                    "start_time": start_time,
                    "end_time": end_time,
                    "duration": duration,
                    "message": message
                }
        )
    except Exception as e:

        print(f"etl_logs_summary | CRITICAL ERROR: {str(e)}")

def logs_detailed(spark, run_id, steps, rows, status, message):

    try:
        spark.sql(
            f"""
            INSERT INTO workspace.logs.etl_logs_detailed (
                log_run_id,
                log_steps,
                log_no_rows,
                log_status,
                log_message
            )
            VALUES (
                :run_id,
                :steps,
                :log_rows,
                :status,
                :message
            )  
            """, args = {
                "run_id": run_id,
                "steps": steps,
                "log_rows": rows,
                "status": status,
                "message": message
            }
        )
    except Exception as e:

        print(f"etl_logs_detailed | CRITICAL ERROR: {str(e)}")
        raise




























