from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from datetime import datetime
from silver_config import transformation_config
from log import logs_summary, logs_detailed
from datetime import datetime
import uuid


def add_hash(df, compare_columns):

    return df.withColumn(
        "hash_code",
        F.sha2(
            F.concat_ws(
                "||",
                *[
                    F.coalesce(
                        F.col(column).cast("string"),
                        F.lit("")
                    )
                    for column in compare_columns
                ]
            ),
            256
        )
    )


def silver_process(spark):

    for item in transformation_config:
    
        print(f">>>TABLE NAME: {item['target_table']}")

        run_id = str(uuid.uuid4())
        start_time = datetime.now()

        # -----------------------------------------
        # Extract + Transform
        # -----------------------------------------
        bronze_transformed_df = spark.sql(item["transformation"])
        logs_detailed(spark, run_id, "TRANSFORM", bronze_transformed_df.count(), "SUCCESS", f"Transform/Cleaned delta table {item["target_table"]}")
        # -----------------------------------------
        # Load Methods:
        # 1. SCD type 2
        # 2. Overwrite
        # -----------------------------------------
        # SCD tpye 2 ------------------------------
        logs_detailed(spark, run_id, "Detect load type", bronze_transformed_df.count(), "SUCCESS", f"Identifying load type")
        if item["load_type"] == "scd2":
            logs_detailed(spark, run_id, "Load type detected", bronze_transformed_df.count(), "SUCCESS", f"Load type {item["load_type"]}")
            print(">>>LOAD TYPE: SCD TYPE 2")
            # -----------------------------------------
            # Add hash
            # -----------------------------------------
            try:
                bronze_transformed_df = add_hash(
                    bronze_transformed_df,item["compare_columns"])
                logs_detailed(spark, run_id, "ADD HASH CODE", bronze_transformed_df.count(), "SUCCESS", f"Add Hash code delta table {item["target_table"]}")

                scd_two(
                    spark,
                    run_id,
                    item["target_table"],
                    bronze_transformed_df,
                    item["table_key"]
                )

                print(f"PROCESS: TRANSFORMATION | TABLE NAME: {item["target_table"]} | STATUSL: SUCCESS")
                print("------------------------------------")

                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()

                logs_summary(spark, run_id, item["source_table"], "SILVER", item["target_table"], "TRANSFORMATION", "SUCCESS", start_time, end_time, duration, None)

            except Exception as e:

                print(f"PROCESS: TRANSFORMATION | TABLE NAME: {item["target_table"]} | STATUSL: FAILED")

                logs_summary(spark, run_id, item["source_table"], "SILVER", item["target_table"], "TRANSFORMATION", "FAILED", start_time, end_time, duration, str(e))
                raise


        elif item["load_type"] == "overwrite":
            logs_detailed(spark, run_id, "Load type detected", bronze_transformed_df.count(), "SUCCESS", f"Load type {item["load_type"]}")
            print(">>>LOAD TYPE: OVERWRTIE")
            try:
                overwrite(
                    spark,
                    run_id,
                    item["target_table"],
                    bronze_transformed_df
                )
                print(f">>>STATUS: SUCCESS")
                print("------------------------------------")
            except Exception:
                print(f">>>STATUS: FAILED")
                raise



def scd_two(spark, run_id, target_table, bronze_transformed_df, table_key):
    
    # incoming_df - Is a data comming from bronze layer
    # existing_df - Is a data that is already in the silver layer
    changes = False

    silver_transformed_df = spark.sql(f"select * from {target_table}")

    joined_df = (
        bronze_transformed_df.alias("a")
        .join(
            silver_transformed_df.alias("b"),
            (F.col(f"a.{table_key}") == F.col(f"b.{table_key}")) &
            (F.col("b.is_active") == True),
            "left"
        )
    )


    # --------------------------------------------------
    # 2. CHANGED RECORDS
    # --------------------------------------------------
    changed_df = (
        joined_df.filter(
            (F.col(f"b.{table_key}").isNotNull()) &
            (F.col("a.hash_code").isNotNull()) &
            (F.col("a.hash_code") != F.col("b.hash_code"))
        )
        .select("a.*")
    )

    changed_count = changed_df.count() 
    print(f"changed_df count: {changed_count}")

    if not changed_df.isEmpty():
        changes = True

        # # 2. INSERT 
        # new_version = (
        #     changed_df
        #     .withColumn("is_active", F.lit(True))
        #     .withColumn("date_activated", F.current_timestamp())
        #     .withColumn("date_deactivated", F.lit(None).cast("timestamp"))
        # )
        
        # new_count = new_version.count()
        # print(f"new version: {new_count}")

        # -------------------------------------------------------------------------

        logs_detailed(spark, run_id, "EXPIRE DATA", changed_df.count(), "SUCCESS", f"Expired data table {target_table}")
        print(">>>>>CHANGED RECORDS")
        target = DeltaTable.forName(
                spark,
                target_table
        )

        # 1. UPDATE 
        print("---UPDATE OLD RECORD (is_active = false, date-deactivated = timestamp())")
        (
            target.alias("b").merge(changed_df.select("a.*").alias("a")
            ,f"""b.{table_key} = a.{table_key} and b.is_active = true """
            ).whenMatchedUpdate(
                set = {
                    "is_active": "false",
                    "date_deactivated": "current_timestamp()"
                }
            ).execute()
        )
        
    # --------------------------------------------------
    # 1. NEW RECORDS
    # --------------------------------------------------
    new_df = (
        joined_df
        .filter(
            (F.col(f"b.{table_key}").isNull()) |
           (F.col(f"a.hash_code") != F.col(f"b.hash_code"))
        )
        .select("a.*")
        .withColumn("is_active", F.lit(True))
        .withColumn("date_activated", F.current_timestamp())
        .withColumn("date_deactivated", F.lit(None).cast("timestamp")
        )
    )

    print(f"new_df: {new_df.count()}")

    if not new_df.isEmpty():
        changes = True
        logs_detailed(spark, run_id, "INSERT NEW DATA", new_df.count(), "SUCCESS", f"Newly data inserted to {target_table}")
        print(">>>>>NEW RECORDS")
        new_df.write.mode("append").saveAsTable(target_table)

    # --------------------------------------------------
    # UNCHANGED
    # -------------------------------------------------
    # unchanged_df = joined_df.filter(
    #     F.col("a.hash_code") == F.col("b.hash_code")
    # )

    # if not unchanged_df.isEmpty():
    #     print(">>>>>NO NEW AND CHANGED RECORDS")
    if changes == False:
        logs_detailed(spark, run_id, "DATA STATUS", new_df.count(), "SUCCESS", f"No changes for delta table {target_table}")
        print(">>>>>NO CHANGES")
    


def overwrite(spark, run_id, target_table, transformation):

    transformation.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(target_table)

    logs_detailed(spark, run_id, "Overwrite data", transformation.count(), "SUCCESS", f"Overwrite delta table {target_table}")
    print(f">>>>>Overwrite completed: {target_table}")