from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from datetime import datetime
import silver_config





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

    for item in silver_config.transformation_config:
    
        print(f">>>TABLE NAME: {item['target_table']}")

        # -----------------------------------------
        # Extract + Transform
        # -----------------------------------------
        bronze_transformed_df = spark.sql(item["transformation"])
        # -----------------------------------------
        # Load Methods:
        # 1. SCD type 2
        # 2. Overwrite
        # -----------------------------------------
        # SCD tpye 2 ------------------------------
        if item["load_type"] == "scd2":
            print(">>>LOAD TYPE: SCD TYPE 2")
            # -----------------------------------------
            # Add hash
            # -----------------------------------------
            try:
                bronze_transformed_df = add_hash(
                    bronze_transformed_df,item["compare_columns"])

                scd_two(
                    item["target_table"],
                    bronze_transformed_df,
                    item["table_key"]
                )
                print(f">>>STATUS: SUCCESS")
                print("------------------------------------")
            except Exception:
                print(f">>>STATUS: FAILED")
                raise


        elif item["load_type"] == "overwrite":
            print(">>>LOAD TYPE: OVERWRTIE")
            try:
                overwrite(
                    item["target_table"],
                    bronze_transformed_df
                )
                print(f">>>STATUS: SUCCESS")
                print("------------------------------------")
            except Exception:
                print(f">>>STATUS: FAILED")
                raise



def scd_two(target_table, bronze_transformed_df, table_key):
    
    # incoming_df - Is a data comming from bronze layer
    # existing_df - Is a data that is already in the silver layer

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
    # 1. NEW RECORDS
    # --------------------------------------------------
    new_df = (
        joined_df
        .filter(F.col(f"b.{table_key}").isNull())
        .select("a.*")
        .withColumn("is_active", F.lit(True))
        .withColumn("date_activated", F.current_timestamp())
        .withColumn("date_deactivated", F.lit(None).cast("timestamp"))
    )

    if not new_df.isEmpty():
        print(">>>>>NEW RECORDS")
        new_df.write.mode("append").saveAsTable(target_table)

    # --------------------------------------------------
    # 2. CHANGED RECORDS
    # --------------------------------------------------
    changed_df = joined_df.filter(
        (F.col(f"b.{table_key}").isNotNull()) &
        (F.col("a.hash_code").isNotNull()) &
        (F.col("a.hash_code") != F.col("b.hash_code"))
    )

    if not changed_df.isEmpty():
        print(">>>>>CHANGED RECORDS")
        target = DeltaTable.forName(
                spark,
                target_table
        )
        # 1. UPDATE 
        print(">>>UPDATE THE OLD RECORD")
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

        # 2. INSERT 
        new_version = (
            changed_df.select("a.*").alias("a")
            .withColumn("is_active", F.lit(True))
            .withColumn("date_activated", F.current_timestamp())
            .withColumn("date_deactivated", F.lit(None).cast("timestamp"))
        )
        
        if not new_version.isEmpty():
            print(">>>INSERT NEW")
            new_version.write.format("delta").mode("append").saveAsTable(target_table)
        # -------------------------------------------------------------------------

    # --------------------------------------------------
    # UNCHANGED
    # -------------------------------------------------
    # unchanged_df = joined_df.filter(
    #     F.col("a.hash_code") == F.col("b.hash_code")
    # )

    # if not unchanged_df.isEmpty():
    #     print(">>>>>NO NEW AND CHANGED RECORDS")
    


def overwrite(target_table, transformation):

    transformation.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(target_table)

    print(f">>>>>Overwrite completed: {target_table}")