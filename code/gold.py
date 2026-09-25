import gold_config

def gold_process(spark):

    for item in gold_config.business_configuration:
        print(f"{item['table_name']}")
        try:
            spark.sql(
                f"""
                CREATE OR REPLACE TABLE {item['table_name']}
                AS
                SELECT * FROM (
                    {item['transformation']}
                )
                """
            )
            print(f"Gold Process | SUCCESS | {item['table_name']}")
        except Exception as e:
            print(f"Gold Process | FAILED | {item['table_name']}")
            raise

