import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim


# Spark Build
def get_spark(app_name: str = "DataEngineerExam"):
    warehouse = os.getenv("APP_WAREHOUSE_DIR", "/opt/app/warehouse")
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

# JDBC Config
def jdbc_options(table: str) -> dict:
    url = os.getenv("JDBC_URL") or f"jdbc:postgresql://{os.getenv('DB_HOST', 'postgres')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'vendas_db')}"
    props = {
        "url": url,
        "driver": "org.postgresql.Driver",
        "dbtable": table,
        "user": os.getenv("DB_USER", "app_user"),
        "password": os.getenv("DB_PASSWORD", "app_password"),
    }
    return props

# Write Data
def write_df_to_postgres(df, table: str, mode: str = "truncate"):
    opts = jdbc_options(table)
    writer = df.write.format("jdbc").options(**opts)
    if mode == "overwrite":
        writer = writer.option("truncate", "true")
    writer.mode(mode).save()

# Tratamentos Simples
def clean_string_cols(df):
    str_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() == "string"]
    for c in str_cols:
        df = df.withColumn(c, trim(col(c)))
    return df

def drop_exact_duplicates(df, subset_cols=None):
    return df.dropDuplicates(subset=subset_cols)
