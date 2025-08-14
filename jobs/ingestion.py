from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import to_date, col, coalesce
from common import get_spark, clean_string_cols, drop_exact_duplicates, write_df_to_postgres

DATA_DIR = "/opt/app/data"

vendas_schema = StructType([
    StructField("DATA_VENDA", StringType(), True),
    StructField("COD_DISTRIBUICAO", IntegerType(), True),
    StructField("CLIENTE", IntegerType(), True),
    StructField("NM_VENDEDOR", StringType(), True),
    StructField("QT_VENDA", DoubleType(), True),
    StructField("VL_VENDA", DoubleType(), True),
])

dist_schema = StructType([
    StructField("COD_DISTRIBUICAO", IntegerType(), True),
    StructField("NM_DISTRIBUICAO", StringType(), True),
])

def main():
    spark = get_spark("ingestion")
    vendas = (
        spark.read.option("header", True).option("sep", ";").schema(vendas_schema).csv(f"{DATA_DIR}/vendas.csv")
        .withColumn("DATA_VENDA", coalesce(to_date(col("DATA_VENDA"), "yyyy-MM-dd"),
                                           to_date(col("DATA_VENDA"), "dd/MM/yyyy")))
    )
    dist = spark.read.option("header", True).option("sep", ";").schema(dist_schema).csv(f"{DATA_DIR}/distribuidores.csv")

    # Limpeza básica
    vendas = clean_string_cols(vendas)
    dist = clean_string_cols(dist)

    # Remover linhas sem chaves essenciais
    vendas = vendas.na.drop(subset=["DATA_VENDA", "COD_DISTRIBUICAO", "CLIENTE"])
    dist = dist.na.drop(subset=["COD_DISTRIBUICAO", "NM_DISTRIBUICAO"])

    # Duplicatas exatas
    vendas = drop_exact_duplicates(vendas, subset_cols=["DATA_VENDA", "COD_DISTRIBUICAO", "CLIENTE", "NM_VENDEDOR", "QT_VENDA", "VL_VENDA"])
    dist = drop_exact_duplicates(dist, subset_cols=["COD_DISTRIBUICAO", "NM_DISTRIBUICAO"])

    # Persistir no Postgres (tabelas base)
    write_df_to_postgres(vendas, "vendas", mode="overwrite")
    write_df_to_postgres(dist, "distribuidores", mode="overwrite")

    print("Ingestion concluída. Tabelas 'vendas' e 'distribuidores' prontas.")

if __name__ == "__main__":
    main()
