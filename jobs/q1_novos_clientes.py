from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common import get_spark, write_df_to_postgres, jdbc_options
import os

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/opt/app/output")

def main():
    spark = get_spark("q1-novos-clientes")

    vendas = spark.read.format("jdbc").options(**jdbc_options("vendas")).load()
    dist = spark.read.format("jdbc").options(**jdbc_options("distribuidores")).load()

    # Enriquecer com nome do distribuidor
    df = (
        vendas.join(F.broadcast(dist), "COD_DISTRIBUICAO", "left")
        .withColumn("ano", F.year("DATA_VENDA"))
        .withColumn("mes", F.month("DATA_VENDA"))
        .withColumn("ANO_MES", F.format_string("%04d|%02d", F.col("ano"), F.col("mes")))
    )

    # Primeiro mês em que o cliente apareceu no distribuidor
    w = Window.partitionBy("COD_DISTRIBUICAO", "CLIENTE")
    df_first = df.withColumn("primeira_data_cliente", F.min("DATA_VENDA").over(w))
    df_first = df_first.withColumn("ano_mes_primeira", F.date_format("primeira_data_cliente", "yyyy|MM"))

    # Registros de "novo cliente": primeira DATA_VENDA do cliente naquele distribuidor
    novos = (
        df_first
        .filter(F.date_format("DATA_VENDA", "yyyy|MM") == F.col("ano_mes_primeira"))
        .select("NM_DISTRIBUICAO", "CLIENTE", "ANO_MES")
        .dropDuplicates(["NM_DISTRIBUICAO", "CLIENTE", "ANO_MES"])
    )

    # Pivot: colunas = ANO_MES, valor = 1 (presença)
    novos_flag = novos.withColumn("flag", F.lit(1))
    pivoted = (
        novos_flag
        .groupBy("NM_DISTRIBUICAO", "CLIENTE")
        .pivot("ANO_MES")
        .agg(F.max("flag"))
        .na.fill(0)
    )

    # Salvar parquet
    out_parquet = f"{OUTPUT_DIR}/q1_novos_clientes_pivot"
    pivoted.write.mode("overwrite").parquet(out_parquet)

    # Gravar no Postgres (tabela pivot)
    write_df_to_postgres(pivoted, "novos_clientes", mode="overwrite")

    print(f"Q1 concluído. Parquet em {out_parquet} e tabela 'novos_clientes' atualizada.")

if __name__ == "__main__":
    main()
