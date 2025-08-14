from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common import get_spark, write_df_to_postgres, jdbc_options
import os

def rank_and_take(df, part_cols, order_col, top_n=10):
    w = Window.partitionBy(*part_cols).orderBy(F.col(order_col).desc(), F.col("Vendedor").asc())
    return (
        df.withColumn("Rank", F.row_number().over(w))
        .filter(F.col("Rank") <= top_n)
    )

def main():
    spark = get_spark("q3-top-vendedores")

    vendas = spark.read.format("jdbc").options(**jdbc_options("vendas")).load()
    dist = spark.read.format("jdbc").options(**jdbc_options("distribuidores")).load()

    df = (
        vendas
        .join(F.broadcast(dist), "COD_DISTRIBUICAO", "left")
        .withColumn("Ano", F.year("DATA_VENDA"))
        .withColumn("Mes", F.month("DATA_VENDA"))
        .withColumn("Quarter", F.quarter("DATA_VENDA"))
        .withColumn("Vendedor", F.col("NM_VENDEDOR"))
    )

    # Agregações por período
    mensal = (
        df.groupBy("NM_DISTRIBUICAO", "Mes", "Vendedor")
        .agg(F.round(F.sum("VL_VENDA"), 2).alias("Valor"))
    )
    trimestral = (
        df.groupBy("NM_DISTRIBUICAO", "Quarter", "Vendedor")
        .agg(F.round(F.sum("VL_VENDA"), 2).alias("Valor"))
    )
    anual = (
        df.groupBy("NM_DISTRIBUICAO", "Ano", "Vendedor")
        .agg(F.round(F.sum("VL_VENDA"), 2).alias("Valor"))
    )

    # Ranking top 10
    top_mensal = rank_and_take(mensal, ["Mes"], "Valor", top_n=10) \
        .select("NM_DISTRIBUICAO", "Mes", "Vendedor", F.round("Valor", 2).alias("Valor"), "Rank")
    top_trimestral = rank_and_take(trimestral, ["Quarter"], "Valor", top_n=10) \
        .select("NM_DISTRIBUICAO", "Quarter", "Vendedor", F.round("Valor", 2).alias("Valor"), "Rank")
    top_anual = rank_and_take(anual, ["Ano"], "Valor", top_n=10) \
        .select("NM_DISTRIBUICAO", "Ano", "Vendedor", F.round("Valor", 2).alias("Valor"), "Rank")

    # Gravar tabelas
    write_df_to_postgres(top_mensal, "ranking_mensal", mode="overwrite")
    write_df_to_postgres(top_trimestral, "ranking_trimestral", mode="overwrite")
    write_df_to_postgres(top_anual, "ranking_anual", mode="overwrite")

    print("Q3 concluído. Tabelas 'ranking_mensal', 'ranking_trimestral' e 'ranking_anual' atualizadas.")

if __name__ == "__main__":
    main()
