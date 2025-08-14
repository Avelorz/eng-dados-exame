from pyspark.sql import functions as F
from common import get_spark, write_df_to_postgres, jdbc_options
import os

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/opt/app/output")

def main():
    spark = get_spark("q2-dias-vendas")

    vendas = spark.read.format("jdbc").options(**jdbc_options("vendas")).load()
    dist = spark.read.format("jdbc").options(**jdbc_options("distribuidores")).load()

    df = (
        vendas
        .join(F.broadcast(dist), "COD_DISTRIBUICAO", "left")
        .withColumn("ano", F.year("DATA_VENDA"))
        .withColumn("mes", F.month("DATA_VENDA"))
        .withColumn("dia", F.dayofmonth("DATA_VENDA"))
        .withColumn("ano_mes", F.format_string("%04d|%02d", F.col("ano"), F.col("mes")))
    )

    # Considera "dia com venda" quando houve quantidade OU valor > 0
    daily = (
        df.groupBy("NM_DISTRIBUICAO", "ano", "mes", "dia")
        .agg(F.sum("QT_VENDA").alias("sum_qt"),
             F.sum("VL_VENDA").alias("sum_vl"))
        .withColumn("tem_venda", (F.col("sum_qt") > 0) | (F.col("sum_vl") > 0))
    )

    dias_com_venda = (
        daily.filter(F.col("tem_venda"))
        .groupBy("NM_DISTRIBUICAO", "ano", "mes")
        .agg(F.countDistinct("dia").alias("dias_com_venda"))
    )

    # Totais dias no mês
    months = df.select("ano", "mes").distinct()
    month_dims = (
        months
        .withColumn("primeiro_dia",F.make_date(F.col("ano"), F.col("mes"), F.lit(1)))
        .withColumn("dias_totais", F.dayofmonth(F.last_day(F.col("primeiro_dia"))))
        .select("ano", "mes", "dias_totais")
    )

    # Destribuido x mes ano.
    dist_month = df.select("NM_DISTRIBUICAO", "ano", "mes").distinct()
    base = dist_month.join(month_dims, ["ano", "mes"], "left")

    # Juntar contagem de dias com dias de venda
    final = (
        base.join(dias_com_venda, ["NM_DISTRIBUICAO", "ano", "mes"], "left")
        .withColumn("dias_com_venda", F.coalesce(F.col("dias_com_venda"), F.lit(0)))
        .withColumn("dias_sem_venda", F.col("dias_totais") - F.col("dias_com_venda"))
        .withColumn("ano_mes", F.format_string("%04d|%02d", F.col("ano"), F.col("mes")))
        .select("NM_DISTRIBUICAO", "ano_mes", "dias_totais", "dias_com_venda", "dias_sem_venda")
        .dropDuplicates()
    )

    # Versão “bonita” só para o CSV
    final_csv = final.selectExpr(
        "NM_DISTRIBUICAO as Distribuidor",
        "ano_mes as `Ano|Mês`",
        "dias_totais as `Dias Totais`",
        "dias_com_venda as `Dias com Venda`",
        "dias_sem_venda as `Dias sem Venda`"
    )

    # Salvar CSV com separador ; e delimitador (aspas)
    out_csv = f"{OUTPUT_DIR}/q2_dias_vendas_distribuidores"
    (
        final_csv.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .option("sep", ";")
        .option("quote", "\"")
        .option("quoteAll", True)
        .csv(out_csv)
    )

    # Tabela no Postgres
    write_df_to_postgres(final, "dias_vendas_distribuidores", mode="overwrite")

    print(f"Q2 concluído. CSV em {out_csv} e tabela 'dias_vendas_distribuidores' atualizada.")

if __name__ == "__main__":
    main()
