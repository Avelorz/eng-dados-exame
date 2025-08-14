from pyspark.sql import SparkSession
from common import get_spark, jdbc_options
import os

spark = get_spark("data_check")

v = spark.read.format("jdbc").options(**jdbc_options("vendas")).load()
d = spark.read.format("jdbc").options(**jdbc_options("distribuidores")).load()

v.show()
d.show()
v.selectExpr("min(DATA_VENDA) as min_d", "max(DATA_VENDA) as max_d").show(truncate=False)
v.selectExpr("year(DATA_VENDA) as ano", "month(DATA_VENDA) as mes").distinct().orderBy("ano","mes").show(20, False)
