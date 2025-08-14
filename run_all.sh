#!/usr/bin/env bash
set -euo pipefail

export JDBC_URL="jdbc:postgresql://${DB_HOST:-postgres}:${DB_PORT:-5432}/${DB_NAME:-vendas_db}"
export JDBC_USER="${DB_USER:-app_user}"
export JDBC_PASSWORD="${DB_PASSWORD:-app_password}"
export PACKAGES="${JDBC_PACKAGES:-org.postgresql:postgresql:42.7.3}"

echo "===> Ingestion"
/opt/bitnami/spark/bin/spark-submit --packages "$PACKAGES" --master ${SPARK_MASTER:-local[*]} jobs/ingestion.py

echo "===> Q1 Novos Clientes"
/opt/bitnami/spark/bin/spark-submit --packages "$PACKAGES" --master ${SPARK_MASTER:-local[*]} jobs/q1_novos_clientes.py

echo "===> Q2 Dias de Vendas"
/opt/bitnami/spark/bin/spark-submit --packages "$PACKAGES" --master ${SPARK_MASTER:-local[*]} jobs/q2_dias_vendas.py

echo "===> Q3 Top Vendedores"
/opt/bitnami/spark/bin/spark-submit --packages "$PACKAGES" --master ${SPARK_MASTER:-local[*]} jobs/q3_top_vendedores.py

echo "===> Q4 Check dos Dados"
/opt/bitnami/spark/bin/spark-submit --packages "$PACKAGES" --master ${SPARK_MASTER:-local[*]} jobs/data_check.py

echo "Concluído. Saídas em $OUTPUT_DIR e tabelas no Postgres."
