# Data Engineer Exam — Spark + PostgreSQL (Docker)

Projeto completo para atender ao enunciado do PDF: construir ambiente Docker com Apache Spark e PostgreSQL, 
processar dados com PySpark e salvar resultados no Postgres e em arquivos (Parquet/CSV).

## Requisitos
- Docker e Docker Compose instalados.

## Estrutura
```
docker/
  spark/            # Dockerfile do Spark
  postgres/         # Dockerfile do Postgres + init scripts
    initdb/01-init.sql
jobs/               # Scripts PySpark
  common.py
  ingestion.py
  q1_novos_clientes.py
  q2_dias_vendas.py
  q3_top_vendedores.py
data/               # CSVs de entrada (vendas.csv, distribuidores.csv)
.env.example        # Variáveis de ambiente (copie para .env e ajuste se desejar)
docker-compose.yml
run_all.sh
Makefile
```

## Como executar
1. Altere o `.env` (se quiser mudar credenciais, host, etc.).
2. Suba os serviços:
   ```bash
   docker-compose up -d --build
   ```
   Aguarde o Postgres ficar pronto (logs do serviço `postgres`).
3. Execute todos os jobs:
   ```bash
   docker-compose exec spark bash run_all.sh
   ```
   Os resultados serão gravados no Postgres e em `/opt/app/output` (mapeado para `./output` na sua máquina).

## Tabelas geradas no Postgres
- `novos_clientes` (pivot por Ano|Mês)
- `dias_vendas_distribuidores`
- `ranking_mensal`
- `ranking_trimestral`
- `ranking_anual`
