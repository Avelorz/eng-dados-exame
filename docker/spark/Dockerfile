FROM bitnami/spark:3.5.1

# Instala pacotes úteis de sistema e Python libs
USER root
RUN install_packages bash curl nano procps

# Diretórios de trabalho
WORKDIR /opt/app
RUN mkdir -p /opt/app/jobs /opt/app/data /opt/app/output /opt/app/warehouse

# Variáveis de ambiente
ENV SPARK_NO_DAEMONIZE=true         PYSPARK_PYTHON=python3         PYSPARK_DRIVER_PYTHON=python3         APP_WAREHOUSE_DIR=/opt/app/warehouse         OUTPUT_DIR=/opt/app/output         JDBC_PACKAGES=org.postgresql:postgresql:42.7.3

# Copia jobs e scripts
COPY jobs /opt/app/jobs
COPY run_all.sh /opt/app/run_all.sh
COPY data /opt/app/data

RUN chmod +x /opt/app/run_all.sh
