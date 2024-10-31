# Aqui eu estou escolhendo qual vai ser a imagem que eu vou utilizar para o meu container
FROM apache/airflow:latest-python3.12

# Mudando para o usuário root
USER root

# Aqui eu preciso instalar o gcc (c++) e o Java, optei pelo 17, mas poderia ser qualquer versão do java
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-17-jdk && \
    apt-get clean

# Para o funcionamento correto do spark, nos precisamos que o java home seja setado
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Alternando para o usuario airflow
USER airflow


# Instalando o Airflow, o spark e a biblioteca do spark para o airflow para que se utilize a lib
RUN pip install apache-airflow apache-airflow-providers-apache-spark pyspark pymysql

# Optional: Add any additional setup or configurations here
