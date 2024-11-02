from pyspark.sql import SparkSession

def collect_and_transfer_data():
    # Cria uma SparkSession
    spark = SparkSession.builder \
        .appName("TransferDataBetweenSchemas") \
        .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.32") \
        .getOrCreate()

    # Configurações de conexão
    jdbc_url = "jdbc:mysql://mysql:3306/airflow"
    source_schema = "airflow"  # Nome do esquema de origem
    source_table = "tabela"  # Nome da tabela de origem
    target_schema = "sales"  # Nome do esquema de destino
    target_table = "new_custs"  # Nome da tabela de destino
    properties = {
        "user": "root",
        "password": "root_password",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    # Lê os dados da tabela no esquema A
    source_table_full = f"{source_schema}.{source_table}"
    df = spark.read.jdbc(url=jdbc_url, table=source_table_full, properties=properties)

    # Realiza operações de transformação se necessário
    # Exemplo: df = df.filter(df['idade'] > 18)  # Exemplo de filtragem

    # Escreve os dados na tabela do esquema B
    target_table_full = f"{target_schema}.{target_table}"
    df.write.jdbc(url=jdbc_url, table=target_table_full, mode="append", properties=properties)

    spark.stop()

if __name__ == "__main__":
    collect_and_transfer_data()
