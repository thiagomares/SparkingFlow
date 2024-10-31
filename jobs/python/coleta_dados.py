from pyspark.sql import SparkSession

def collect_data_from_mysql():
    # Cria uma SparkSession
    spark = SparkSession.builder \
        .appName("CollectDataFromMySQL") \
        .config("spark.jars.packages", "mysql:mysql-connector-java-8.0.32.jar") \
        .getOrCreate()

    # Configurações de conexão
    jdbc_url = "jdbc:mysql://mysql:3306/airflow"
    table_name = "tabela"  # Substitua pelo nome da sua tabela
    properties = {
        "user": "airflow",
        "password": "airflow_password",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    # Lê os dados da tabela MySQL
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)

    spark.stop()

if __name__ == "__main__":
    collect_data_from_mysql()
