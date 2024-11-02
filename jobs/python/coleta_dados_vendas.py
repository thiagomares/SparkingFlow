from pyspark.sql import SparkSession

def collect_and_transfer_data():
    spark = SparkSession.builder \
        .appName("TransferDataBetweenSchemas") \
        .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.32,"
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.200") \
        .getOrCreate()

    bucket_name = 'bucketestudosengdados'
    file_key = 'mes/dia/hora/13/vendas.csv'
    s3_path = f's3a://{bucket_name}/{file_key}'

    jdbc_url = "jdbc:mysql://mysql:3306/airflow"
    target_schema = "airflow"
    target_table = "vendas"
    properties = {
        "user": "root",
        "password": "root_password",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAZI2LDILVXCJPTPHI")
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.sa-east-1.amazonaws.com")

    df = spark.read.csv(s3_path, header=True, inferSchema=True, sep=';')

    target_table_full = f"{target_schema}.{target_table}"
    df.write.jdbc(url=jdbc_url, table=target_table_full, mode="append", properties=properties)

    spark.stop()

if __name__ == "__main__":
    collect_and_transfer_data()
