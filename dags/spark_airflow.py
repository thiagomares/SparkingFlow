import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.hooks.base import BaseHook
import logging

dag = DAG(
    dag_id="sparking_flow",
    default_args={
        "owner": "Thiago Mares",
        "start_date": airflow.utils.dates.days_ago(1),
        "retries": 1,
        "retry_delay": airflow.utils.dates.timedelta(minutes=5),
    },
    schedule_interval="@daily"
)

def log_to_db(task_name, status, message, dag_id):
    mysql_conn = BaseHook.get_connection('mysql-conn')
    sql = f"""
    create table if not exists logs (
        id int auto_increment primary key,
        task_name varchar(100),
        status varchar(20),
        message text,
        dag_name varchar(100),
        timestamp timestamp default current_timestamp
    );
    INSERT INTO logs (task_name, status, message, dag_name)
    VALUES ('{task_name}', '{status}', '{message}', '{dag_id}');
    """
    
    log_task = MySqlOperator(
        task_id=f'log_{task_name}',
        sql=sql,
        mysql_conn_id='mysql-conn',
        dag=dag,
    )
    
    log_task.execute(context={})

def teste_coleta():
    import boto3
    import os

    AWS_ACCESS_KEY_ID = 'AKIAZI2LDILVTCRC5Y5I'
    AWS_SECRET_ACCESS_KEY = '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'
    AWS_REGION = 'sa-east-1' 

    bucket_name = 'bucketestudosengdados'
    file_key = 'mes/dia/hora/13/clientes.csv'
    local_file_path = 'clientes.csv' 

    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

    s3 = session.client('s3')

    try:
        s3.download_file(bucket_name, file_key, local_file_path)
        print(f"Arquivo {local_file_path} baixado com sucesso.")
    except Exception as e:
        print(f"Erro ao baixar o arquivo: {e}")


def start_job(**kwargs):
    logging.info("Jobs started")
    log_to_db('start', 'success', 'Jobs started.', dag.dag_id)

def end_job(**kwargs):
    logging.info("Jobs completed successfully")
    log_to_db('end', 'success', 'Jobs completed successfully.', dag.dag_id)

def log_task_status(task_name):
    def log_status(**kwargs):
        task_instance = kwargs['task_instance']
        status = 'success' if task_instance.state == 'success' else 'failure'
        message = f"{task_name} {'succeeded' if status == 'success' else 'failed'}."
        log_to_db(task_name, status, message, dag.dag_id)

    return log_status

criar_tabela_logs = MySqlOperator(
    task_id='check_status',
    sql=""" 
    CREATE TABLE IF NOT EXISTS logs (
        id INT AUTO_INCREMENT PRIMARY KEY,
        task_name VARCHAR(100),
        status VARCHAR(20),
        message TEXT,
        dag_name VARCHAR(100),
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    mysql_conn_id='mysql-conn',
)

cria_backup = MySqlOperator(
    task_id='cria_backup',
    sql=""" 
    CREATE TABLE IF NOT EXISTS tabela_backup AS 
    SELECT * FROM tabela WHERE data_criacao < DATE_SUB(NOW(), INTERVAL 1 DAY);
    """,
    mysql_conn_id='mysql-conn',
    on_success_callback=log_task_status('cria_backup'),
    on_failure_callback=log_task_status('cria_backup'),
)

start = PythonOperator(
    task_id="start",
    python_callable=start_job,
    provide_context=True,
    dag=dag
)

teste_coletador = PythonOperator(
    task_id="teste_coleta",  # Renomeado para evitar duplicação
    python_callable=teste_coleta,
    provide_context=True,
    dag=dag
)

tabela_clientes = SparkSubmitOperator(
    task_id="tabela_clientes",
    conn_id="spark-conn",
    application="jobs/python/coleta_dados_clientes.py",
    packages='mysql:mysql-connector-java:8.0.32,com.amazonaws:aws-java-sdk-s3:1.12.200,org.apache.hadoop:hadoop-aws:3.3.1',
    dag=dag,
    on_success_callback=log_task_status('testando'),
    on_failure_callback=log_task_status('testando'),
)

tabela_vendas = SparkSubmitOperator(
    task_id="tabela_vendas",
    conn_id="spark-conn",
    application="jobs/python/coleta_dados_vendas.py",
    packages='mysql:mysql-connector-java:8.0.32,com.amazonaws:aws-java-sdk-s3:1.12.200,org.apache.hadoop:hadoop-aws:3.3.1',
    dag=dag,
    on_success_callback=log_task_status('testando'),
    on_failure_callback=log_task_status('testando'),
)

tabela_vendedores = SparkSubmitOperator(
    task_id="tabela_vendedores",
    conn_id="spark-conn",
    application="jobs/python/coleta_dados_vendedores.py",
    packages='mysql:mysql-connector-java:8.0.32,com.amazonaws:aws-java-sdk-s3:1.12.200,org.apache.hadoop:hadoop-aws:3.3.1',
    dag=dag,
    on_success_callback=log_task_status('testando'),
    on_failure_callback=log_task_status('testando'),
)

tabela_produtos = SparkSubmitOperator(
    task_id="tabela_produtos",
    conn_id="spark-conn",
    application="jobs/python/coleta_dados_produtos.py",
    packages='mysql:mysql-connector-java:8.0.32,com.amazonaws:aws-java-sdk-s3:1.12.200,org.apache.hadoop:hadoop-aws:3.3.1',
    dag=dag,
    on_success_callback=log_task_status('testando'),
    on_failure_callback=log_task_status('testando'),
)

tabela_itens = SparkSubmitOperator(
    task_id="tabela_itens",
    conn_id="spark-conn",
    application="jobs/python/coleta_dados_itens_venda.py",
    packages='mysql:mysql-connector-java:8.0.32,com.amazonaws:aws-java-sdk-s3:1.12.200,org.apache.hadoop:hadoop-aws:3.3.1',
    dag=dag,
    on_success_callback=log_task_status('testando'),
    on_failure_callback=log_task_status('testando'),
)

end = PythonOperator(
    task_id="end",
    python_callable=end_job,
    provide_context=True,
    dag=dag,
)

start >> criar_tabela_logs >> [teste_coletador, cria_backup] 
teste_coletador >> [tabela_clientes, tabela_vendas, tabela_vendedores, tabela_produtos, tabela_itens] >> end

