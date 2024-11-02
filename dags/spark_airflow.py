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
    INSERT INTO logs (task_name, status, message, dag_name)
    VALUES ('{task_name}', '{status}', '{message}', '{dag_id}');
    """
    
    # Usando MySqlOperator para garantir que a operação ocorra dentro do contexto do Airflow
    log_task = MySqlOperator(
        task_id=f'log_{task_name}',
        sql=sql,
        mysql_conn_id='mysql-conn',
        dag=dag,
    )
    
    # Executando a tarefa de log
    log_task.execute(context={})

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

# Criar a tabela de logs se não existir
criar_tabela_logs = MySqlOperator(
    task_id='criar_tabela_logs',
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

criar_tabela = MySqlOperator(
    task_id='criar_tabela',
    sql="""
    CREATE TABLE IF NOT EXISTS tabela (
        id INT AUTO_INCREMENT PRIMARY KEY,
        nome VARCHAR(100),
        idade INT,
        data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    mysql_conn_id='mysql-conn',
    on_success_callback=log_task_status('criar_tabela'),
    on_failure_callback=log_task_status('criar_tabela'),
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

python_job = SparkSubmitOperator(
    task_id="python_job",
    conn_id="spark-conn",
    application="jobs/python/wordcountjob.py",
    dag=dag,
    on_success_callback=log_task_status('python_job'),
    on_failure_callback=log_task_status('python_job'),
)

testando = SparkSubmitOperator(
    task_id="testando",
    conn_id="spark-conn",
    application="jobs/python/coleta_dados.py",
    packages='mysql:mysql-connector-java:8.0.32',
    dag=dag,
    on_success_callback=log_task_status('testando'),
    on_failure_callback=log_task_status('testando'),
)

scala_job = SparkSubmitOperator(
    task_id="scala_job",
    conn_id="spark-conn",
    application="jobs/scala/target/scala-2.12/word-count_2.12-0.1.jar",
    dag=dag,
    on_success_callback=log_task_status('scala_job'),
    on_failure_callback=log_task_status('scala_job'),
)

java_job = SparkSubmitOperator(
    task_id="java_job",
    conn_id="spark-conn",
    application="jobs/java/spark-job/target/spark-job-1.0-SNAPSHOT.jar",
    java_class="com.airscholar.spark.WordCountJob",
    dag=dag,
    on_success_callback=log_task_status('java_job'),
    on_failure_callback=log_task_status('java_job'),
)

end = PythonOperator(
    task_id="end",
    python_callable=end_job,
    provide_context=True,
    dag=dag,
)

# Definindo a ordem das tarefas
start >> criar_tabela_logs >> [criar_tabela, cria_backup]
criar_tabela >> [python_job, scala_job, java_job] >> testando >> end
cria_backup >> [python_job, scala_job, java_job] >> testando >> end
