import airflow
from airflow.decorators import dag, task
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.hooks.base import BaseHook
from airflow.operators.dummy_operator import DummyOperator

@dag(
    dag_id="sparking_flow",
    default_args={
        "owner": "Thiago Mares",
        "start_date": airflow.utils.dates.days_ago(1),
        "retries": 1,
        "retry_delay": airflow.utils.dates.timedelta(minutes=5),
    },
    schedule_interval="@daily"
)

@task(task_id="criar_tabela_logs")
def tabela_logs(task_name, status, msg, dag):
    mysql_conn = BaseHook.get_connection('mysql-conn')
    sql = f"""
    INSERT INTO logs (task_name, status, msg, dag_name)
    VALUES ('{task_name}', '{status}', '{msg}', '{dag}');
    """
    
    # Usando MySqlOperator para garantir que a operação ocorra dentro do contexto do Airflow
    log_task = MySqlOperator(
        task_id=f'log_{task_name}',
        sql=sql,
        mysql_conn_id='mysql-conn',
        dag=dag,
    )
    
    # Executando a task_name de log
    log_task.execute(context={})
    


