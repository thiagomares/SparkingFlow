from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pymysql

def test_mysql_connection(**kwargs):
    conn = pymysql.connect(
        host='mysql',
        user='airflow',
        password='airflow_password',
        database='airflow'
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT VERSION()")
            result = cursor.fetchone()
            print(f"MySQL version: {result[0]}")
    except Exception as e:
        print(f"Error connecting to MySQL: {e}")
    finally:
        conn.close()

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='test_mysql_connection',
    default_args=default_args,
    schedule_interval='@once',  # Execute uma vez
    catchup=False,
) as dag:
    
    test_connection = PythonOperator(
        task_id='test_connection',
        python_callable=test_mysql_connection,
        provide_context=True,
    )

test_connection
