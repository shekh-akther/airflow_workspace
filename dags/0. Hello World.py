from datetime import datetime

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="00_hello_world",
    start_date=datetime(year=2024, month=6, day=1),
    end_date=datetime(year=2024, month=6, day=6),
    schedule="@daily",
):
    hello = BashOperator(task_id="hello", bash_command="echo 'hello'")
    
    world = PythonOperator(task_id="world", python_callable=lambda: print("world"))

    hello >> world
