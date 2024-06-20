from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from pprint import pprint


def print_context(**context):
    pprint(context)



def _print_exec_date(**context): 
    pprint(f"here we go... {context}")
    # pprint(f"here we go... {context['templates_dict']['execution_date']}" )
    # pprint(f"This script was executed at {context['execution_date']}")
    # pprint(f"Three days after execution is {context['execution_date']}")
    pprint(f"This script run date is {context['start_date']}")
    
    
                                              
with DAG(
    dag_id="exercise3_print_context6",
    start_date=datetime(year=2024, month=6, day=1),
    schedule="@daily"
):
  
    print_exec_date_bash = BashOperator(
        task_id="demo_templating_bash",
        bash_command="echo {{task.task_id}} in the {{dag.dag_id}} pipeline"
    )

    print_exec_date_py = PythonOperator(
        task_id='demo_templating_py',
        python_callable=_print_exec_date,
        provide_context=True,
        templates_dict={
            "execution_date": "{{ execution_date }}"
            }, 
    )
    
    print_exec_date_bash >> print_exec_date_py
    
   