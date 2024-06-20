from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator

from pprint import pprint


def prepare_steps():
    procure_rocket_material = EmptyOperator(task_id="procure_rocket_material")
    procure_fuel = EmptyOperator(task_id="procure_fuel")

    build_stage_1 = EmptyOperator(task_id="build_stage_1")
    build_stage_2 = EmptyOperator(task_id="build_stage_2")
    build_stage_3 = EmptyOperator(task_id="build_stage_3")

    launch = EmptyOperator(task_id="launch")

    procure_rocket_material >> [build_stage_1, build_stage_2, build_stage_3] >> launch
    procure_fuel >> build_stage_3 >> launch


def print_context(**context):
    pprint(context)

with DAG(
    dag_id='01_rocket_launch',
    start_date=datetime(year=2024, month=6, day=5),
    end_date=datetime(year=2024, month=6, day=30),
    schedule="45 13 * * 1,3,5"
):
    prepare_steps()


with DAG(
    dag_id="02_rocket_launch_mon_wed_friday",
    start_date=(datetime.now() - timedelta(90)),
    schedule="45 13 * * 1,3,5"
):
    prepare_steps()


with DAG(
    dag_id='03_rocket_launch_every_3_days_timedelta',
    start_date=(datetime.now() - timedelta(90)),
    schedule=timedelta(days=3)
    ):
    prepare_steps()
