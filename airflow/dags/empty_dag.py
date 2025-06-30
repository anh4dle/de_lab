import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id="my_dag_name",
    start_date=datetime.datetime(2021, 1, 1),
    schedule="@daily",
):
    task = EmptyOperator(task_id="task")

    task2 = EmptyOperator(task_id="task2")
    task >> task2    
    