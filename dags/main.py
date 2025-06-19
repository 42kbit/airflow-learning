import datetime

from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id="my_dag_namee",
    start_date=datetime.datetime(2021, 1, 1),
    schedule="@daily",
):
    EmptyOperator(task_id="task")

if __name__ == "__main__":
    # Tests
    print("Test")
