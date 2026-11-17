"""Example of a DAG for this Airflow instance.

Add a folder under dags/, then a Python file that declares one DAG.
Airflow loads every file under dags/. This one prints hello once a day.
New DAGs stay paused until someone turns them on in the UI.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from airflow.sdk import DAG, task

JAKARTA = ZoneInfo("Asia/Jakarta")

with DAG(
    dag_id="sample_hello_daily",
    description="Example DAG that prints hello once a day.",
    start_date=datetime(2026, 1, 1, tzinfo=JAKARTA),
    schedule="@daily",
    catchup=False,
    tags=["sample"],
):

    @task
    def print_hello() -> None:
        print("hello")

    print_hello()
