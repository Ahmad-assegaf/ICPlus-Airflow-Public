# Intern Airflow

This repository is how you add DAGs to the **intern-airflow** instance. You do not log in to the server. Push to `main` on GitHub. The server pulls this repo and loads every Python file under `dags/`.

This instance is separate from the production Airflow. The page title in the UI is `intern-airflow`. Ask the admin for the UI address.

Airflow version: 3.1.1.

## Add a DAG

1. Create a folder under `dags/` for your project, for example `dags/my_project/`.
2. Add a Python file in that folder. One file can declare one DAG.
3. Commit and push to `main`.

```text
dags/
  sample/
    hello_daily.py
  my_project/
    my_dag.py
```

After the admin has connected this repo, a push shows up in the UI in about a minute. A new DAG stays **paused** until someone turns it on. Open the DAG, unpause it, and trigger it once if you do not want to wait for the next schedule. The task log is where `print` output appears.

## Example

[`dags/sample/hello_daily.py`](dags/sample/hello_daily.py) prints `hello` once a day. Copy that file and change the ids.

```python
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
```

What each part does:

- `dag_id` is the name in the UI. It must be unique across this instance.
- `start_date` is when the schedule begins. Use a timezone, here Asia/Jakarta.
- `schedule` controls how often it runs. `@daily` is once a day. `@hourly` is once an hour. A cron expression such as `"0 8 * * *"` is 08:00 every day.
- `catchup=False` means Airflow does not run every missed day between `start_date` and today.
- `@task` marks the function as a task. Call the function inside the `with DAG` block so it is part of the DAG.
- `tags` are labels for filtering in the UI.

Import `DAG` and `task` from `airflow.sdk`, as in the example. That is the Airflow 3 style used here.

## What not to commit

Only DAG code belongs in this repo. Do not commit passwords, tokens, `.env`, or connection strings. Do not change `docker-compose.yml`; that file runs the instance on the server and is not how a DAG is deployed.
