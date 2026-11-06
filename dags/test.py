from datetime import timedelta

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="print_test",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=timedelta(seconds=5),
    catchup=False,
    max_active_runs=1,
)
def print_test():

    @task
    def print_message():
        maintenant = pendulum.now("Europe/Brussels")
        print(f"It is {maintenant.to_datetime_string()}")

    print_message()


print_test()