import subprocess
from datetime import date, timedelta

import pendulum
from airflow.sdk import DAG, Param, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable


TAXI_PYTHON = "/opt/airflow/taxi-venv/bin/python"


def run_pipeline(*args):
    subprocess.run([TAXI_PYTHON, "-m", "taxi_pipeline", *args], check=True)


with DAG(
    dag_id="nyc_taxi_monthly",
    description="Load TLC yellow taxi records and rebuild reporting tables",
    start_date=pendulum.datetime(2024, 3, 1, tz="UTC"),
    schedule=CronDataIntervalTimetable("0 0 1 * *", timezone="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    params={
        "source_month": Param(
            None,
            type=["null", "string"],
            pattern=r"^[1-9][0-9]{3}-(0[1-9]|1[0-2])$",
            description="Set a historical source month as YYYY-MM. Scheduled runs use the publication lag.",
        ),
    },
    tags=["nyc-taxi"],
):
    @task
    def load_zones():
        run_pipeline("load-zones")

    @task
    def load_trips():
        context = get_current_context()
        override = context["params"]["source_month"]
        if override:
            month = date.fromisoformat(f"{override}-01")
        else:
            interval_start = context.get("data_interval_start")
            if interval_start is None:
                raise ValueError("Provide source_month (YYYY-MM) when triggering a historical run")
            # A May 1 run closes the April interval and requests February records.
            month = pendulum.instance(interval_start).subtract(months=2).date()
        run_pipeline("load-trips", "--year", str(month.year), "--month", str(month.month))

    @task
    def build_reporting():
        run_pipeline("build")

    load_zones() >> load_trips() >> build_reporting()
