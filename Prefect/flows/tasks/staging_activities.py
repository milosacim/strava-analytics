from pathlib import Path

from prefect import task
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

DBT_DIR = Path(__file__).resolve().parents[3] / "dbt"

runner = PrefectDbtRunner(
    settings=PrefectDbtSettings(
        project_dir=str(DBT_DIR),
        profiles_dir=str(DBT_DIR),
    )
)

@task
def create_staging_activities():
    runner.invoke(["run", "--select", "stg_strava__activities"])