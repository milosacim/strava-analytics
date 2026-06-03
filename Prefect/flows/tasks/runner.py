from pathlib import Path
from prefect import task
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

DBT_DIR = Path(__file__).resolve().parents[3] / "dbt"

_runner = PrefectDbtRunner(
    settings=PrefectDbtSettings(
        project_dir=str(DBT_DIR),
        profiles_dir=str(DBT_DIR),
    )
)

@task
def run_dbt_staging():
    """
    Prefect task. Runs every dbt model tagged 'staging' (the silver layer)."""
    _runner.invoke(["run", "--select", "tag:staging"])