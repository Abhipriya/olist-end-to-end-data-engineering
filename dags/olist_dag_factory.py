from pathlib import Path

from airflow import DAG
from dagfactory import load_yaml_dags


DAGS_DIR = Path(__file__).parent
CONFIG_FILE = DAGS_DIR / "olist_ingestion.yml"


load_yaml_dags(
    globals_dict=globals(),
    config_filepath=str(CONFIG_FILE),
)