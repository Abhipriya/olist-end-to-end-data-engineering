import json
from pathlib import Path

import pandas as pd

from snowflake_config import get_snowflake_hook


LANDING_ROOT = Path("/opt/airflow/data/landing")
FORMATTED_ROOT = Path("/opt/airflow/data/formatted")
CONFIG_FILE = Path("/opt/airflow/dags/config.json")

def load_config():
    """
    Load dataset configuration from config.json.
    """

    with open(CONFIG_FILE, "r") as file:
        return json.load(file)

def discover_datasets():
    """
    Discover all CSV files for each configured dataset.
    """

    config = load_config()

    datasets = []

    for folder in sorted(LANDING_ROOT.iterdir()):

        if not folder.is_dir():
            continue

        dataset_name = folder.name

        if dataset_name not in config:
            print(
                f"Skipping {dataset_name}: "
                f"no configuration found"
            )
            continue

        csv_files = sorted(
            folder.glob("*.csv")
        )

        if len(csv_files) == 0:
            print(
                f"Skipping {dataset_name}: "
                f"no CSV files found"
            )
            continue

        dataset_config = config[dataset_name]

        dataset = {
            "dataset_name": dataset_name,
            "csv_paths": [
                str(csv_file)
                for csv_file in csv_files
            ],
            "table_name": (
                dataset_config["table_name"]
            ),
            "load_strategy": (
                dataset_config["load_strategy"]
            ),
            "database": (
                dataset_config["database"]
            ),
            "schema": (
                dataset_config["schema"]
            ),
            "stage": (
                dataset_config["stage"]
            ),
            "file_format": (
                dataset_config["file_format"]
            ),
        }

        datasets.append([dataset])

    if not datasets:
        raise ValueError(
            "No datasets found in landing directory."
        )

    print(
        f"Discovered {len(datasets)} datasets"
    )

    for item in datasets:
        dataset = item[0]

        print(
            f"{dataset['dataset_name']}: "
            f"{len(dataset['csv_paths'])} CSV file(s)"
        )

    return datasets

def csv_to_parquet(dataset):
    """
    Convert all CSV files of one dataset into Parquet files.
    """

    dataset_name = dataset["dataset_name"]
    csv_paths = dataset["csv_paths"]

    output_directory = (
        FORMATTED_ROOT / dataset_name
    )

    output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    parquet_paths = []
    total_rows = 0

    for csv_path_str in csv_paths:

        csv_path = Path(csv_path_str)

        parquet_path = (
            output_directory
            / f"{csv_path.stem}.parquet"
        )

        print(f"Reading: {csv_path}")

        df = pd.read_csv(csv_path)

        print(f"Rows: {len(df)}")
        print(f"Columns: {len(df.columns)}")

        df.to_parquet(
            parquet_path,
            engine="pyarrow",
            index=False,
        )

        print(f"Created: {parquet_path}")

        parquet_paths.append(
            str(parquet_path)
        )

        total_rows += len(df)

    print(
        f"Dataset {dataset_name}: "
        f"{len(parquet_paths)} file(s), "
        f"{total_rows} total rows"
    )

    return [{
        **dataset,
        "parquet_paths": parquet_paths,
        "row_count": total_rows,
    }]


def upload_parquet_to_stage(dataset):
    """
    Upload all Parquet files of one dataset
    to the Snowflake internal stage.
    """

    dataset_name = dataset["dataset_name"]
    parquet_paths = dataset["parquet_paths"]

    database = dataset["database"]
    schema = dataset["schema"]
    stage = dataset["stage"]

    stage_path = (
        f"@{database}."
        f"{schema}."
        f"{stage}/"
        f"{dataset_name}"
    )

    hook = get_snowflake_hook()

    staged_files = []

    for parquet_path_str in parquet_paths:

        parquet_path = Path(parquet_path_str)

        if not parquet_path.exists():
            raise FileNotFoundError(
                f"Parquet file not found: {parquet_path}"
            )

        file_uri = f"file://{parquet_path}"

        sql = f"""
            PUT '{file_uri}'
            {stage_path}
            AUTO_COMPRESS = FALSE
            OVERWRITE = TRUE
        """

        print(f"Uploading: {parquet_path}")
        print(f"Stage: {stage_path}")

        hook.run(sql)

        staged_file = (
            f"{stage_path}/{parquet_path.name}"
        )

        staged_files.append(staged_file)

        print(
            f"Uploaded {parquet_path.name}"
        )

    print(
        f"Dataset {dataset_name}: "
        f"{len(staged_files)} file(s) uploaded"
    )

    return [{
        **dataset,
        "stage_paths": staged_files,
    }]

def copy_parquet_to_raw(dataset):
    """
    Load all staged Parquet files into the RAW table.

    delete_insert:
        Clear the table once, then load all current files.

    append:
        Keep existing data and append all current files.
    """

    stage_paths = dataset["stage_paths"]
    table_name = dataset["table_name"]
    load_strategy = dataset["load_strategy"]

    database = dataset["database"]
    schema = dataset["schema"]
    file_format = dataset["file_format"]

    full_table_name = (
        f"{database}."
        f"{schema}."
        f"{table_name}"
    )

    full_file_format = (
        f"{database}."
        f"{schema}."
        f"{file_format}"
    )

    hook = get_snowflake_hook()

    result = hook.get_first(
        f"SELECT COUNT(*) FROM {full_table_name}"
    )

    rows_before_load = result[0]

    if load_strategy == "delete_insert":

        print(
            f"Delete-insert strategy: "
            f"{full_table_name}"
        )

        hook.run(
            f"TRUNCATE TABLE {full_table_name}"
        )

    elif load_strategy == "append":

        print(
            f"Append strategy: "
            f"{full_table_name}"
        )

    else:
        raise ValueError(
            f"Unsupported load strategy: "
            f"{load_strategy}"
        )

    for stage_path in stage_paths:

        sql = f"""
            COPY INTO {full_table_name}
            FROM {stage_path}
            FILE_FORMAT = (
                FORMAT_NAME = '{full_file_format}'
            )
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = ABORT_STATEMENT
            FORCE = TRUE
        """

        print(
            f"Loading: {stage_path}"
        )

        hook.run(sql)

    print(
        f"Loaded {len(stage_paths)} file(s) "
        f"into {full_table_name}"
    )

    return [{
        **dataset,
        "full_table_name": full_table_name,
        "rows_before_load": rows_before_load,
    }]


def validate_raw_load(dataset):
    """
    Validate the RAW table row count.

    delete_insert:
        expected rows = rows from current batch

    append:
        expected rows = old rows + current batch rows
    """

    full_table_name = dataset["full_table_name"]
    load_strategy = dataset["load_strategy"]

    source_rows = dataset["row_count"]
    rows_before_load = dataset["rows_before_load"]

    result = get_snowflake_hook().get_first(
        f"SELECT COUNT(*) FROM {full_table_name}"
    )

    actual_rows = result[0]

    if load_strategy == "delete_insert":
        expected_rows = source_rows

    elif load_strategy == "append":
        expected_rows = (
            rows_before_load + source_rows
        )

    else:
        raise ValueError(
            f"Unsupported load strategy: "
            f"{load_strategy}"
        )

    print(f"Table: {full_table_name}")
    print(f"Load strategy: {load_strategy}")
    print(f"Rows before load: {rows_before_load}")
    print(f"Current batch rows: {source_rows}")
    print(f"Expected rows: {expected_rows}")
    print(f"Actual rows: {actual_rows}")

    if actual_rows != expected_rows:
        raise ValueError(
            f"Row count mismatch for "
            f"{full_table_name}: "
            f"expected={expected_rows}, "
            f"actual={actual_rows}"
        )

    print(
        f"Validation successful: "
        f"{actual_rows} rows"
    )

    return {
        **dataset,
        "loaded_row_count": actual_rows,
    }