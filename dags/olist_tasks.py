from pathlib import Path

import pandas as pd

from snowflake_config import get_snowflake_hook


LANDING_ROOT = Path("/opt/airflow/data/landing")
FORMATTED_ROOT = Path("/opt/airflow/data/formatted")

SNOWFLAKE_DATABASE = "OLIST_ECOMMERCE"
SNOWFLAKE_SCHEMA = "RAW"
SNOWFLAKE_STAGE = "OLIST_STAGE"
SNOWFLAKE_FILE_FORMAT = "OLIST_PARQUET_FORMAT"


def discover_datasets():
    """
    Discover CSV datasets from the landing directory.
    """

    datasets = []

    for folder in sorted(LANDING_ROOT.iterdir()):

        if not folder.is_dir():
            continue

        csv_files = list(folder.glob("*.csv"))

        if len(csv_files) == 0:
            print(f"Skipping {folder.name}: no CSV found")
            continue

        if len(csv_files) > 1:
            raise ValueError(
                f"{folder} contains more than one CSV file."
            )

        csv_file = csv_files[0]

        dataset = {
            "dataset_name": folder.name,
            "csv_path": str(csv_file),
            "table_name": f"RAW_{folder.name.upper()}",
        }

        datasets.append([dataset])

    if not datasets:
        raise ValueError(
            "No datasets found in landing directory."
        )

    print(f"Discovered {len(datasets)} datasets")

    for item in datasets:
        print(item[0])

    return datasets


def csv_to_parquet(dataset):
    """
    Convert one CSV dataset into Parquet.
    """

    dataset_name = dataset["dataset_name"]
    csv_path = Path(dataset["csv_path"])

    output_directory = (
        FORMATTED_ROOT / dataset_name
    )

    output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    parquet_path = (
        output_directory
        / f"{csv_path.stem}.parquet"
    )

    print(f"Dataset: {dataset_name}")
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

    return [{
        "dataset_name": dataset_name,
        "csv_path": str(csv_path),
        "parquet_path": str(parquet_path),
        "table_name": dataset["table_name"],
        "row_count": len(df),
    }]


def upload_parquet_to_stage(dataset):
    """
    Upload one Parquet file to Snowflake stage.
    """

    parquet_path = Path(
        dataset["parquet_path"]
    )

    dataset_name = dataset["dataset_name"]

    if not parquet_path.exists():
        raise FileNotFoundError(
            f"Parquet file not found: {parquet_path}"
        )

    stage_path = (
        f"@{SNOWFLAKE_DATABASE}."
        f"{SNOWFLAKE_SCHEMA}."
        f"{SNOWFLAKE_STAGE}/"
        f"{dataset_name}"
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

    get_snowflake_hook().run(sql)

    print(
        f"Uploaded {parquet_path.name} "
        f"to {stage_path}"
    )

    return [{
        **dataset,
        "stage_path": (
            f"{stage_path}/{parquet_path.name}"
        ),
    }]


def copy_parquet_to_raw(dataset):
    """
    Load staged Parquet into Terraform-created RAW table.
    """

    stage_path = dataset["stage_path"]
    table_name = dataset["table_name"]

    full_table_name = (
        f"{SNOWFLAKE_DATABASE}."
        f"{SNOWFLAKE_SCHEMA}."
        f"{table_name}"
    )

    file_format = (
        f"{SNOWFLAKE_DATABASE}."
        f"{SNOWFLAKE_SCHEMA}."
        f"{SNOWFLAKE_FILE_FORMAT}"
    )

    hook = get_snowflake_hook()

    print(
        f"Clearing existing data from: "
        f"{full_table_name}"
    )

    hook.run(
        f"TRUNCATE TABLE {full_table_name}"
    )

    sql = f"""
        COPY INTO {full_table_name}
        FROM {stage_path}
        FILE_FORMAT = (
            FORMAT_NAME = '{file_format}'
        )
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = ABORT_STATEMENT
        FORCE = TRUE
    """

    print(f"Loading: {stage_path}")
    print(f"Target table: {full_table_name}")

    hook.run(sql)

    print(
        f"Loaded data into: {full_table_name}"
    )

    return [{
        **dataset,
        "full_table_name": full_table_name,
    }]


def validate_raw_load(dataset):
    """
    Validate RAW table row count.
    """

    full_table_name = dataset["full_table_name"]
    expected_rows = dataset["row_count"]

    result = get_snowflake_hook().get_first(
        f"SELECT COUNT(*) FROM {full_table_name}"
    )

    actual_rows = result[0]

    print(f"Table: {full_table_name}")
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