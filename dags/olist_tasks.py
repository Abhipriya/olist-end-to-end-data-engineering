from pathlib import Path

import pandas as pd

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


LANDING_ROOT = Path("/opt/airflow/data/landing")
FORMATTED_ROOT = Path("/opt/airflow/data/formatted")


def discover_datasets():
    """
    Find every folder inside the landing directory.

    Rule:
    Each dataset folder must contain exactly one CSV file.

    Returns data in this shape:

    [
        [dataset_1],
        [dataset_2],
        ...
    ]

    The nested list structure is intentional because DAG Factory
    uses it as positional arguments for dynamic task mapping.
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

        # One list = positional arguments for one mapped task
        datasets.append([dataset])

    if not datasets:
        raise ValueError("No datasets found in landing directory.")

    print(f"Discovered {len(datasets)} datasets")

    for item in datasets:
        print(item[0])

    return datasets


def csv_to_parquet(dataset):
    """
    Convert one discovered CSV dataset into Parquet.
    """

    dataset_name = dataset["dataset_name"]
    csv_path = Path(dataset["csv_path"])

    output_directory = FORMATTED_ROOT / dataset_name

    output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    parquet_path = output_directory / (
        csv_path.stem + ".parquet"
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
    Upload one Parquet file to the Snowflake internal stage.
    """

    parquet_path = Path(dataset["parquet_path"])
    dataset_name = dataset["dataset_name"]

    if not parquet_path.exists():
        raise FileNotFoundError(
            f"Parquet file not found: {parquet_path}"
        )

    stage_path = (
        f"@OLIST_ECOMMERCE.RAW.OLIST_STAGE/{dataset_name}"
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

    hook = SnowflakeHook(
        snowflake_conn_id="snowflake_default"
    )

    hook.run(sql)

    print(
        f"Uploaded {parquet_path.name} "
        f"to {stage_path}"
    )

    result = {
        **dataset,
        "stage_path": (
            f"{stage_path}/{parquet_path.name}"
        ),
    }

    return [result]


def create_raw_table(dataset):
    """
    Create one RAW Snowflake table by inferring the schema
    from the staged Parquet file.
    """

    table_name = dataset["table_name"]
    stage_path = dataset["stage_path"]

    full_table_name = (
        f"OLIST_ECOMMERCE.RAW.{table_name}"
    )

    file_format = (
        "OLIST_ECOMMERCE.RAW.OLIST_PARQUET_FORMAT"
    )

    sql = f"""
        CREATE OR REPLACE TABLE {full_table_name}
        USING TEMPLATE (
            SELECT ARRAY_AGG(
                OBJECT_CONSTRUCT(*)
            )
            WITHIN GROUP (
                ORDER BY order_id
            )
            FROM TABLE(
                INFER_SCHEMA(
                    LOCATION => '{stage_path}',
                    FILE_FORMAT => '{file_format}'
                )
            )
        )
    """

    print(f"Creating RAW table: {full_table_name}")
    print(f"Inferring schema from: {stage_path}")

    hook = SnowflakeHook(
        snowflake_conn_id="snowflake_default"
    )

    hook.run(sql)

    print(f"Created table: {full_table_name}")

    result = {
        **dataset,
        "full_table_name": full_table_name,
    }

    return [result]


def copy_parquet_to_raw(dataset):
    """
    Load one staged Parquet file into its RAW Snowflake table.
    """

    stage_path = dataset["stage_path"]
    full_table_name = dataset["full_table_name"]

    file_format = (
        "OLIST_ECOMMERCE.RAW.OLIST_PARQUET_FORMAT"
    )

    sql = f"""
        COPY INTO {full_table_name}
        FROM {stage_path}
        FILE_FORMAT = (
            FORMAT_NAME = '{file_format}'
        )
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = ABORT_STATEMENT
    """

    print(f"Loading: {stage_path}")
    print(f"Target table: {full_table_name}")

    hook = SnowflakeHook(
        snowflake_conn_id="snowflake_default"
    )

    hook.run(sql)

    print(f"Loaded data into: {full_table_name}")

    return [dataset]


def validate_raw_load(dataset):
    """
    Verify that Snowflake contains the same number of rows
    as the source CSV/Parquet dataset.
    """

    full_table_name = dataset["full_table_name"]

    expected_rows = dataset["row_count"]

    hook = SnowflakeHook(
        snowflake_conn_id="snowflake_default"
    )

    result = hook.get_first(
        f"SELECT COUNT(*) FROM {full_table_name}"
    )

    actual_rows = result[0]

    print(f"Table: {full_table_name}")
    print(f"Expected rows: {expected_rows}")
    print(f"Actual rows: {actual_rows}")

    if actual_rows != expected_rows:
        raise ValueError(
            f"Row count mismatch for {full_table_name}: "
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