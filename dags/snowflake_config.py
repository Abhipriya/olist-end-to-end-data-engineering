from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


def get_snowflake_hook():
    return SnowflakeHook(
        snowflake_conn_id="snowflake_default"
    )