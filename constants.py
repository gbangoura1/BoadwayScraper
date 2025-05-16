class AirflowConstants:
    # TODO: replace this constant with your Snowflake user animal
    ANIMAL = 'BISON'

    SNOWFLAKE_CONN_ID = 'SnowFlakeConn'  # Updated to match the Airflow connection ID
    STAGE_NAME = f'{ANIMAL}_STG'
    DEFAULT_DATABASE = f'{ANIMAL}_DB'
    DEFAULT_WAREHOUSE = f'{ANIMAL}_DB'

    SUCCESS_STATUS = 'success'

