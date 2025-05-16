from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from assignment6_starter_files.plugins.operators.snowflake_signal_sensor import SnowflakeSignalSensor
from assignment6_starter_files.plugins.operators.snowflake_percentage_check_operator import SnowflakePercentageCheckOperator
from assignment6_starter_files.plugins.operators.snowflake_historical_operator import SnowflakeHistoricalOperator
from assignment6_starter_files.dags.utils.constants import AirflowConstants
from airflow.models.baseoperator import chain

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'broadway_musicals_historical',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

# ------------------------------------------------------------------
# (1) Reload staging data (since previous DAG cleared these tables).
# ------------------------------------------------------------------
load_staging_starts = SnowflakeOperator(
    task_id="load_staging_starts",
    snowflake_conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    sql="""
         COPY INTO AIRFLOW.STAGING_STARTS
         FROM @bison_db.AIRFLOW.BISON_STG/pre_1985_starts.csv
         FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY='"', SKIP_HEADER=1);
    """,
    dag=dag,
)

load_staging_synopses = SnowflakeOperator(
    task_id="load_staging_synopses",
    snowflake_conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    sql="""
         COPY INTO AIRFLOW.STAGING_SYNOPSES
         FROM @bison_db.AIRFLOW.BISON_STG/synopses.csv
         FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY='"', SKIP_HEADER=1);
    """,
    dag=dag,
)

load_staging_grosses = SnowflakeOperator(
    task_id="load_staging_grosses",
    snowflake_conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    sql="""
         COPY INTO AIRFLOW.STAGING_GROSSES
         FROM @bison_db.AIRFLOW.BISON_STG/grosses.csv
         FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY='"', SKIP_HEADER=1, NULL_IF=('NA','null'))
         ON_ERROR = 'CONTINUE';
    """,
    dag=dag,
)

load_staging_cpi = SnowflakeOperator(
    task_id="load_staging_cpi",
    snowflake_conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    sql="""
         COPY INTO AIRFLOW.STAGING_CPI
         FROM @bison_db.AIRFLOW.BISON_STG/cpi.csv
         FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY='"', SKIP_HEADER=1);
    """,
    dag=dag,
)

# ------------------------------------------------------------------
# (2) Insert signal rows for each staging table.
# ------------------------------------------------------------------
insert_signal_starts = SnowflakeOperator(
    task_id="insert_signal_starts",
    snowflake_conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    sql="""
         INSERT INTO AIRFLOW.signal (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, PARTITION_ID, STATUS)
         VALUES ('BISON_DB', 'AIRFLOW', 'STAGING_STARTS', '{{ ds }}', 'success');
    """,
    dag=dag,
)

insert_signal_synopses = SnowflakeOperator(
    task_id="insert_signal_synopses",
    snowflake_conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    sql="""
         INSERT INTO AIRFLOW.signal (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, PARTITION_ID, STATUS)
         VALUES ('BISON_DB', 'AIRFLOW', 'STAGING_SYNOPSES', '{{ ds }}', 'success');
    """,
    dag=dag,
)

insert_signal_grosses = SnowflakeOperator(
    task_id="insert_signal_grosses",
    snowflake_conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    sql="""
         INSERT INTO AIRFLOW.signal (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, PARTITION_ID, STATUS)
         VALUES ('BISON_DB', 'AIRFLOW', 'STAGING_GROSSES', '{{ ds }}', 'success');
    """,
    dag=dag,
)

insert_signal_cpi = SnowflakeOperator(
    task_id="insert_signal_cpi",
    snowflake_conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    sql="""
         INSERT INTO AIRFLOW.signal (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, PARTITION_ID, STATUS)
         VALUES ('BISON_DB', 'AIRFLOW', 'STAGING_CPI', '{{ ds }}', 'success');
    """,
    dag=dag,
)

# ------------------------------------------------------------------
# (3) Sensors ensure the raw staging signals exist.
# ------------------------------------------------------------------
sensor_starts = SnowflakeSignalSensor(
    task_id='sensor_starts',
    conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    table_name='STAGING_STARTS',
    schema='AIRFLOW',
    database='BISON_DB',
    partition_id='{{ ds }}',
    poke_interval=30,
    timeout=600,
    dag=dag
)

sensor_synopses = SnowflakeSignalSensor(
    task_id='sensor_synopses',
    conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    table_name='STAGING_SYNOPSES',
    schema='AIRFLOW',
    database='BISON_DB',
    partition_id='{{ ds }}',
    poke_interval=30,
    timeout=600,
    dag=dag
)

sensor_grosses = SnowflakeSignalSensor(
    task_id='sensor_grosses',
    conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    table_name='STAGING_GROSSES',
    schema='AIRFLOW',
    database='BISON_DB',
    partition_id='{{ ds }}',
    poke_interval=30,
    timeout=600,
    dag=dag
)

sensor_cpi = SnowflakeSignalSensor(
    task_id='sensor_cpi',
    conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    table_name='STAGING_CPI',
    schema='AIRFLOW',
    database='BISON_DB',
    partition_id='{{ ds }}',
    poke_interval=30,
    timeout=600,
    dag=dag
)

# ------------------------------------------------------------------
# (4) Create the derived historical table DIM_MUSICALS_HISTORICAL.
# ------------------------------------------------------------------
create_dim_musicals_table = SnowflakeOperator(
    task_id='create_dim_musicals_table',
    snowflake_conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    sql="""
         CREATE OR REPLACE TABLE AIRFLOW.DIM_MUSICALS_HISTORICAL (
            ds DATE,
            show_name STRING PRIMARY KEY,
            start_date DATE,
            synopsis STRING,
            total_seats_sold INT,
            total_box_office_gross FLOAT,
            total_adjusted_box_office_gross FLOAT,
            fraction_adjusted_box_office_gross FLOAT
         );
    """,
    dag=dag
)

# ------------------------------------------------------------------
# (4b) Create the historical signal table (expected by the historical operator).
# ------------------------------------------------------------------
create_dim_musicals_signal_table = SnowflakeOperator(
    task_id='create_dim_musicals_signal_table',
    snowflake_conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    sql="""
         CREATE OR REPLACE TABLE AIRFLOW.DIM_MUSICALS_signal (
             view_name STRING,
             execution_date TIMESTAMP
         );
    """,
    dag=dag
)

# ------------------------------------------------------------------
# (5) Populate DIM_MUSICALS_HISTORICAL using the SnowflakeHistoricalOperator.
# ------------------------------------------------------------------
# Modified select_sql now returns 8 columns (the first column is ds).
select_sql = """
SELECT 
    CURRENT_DATE AS ds,
    s.show AS show_name,
    s.start_date,
    sy.synopsis,
    SUM(g.seats_sold) AS total_seats_sold,
    SUM(g.weekly_gross) AS total_box_office_gross,
    SUM(g.weekly_gross / c.cpi * 100) AS total_adjusted_box_office_gross,
    CASE WHEN SUM(g.weekly_gross) = 0 THEN 0
         ELSE SUM(g.weekly_gross / c.cpi * 100) / SUM(g.weekly_gross)
    END AS fraction_adjusted_box_office_gross
FROM AIRFLOW.STAGING_STARTS s
LEFT JOIN AIRFLOW.STAGING_SYNOPSES sy ON s.show = sy.show
JOIN AIRFLOW.STAGING_GROSSES g ON s.show = g.show
JOIN AIRFLOW.STAGING_CPI c ON DATE_TRUNC('month', s.start_date) = c.year_month
GROUP BY s.show, s.start_date, sy.synopsis
"""

populate_dim_musicals = SnowflakeHistoricalOperator(
    task_id='populate_dim_musicals_historical',
    conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    select_sql=select_sql,
    target_table='DIM_MUSICALS_HISTORICAL',
    view_name='DIM_MUSICALS',
    schema='AIRFLOW',
    dag=dag
)

# ------------------------------------------------------------------
# (6) Use SnowflakePercentageCheckOperator to verify that the fraction is between 0 and 1.
# ------------------------------------------------------------------
check_fraction = SnowflakePercentageCheckOperator(
    task_id='check_fraction',
    table_name='DIM_MUSICALS_HISTORICAL',
    schema='AIRFLOW',
    column_names=['fraction_adjusted_box_office_gross'],
    conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    dag=dag
)

# ------------------------------------------------------------------
# Define task dependencies using chain.
# ------------------------------------------------------------------
# (A) First, reload staging data then insert signal rows.
chain(
    [load_staging_starts, load_staging_synopses, load_staging_grosses, load_staging_cpi],
    [insert_signal_starts, insert_signal_synopses, insert_signal_grosses, insert_signal_cpi]
)

# (B) Then wait on the signal rows with sensors.
chain(
    [insert_signal_starts, insert_signal_synopses, insert_signal_grosses, insert_signal_cpi],
    [sensor_starts, sensor_synopses, sensor_grosses, sensor_cpi]
)

# (C) Once the sensors have passed, create the derived table and its signal table.
chain(
    [sensor_starts, sensor_synopses, sensor_grosses, sensor_cpi],
    create_dim_musicals_table,
    create_dim_musicals_signal_table
)

# (D) Then populate the derived historical table and finally run the percentage check.
chain(
    create_dim_musicals_signal_table,
    populate_dim_musicals,
    check_fraction
)
