from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from assignment6_starter_files.plugins.operators.snowflake_percentage_check_operator import SnowflakePercentageCheckOperator
from assignment6_starter_files.plugins.operators.snowflake_signal_sensor import SnowflakeSignalSensor
from assignment6_starter_files.dags.utils.constants import AirflowConstants

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'broadway_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

# 1. Create all necessary tables.
create_tables = SnowflakeOperator(
    task_id='create_tables',
    snowflake_conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    sql="""
        -- Create staging tables.
        CREATE OR REPLACE TABLE AIRFLOW.STAGING_STARTS (show STRING, start_date DATE);
        CREATE OR REPLACE TABLE AIRFLOW.STAGING_CPI (year_month DATE, cpi FLOAT);
        CREATE OR REPLACE TABLE AIRFLOW.STAGING_SYNOPSES (show STRING, synopsis STRING);
        CREATE OR REPLACE TABLE AIRFLOW.STAGING_GROSSES (
            week_ending DATE, week_number INT, weekly_gross_overall FLOAT,
            show STRING, theatre STRING, weekly_gross FLOAT, potential_gross FLOAT,
            avg_ticket_price FLOAT, top_ticket_price FLOAT,
            seats_sold INT, seats_in_theatre INT, pct_capacity FLOAT,
            performances INT, previews INT
        );
        -- Create the signal table.
        CREATE OR REPLACE TABLE AIRFLOW.signal (
            DATABASE_NAME STRING,
            SCHEMA_NAME STRING,
            TABLE_NAME STRING,
            PARTITION_ID STRING,
            STATUS STRING
        );
        -- Create historical tables (with partition column "ds").
        CREATE OR REPLACE TABLE AIRFLOW.DIM_STARTS_HISTORICAL (
            show STRING,
            start_date DATE,
            ds DATE
        );
        CREATE OR REPLACE TABLE AIRFLOW.DIM_CPI_HISTORICAL (
            year_month DATE,
            cpi FLOAT,
            ds DATE
        );
        CREATE OR REPLACE TABLE AIRFLOW.DIM_SYNOPSES_HISTORICAL (
            show STRING,
            synopsis STRING,
            ds DATE
        );
        CREATE OR REPLACE TABLE AIRFLOW.DIM_GROSSES_HISTORICAL (
            week_ending DATE,
            week_number INT,
            weekly_gross_overall FLOAT,
            show STRING,
            theatre STRING,
            weekly_gross FLOAT,
            potential_gross FLOAT,
            avg_ticket_price FLOAT,
            top_ticket_price FLOAT,
            seats_sold INT,
            seats_in_theatre INT,
            pct_capacity FLOAT,
            performances INT,
            previews INT,
            ds DATE
        );
    """,
    dag=dag
)

# 2. Load data into staging tables.
load_starts = SnowflakeOperator(
    task_id='load_pre1985_starts',
    snowflake_conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    sql="""
        COPY INTO AIRFLOW.STAGING_STARTS
        FROM @bison_db.AIRFLOW.BISON_STG/pre_1985_starts.csv
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
    """,
    dag=dag
)

load_cpi = SnowflakeOperator(
    task_id='load_cpi',
    snowflake_conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    sql="""
        COPY INTO AIRFLOW.STAGING_CPI
        FROM @bison_db.AIRFLOW.BISON_STG/cpi.csv
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
    """,
    dag=dag
)

load_synopses = SnowflakeOperator(
    task_id='load_synopses',
    snowflake_conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    sql="""
        COPY INTO AIRFLOW.STAGING_SYNOPSES
        FROM @bison_db.AIRFLOW.BISON_STG/synopses.csv
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
    """,
    dag=dag
)

load_grosses = SnowflakeOperator(
    task_id='load_grosses',
    snowflake_conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    sql="""
        COPY INTO AIRFLOW.STAGING_GROSSES
        FROM @bison_db.AIRFLOW.BISON_STG/grosses.csv
        FILE_FORMAT = (
            TYPE = 'CSV'
            FIELD_OPTIONALLY_ENCLOSED_BY='"'
            SKIP_HEADER=1
            NULL_IF=('NA', 'null')
        )
        ON_ERROR = 'CONTINUE';
    """,
    dag=dag
)

# 3. Populate historical tables from staging tables.
populate_hist_starts = SnowflakeOperator(
    task_id='populate_hist_starts',
    snowflake_conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    sql="""
        INSERT INTO AIRFLOW.DIM_STARTS_HISTORICAL (show, start_date, ds)
        SELECT show, start_date, CURRENT_DATE AS ds
        FROM AIRFLOW.STAGING_STARTS;
    """,
    dag=dag
)

populate_hist_cpi = SnowflakeOperator(
    task_id='populate_hist_cpi',
    snowflake_conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    sql="""
        INSERT INTO AIRFLOW.DIM_CPI_HISTORICAL (year_month, cpi, ds)
        SELECT year_month, cpi, CURRENT_DATE AS ds
        FROM AIRFLOW.STAGING_CPI;
    """,
    dag=dag
)

populate_hist_synopses = SnowflakeOperator(
    task_id='populate_hist_synopses',
    snowflake_conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    sql="""
        INSERT INTO AIRFLOW.DIM_SYNOPSES_HISTORICAL (show, synopsis, ds)
        SELECT show, synopsis, CURRENT_DATE AS ds
        FROM AIRFLOW.STAGING_SYNOPSES;
    """,
    dag=dag
)

populate_hist_grosses = SnowflakeOperator(
    task_id='populate_hist_grosses',
    snowflake_conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    sql="""
        INSERT INTO AIRFLOW.DIM_GROSSES_HISTORICAL (
            week_ending, week_number, weekly_gross_overall, show, theatre, weekly_gross, 
            potential_gross, avg_ticket_price, top_ticket_price, seats_sold, seats_in_theatre, 
            pct_capacity, performances, previews, ds
        )
        SELECT 
            week_ending, week_number, weekly_gross_overall, show, theatre, weekly_gross, 
            potential_gross, avg_ticket_price, top_ticket_price, seats_sold, seats_in_theatre, 
            pct_capacity, performances, previews, CURRENT_DATE AS ds
        FROM AIRFLOW.STAGING_GROSSES;
    """,
    dag=dag
)

# 4. (Optional) Insert signals that historical loads are complete.
insert_signal_hist_starts = SnowflakeOperator(
    task_id='insert_signal_hist_starts',
    snowflake_conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    sql="""
         INSERT INTO AIRFLOW.signal (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, PARTITION_ID, STATUS)
         VALUES ('BISON_DB', 'AIRFLOW', 'DIM_STARTS_HISTORICAL', TO_VARCHAR(CURRENT_DATE), 'historical_loaded');
    """,
    dag=dag
)

insert_signal_hist_cpi = SnowflakeOperator(
    task_id='insert_signal_hist_cpi',
    snowflake_conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    sql="""
         INSERT INTO AIRFLOW.signal (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, PARTITION_ID, STATUS)
         VALUES ('BISON_DB', 'AIRFLOW', 'DIM_CPI_HISTORICAL', TO_VARCHAR(CURRENT_DATE), 'historical_loaded');
    """,
    dag=dag
)

insert_signal_hist_synopses = SnowflakeOperator(
    task_id='insert_signal_hist_synopses',
    snowflake_conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    sql="""
         INSERT INTO AIRFLOW.signal (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, PARTITION_ID, STATUS)
         VALUES ('BISON_DB', 'AIRFLOW', 'DIM_SYNOPSES_HISTORICAL', TO_VARCHAR(CURRENT_DATE), 'historical_loaded');
    """,
    dag=dag
)

insert_signal_hist_grosses = SnowflakeOperator(
    task_id='insert_signal_hist_grosses',
    snowflake_conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    sql="""
         INSERT INTO AIRFLOW.signal (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, PARTITION_ID, STATUS)
         VALUES ('BISON_DB', 'AIRFLOW', 'DIM_GROSSES_HISTORICAL', TO_VARCHAR(CURRENT_DATE), 'historical_loaded');
    """,
    dag=dag
)

# 5. Create the refresh procedure (without logging the refresh event).
create_refresh_procedure = SnowflakeOperator(
    task_id='create_refresh_procedure',
    snowflake_conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    sql="""
       CREATE OR REPLACE PROCEDURE refresh_broadway_view(view_name STRING)
       RETURNS STRING
       LANGUAGE JAVASCRIPT
       EXECUTE AS CALLER
       AS
       $$
           // Retrieve the view name from the procedure parameter.
           var viewName = arguments[0];
           // Construct the historical table name.
           var hist_table = "AIRFLOW.DIM_" + viewName + "_HISTORICAL";
           // Fully qualified view name.
           var view_full = "AIRFLOW." + viewName;
           
           // Query the historical table for the maximum partition value (assumes 'ds' column).
           var sql_command = "SELECT MAX(ds) AS max_ds FROM " + hist_table;
           var stmt = snowflake.createStatement({sqlText: sql_command});
           var result = stmt.execute();
           var max_ds = null;
           if (result.next()){
                max_ds = result.getColumnValue("MAX_DS");
           }
           
           if (max_ds === null){
                return "No partitions found in " + hist_table;
           }
           
           // Drop the view if it exists.
           sql_command = "DROP VIEW IF EXISTS " + view_full;
           stmt = snowflake.createStatement({sqlText: sql_command});
           stmt.execute();
           
           // Create the view to point to only rows with the latest partition.
           sql_command = "CREATE OR REPLACE VIEW " + view_full + " AS " +
                         "SELECT * FROM " + hist_table + " WHERE ds = '" + max_ds + "'";
           stmt = snowflake.createStatement({sqlText: sql_command});
           stmt.execute();
           
           return "View " + viewName + " refreshed successfully.";
       $$;
    """,
    dag=dag
)

# 6. Refresh view tasks.
refresh_starts_view = SnowflakeOperator(
    task_id='refresh_starts_view',
    snowflake_conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    sql="CALL refresh_broadway_view('STARTS');",
    dag=dag
)

refresh_cpi_view = SnowflakeOperator(
    task_id='refresh_cpi_view',
    snowflake_conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    sql="CALL refresh_broadway_view('CPI');",
    dag=dag
)

refresh_synopses_view = SnowflakeOperator(
    task_id='refresh_synopses_view',
    snowflake_conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    sql="CALL refresh_broadway_view('SYNOPSES');",
    dag=dag
)

refresh_grosses_view = SnowflakeOperator(
    task_id='refresh_grosses_view',
    snowflake_conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    sql="CALL refresh_broadway_view('GROSSES');",
    dag=dag
)

# 7. Log the refresh events (separate tasks, to avoid spanning transactions in the procedure).
log_refresh_starts = SnowflakeOperator(
    task_id='log_refresh_starts',
    snowflake_conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    sql="""
         INSERT INTO AIRFLOW.signal (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, PARTITION_ID, STATUS)
         VALUES ('BISON_DB', 'AIRFLOW', 'AIRFLOW.STARTS', 'VIEW_REFRESH', 'refreshed');
    """,
    dag=dag
)

log_refresh_cpi = SnowflakeOperator(
    task_id='log_refresh_cpi',
    snowflake_conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    sql="""
         INSERT INTO AIRFLOW.signal (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, PARTITION_ID, STATUS)
         VALUES ('BISON_DB', 'AIRFLOW', 'AIRFLOW.CPI', 'VIEW_REFRESH', 'refreshed');
    """,
    dag=dag
)

log_refresh_synopses = SnowflakeOperator(
    task_id='log_refresh_synopses',
    snowflake_conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    sql="""
         INSERT INTO AIRFLOW.signal (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, PARTITION_ID, STATUS)
         VALUES ('BISON_DB', 'AIRFLOW', 'AIRFLOW.SYNOPSES', 'VIEW_REFRESH', 'refreshed');
    """,
    dag=dag
)

log_refresh_grosses = SnowflakeOperator(
    task_id='log_refresh_grosses',
    snowflake_conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    sql="""
         INSERT INTO AIRFLOW.signal (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, PARTITION_ID, STATUS)
         VALUES ('BISON_DB', 'AIRFLOW', 'AIRFLOW.GROSSES', 'VIEW_REFRESH', 'refreshed');
    """,
    dag=dag
)

# 8. Cleanup staging tables.
cleanup_staging = SnowflakeOperator(
    task_id='cleanup_staging',
    snowflake_conn_id=AirflowConstants.SNOWFLAKE_CONN_ID,
    sql="""
        TRUNCATE TABLE AIRFLOW.STAGING_STARTS;
        TRUNCATE TABLE AIRFLOW.STAGING_CPI;
        TRUNCATE TABLE AIRFLOW.STAGING_SYNOPSES;
        TRUNCATE TABLE AIRFLOW.STAGING_GROSSES;
    """,
    dag=dag
)

# ------------------------- Define Task Dependencies -------------------------

# (1) Create all tables.
create_tables >> [load_starts, load_cpi, load_synopses, load_grosses]

# (2) Load staging data then populate historical tables.
load_starts >> populate_hist_starts
load_cpi    >> populate_hist_cpi
load_synopses >> populate_hist_synopses
load_grosses >> populate_hist_grosses

# (3) When historical tables are populated, log that they are loaded.
populate_hist_starts   >> insert_signal_hist_starts
populate_hist_cpi      >> insert_signal_hist_cpi
populate_hist_synopses >> insert_signal_hist_synopses
populate_hist_grosses  >> insert_signal_hist_grosses

# (4) Once those signals are in place, create the refresh procedure.
[insert_signal_hist_starts, insert_signal_hist_cpi, insert_signal_hist_synopses, insert_signal_hist_grosses] >> create_refresh_procedure

# (5) Then call the refresh procedure for each view.
create_refresh_procedure >> [refresh_starts_view, refresh_cpi_view, refresh_synopses_view, refresh_grosses_view]

# (6) Log the refresh events (outside the stored procedure).
refresh_starts_view >> log_refresh_starts
refresh_cpi_view >> log_refresh_cpi
refresh_synopses_view >> log_refresh_synopses
refresh_grosses_view >> log_refresh_grosses

# (7) Finally, clean up the staging tables.
[log_refresh_starts, log_refresh_cpi, log_refresh_synopses, log_refresh_grosses] >> cleanup_staging

