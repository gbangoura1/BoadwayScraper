from typing import List
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from assignment6_starter_files.dags.utils.constants import AirflowConstants  # Corrected import

class SnowflakeHistoricalOperator(SQLExecuteQueryOperator):
    """
    Generates a transaction with the SQL to do the following:
    - Inserts data into a historical table (idempotently),
    - Inserts a row into the signal table,
    - Recreates the view for this table's most recent data.
    """

    def __init__(
        self,
        select_sql: str,  # SQL query generating data to insert
        target_table: str,  # Name of the target table for inserting historical data
        view_name: str,  # Name of the view to recreate
        schema: str,
        database: str = AirflowConstants.DEFAULT_DATABASE,
        *args,
        **kwargs
    ) -> None:
        super().__init__(
            sql=self._generate_transaction(
                select_sql=select_sql,
                target_table=target_table,  # Pass to the method
                view_name=view_name,  # Pass to the method
                schema=schema,
                database=database,
            ),
            *args,
            **kwargs,  # Pass the rest of the arguments (including snowflake_conn_id, which will be handled by Airflow)
        )

    def _generate_transaction(
        self,
        select_sql: str,
        target_table: str,  # target table for inserting historical data
        view_name: str,  # name of the view to update
        schema: str,
        database: str,
    ) -> List[str]:
        """
        Generates a SQL transaction to:
        1. Insert data into a historical table,
        2. Insert a row into the signal table,
        3. Recreate the view for the most recent data (for the last 7 days).
        """
        # SQL to insert data into the historical table
        insert_sql = f"""
        INSERT INTO {schema}.{target_table}  -- Use target_table here
        ({select_sql})
        """

        # SQL to insert a row into the signal table
        signal_sql = f"""
        INSERT INTO {schema}.{view_name}_signal (view_name, execution_date)
        VALUES ('{view_name}', CURRENT_TIMESTAMP)
        """

        # SQL to recreate the view for the most recent data using the 'ds' column instead of execution_date.
        recreate_view_sql = f"""
        CREATE OR REPLACE VIEW {schema}.{view_name}_view AS
        SELECT * FROM {schema}.{target_table}  -- Use target_table here
        WHERE ds >= CURRENT_DATE - INTERVAL '7 days'
        """

        # Return all the SQL statements in a list
        return [insert_sql, signal_sql, recreate_view_sql]

