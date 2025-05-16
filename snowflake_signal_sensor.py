from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from assignment6_starter_files.dags.utils.constants import AirflowConstants
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

class SnowflakeSignalSensor(BaseSensorOperator):
    """Checks the signal table to see if a table partition has landed."""
    
    template_fields = ("partition_id",)

    def __init__(
        self,
        conn_id: str,
        schema: str,
        table_name: str,
        database: str,
        partition_id: str,  # Adjusted for 'PARTITION_ID'
        status: str = AirflowConstants.SUCCESS_STATUS,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.schema = schema
        self.table_name = table_name
        self.database = database
        self.partition_id = partition_id
        self.status = status

    def poke(self, context: Context) -> bool:
        """Returns True if the signal row exists in the table."""
        
        # Resolve the partition_id template
        partition_id_resolved = self.render_template(self.partition_id, context)

        # Initialize Snowflake hook
        hook = SnowflakeHook(snowflake_conn_id=self.conn_id)

        # Construct the SQL query
        sql = f"""
            SELECT 1
            FROM {self.database}.{self.schema}.signal
            WHERE PARTITION_ID = '{partition_id_resolved}'
              AND TABLE_NAME = '{self.table_name}'
              AND status = '{self.status}'
            LIMIT 1
        """

        # Log the SQL for debugging
        self.log.info("Executing SQL: %s", sql)

        # Execute query
        records = hook.get_records(sql)

        # Return True if records exist
        return bool(records)

