"""
Checks that a set of column names are valid percentages/fractions, i.e.,
have a value between 0-100 (for percentages) or 0-1 (for fractions).
This operator has been updated so that if there are zero invalid rows,
the query returns 1 (and the check passes), and if any invalid rows exist, it returns 0.
"""
from typing import List, Optional
from airflow.providers.common.sql.operators.sql import SQLCheckOperator  # Import here
from assignment6_starter_files.dags.utils.constants import AirflowConstants

class SnowflakePercentageCheckOperator(SQLCheckOperator):
    """
    Generates a set of SQL statements to ensure that all the given columns fall within
    the correct range. The query is wrapped so that if there are zero invalid rows, it returns 1,
    and if any invalid rows are found, it returns 0.
    """
    def __init__(
        self,
        column_names: List[str],
        table_name: str,
        schema: str,
        is_fraction: bool = True,  # True if the valid range is 0-1, False for 0-100 percentages
        database: str = AirflowConstants.DEFAULT_DATABASE,
        partition_id: Optional[str] = None,
        *args,
        **kwargs
    ) -> None:
        self.column_names = column_names
        self.table_name = table_name
        self.schema = schema
        self.is_fraction = is_fraction
        # If the caller does not provide a 'sql' keyword argument, generate it
        if "sql" not in kwargs:
            kwargs["sql"] = self._generate_sql()
        super().__init__(*args, **kwargs)

    def _generate_sql(self) -> str:
        """
        Generates SQL that returns 1 (pass) if no invalid values exist in the specified columns,
        and 0 (fail) if any value is out-of-range.
        """
        min_value = 0
        max_value = 1 if self.is_fraction else 100

        conditions = [
            f"({self.schema}.{self.table_name}.{column} >= {min_value} AND {self.schema}.{self.table_name}.{column} <= {max_value})"
            for column in self.column_names
        ]
        sql_condition = " AND ".join(conditions)
        # Wrap the query so that if COUNT(*) of invalid rows is 0, return 1 (pass), else return 0 (fail)
        return (
            f"SELECT CASE WHEN (SELECT COUNT(*) FROM {self.schema}.{self.table_name} "
            f"WHERE NOT ({sql_condition})) = 0 THEN 1 ELSE 0 END"
        )

