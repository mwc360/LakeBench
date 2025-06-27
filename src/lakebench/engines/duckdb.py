from .base import BaseEngine
from  .delta_rs import DeltaRs

import posixpath

class DuckDB(BaseEngine):
    """
    DuckDB Engine for ELT Benchmarks.
    """
    SQLGLOT_DIALECT = "duckdb"
    REQUIRED_READ_ENDPOINT = None
    REQUIRED_WRITE_ENDPOINT = "abfss"

    def __init__(
            self, 
            delta_abfss_schema_path: str
            ):
        """
        Initialize the DuckDB Engine Configs
        """
        import duckdb
        self.duckdb = duckdb
        self.delta_abfss_schema_path = delta_abfss_schema_path
        self.deltars = DeltaRs()
        self.catalog_name = None
        self.schema_name = None

    def load_parquet_to_delta(self, parquet_folder_path: str, table_name: str):
        arrow_df = self.duckdb.sql(f""" FROM parquet_scan('{posixpath.join(parquet_folder_path, '*.parquet')}') """).record_batch()
        self.deltars.write_deltalake(
            posixpath.join(self.delta_abfss_schema_path, table_name),
            arrow_df,
            mode="overwrite",
            engine='pyarrow'
        )  

    def register_table(self, table_name: str):
        """
        Register a Delta table in DuckDB.
        """
        self.duckdb.sql(f"""
            CREATE OR REPLACE VIEW {table_name} 
            AS SELECT * FROM delta_scan('{posixpath.join(self.delta_abfss_schema_path, table_name)}')
        """)

    def execute_sql_query(self, query: str):
        """
        Execute a SQL query using DuckDB.
        """
        result = self.duckdb.sql(query).df()

    def optimize_table(self, table_name: str):
        fact_table = self.deltars.DeltaTable(
            posixpath.join(self.delta_abfss_schema_path, table_name)
        )
        fact_table.optimize.compact()

    def vacuum_table(self, table_name: str, retain_hours: int = 168, retention_check: bool = True):
        fact_table = self.deltars.DeltaTable(
            posixpath.join(self.delta_abfss_schema_path, table_name)
        )
        fact_table.vacuum(retain_hours, enforce_retention_duration=retention_check, dry_run=False)