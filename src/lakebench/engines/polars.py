from .base import BaseEngine
from .delta_rs import DeltaRs

import posixpath

class Polars(BaseEngine):
    """
    Polars Engine for ELT Benchmarks.
    """
    SQLGLOT_DIALECT = "duckdb"
    REQUIRED_READ_ENDPOINT = None
    REQUIRED_WRITE_ENDPOINT = "abfss"
    SUPPORTS_ONELAKE = True

    def __init__(
            self, 
            delta_abfss_schema_path: str
            ):
        """
        Initialize the Polars Engine Configs
        """
        import polars as pl
        self.pl = pl
        self.delta_abfss_schema_path = delta_abfss_schema_path
        self.deltars = DeltaRs()
        self.storage_options={
            "bearer_token": self.notebookutils.credentials.getToken('storage')
        }
        self.catalog_name = None
        self.schema_name = None
        self.sql = pl.SQLContext()

    def load_parquet_to_delta(self, parquet_folder_path: str, table_name: str):
        table_df = self.pl.scan_parquet(
            posixpath.join(parquet_folder_path, '*.parquet'), 
            storage_options=self.storage_options
        )
        table_df.collect(engine='streaming').write_delta(
            posixpath.join(self.delta_abfss_schema_path, table_name), 
            mode="overwrite", 
            storage_options=self.storage_options
        )

    def register_table(self, table_name: str):
        """
        Register a Delta table LazyFrame in Polars.
        """
        df = self.pl.scan_delta(
            posixpath.join(self.delta_abfss_schema_path, table_name), 
            storage_options=self.storage_options
        )
        self.sql.register(table_name, df)

    def execute_sql_query(self, query: str):
        """
        Execute a SQL query using Polars.
        """
        result = self.sql.execute(query).collect(engine='streaming')

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