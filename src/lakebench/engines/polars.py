from .base import BaseEngine
from .delta_rs import DeltaRs

import posixpath
from typing import Any, Optional
from importlib.metadata import version

class Polars(BaseEngine):
    """
    Polars Engine
    """
    SQLGLOT_DIALECT = "duckdb"
    SUPPORTS_ONELAKE = True
    SUPPORTS_SCHEMA_PREP = False
    SUPPORTS_MOUNT_PATH = True

    def __init__(
            self, 
            schema_or_working_directory_uri: str,
            cost_per_vcore_hour: Optional[float] = None,
            storage_options: Optional[dict[str, Any]] = None
            ):
        """
        Initialize the Polars Engine Configs
        """
        super().__init__(schema_or_working_directory_uri, storage_options)
        import polars as pl
        self.pl = pl
        self.deltars = DeltaRs()
        self.catalog_name = None
        self.schema_name = None
        self.sql = pl.SQLContext()
        self.version: str = f"{version('polars')} (deltalake=={version('deltalake')})"
        self.cost_per_vcore_hour = cost_per_vcore_hour or getattr(self, '_FABRIC_USD_COST_PER_VCORE_HOUR', None)

    def load_parquet_to_delta(self, parquet_folder_uri: str, table_name: str, table_is_precreated: bool = False, context_decorator: Optional[str] = None):
        table_df = self.pl.scan_parquet(
            posixpath.join(parquet_folder_uri, '*.parquet'), 
            storage_options=self.storage_options
        )
        table_df.collect(engine='streaming').write_delta(
            posixpath.join(self.schema_or_working_directory_uri, table_name), 
            mode="overwrite", 
            storage_options=self.storage_options
        )

    def register_table(self, table_name: str):
        """
        Register a Delta table LazyFrame in Polars.
        """
        df = self.pl.scan_delta(
            posixpath.join(self.schema_or_working_directory_uri, table_name), 
            storage_options=self.storage_options
        )
        self.sql.register(table_name, df)

    def execute_sql_query(self, query: str, context_decorator: Optional[str] = None):
        """
        Execute a SQL query using Polars.
        """
        result = self.sql.execute(query).collect(engine='streaming')

    def optimize_table(self, table_name: str):
        fact_table = self.deltars.DeltaTable(
            table_uri=posixpath.join(self.schema_or_working_directory_uri, table_name),
            storage_options=self.storage_options,
        )
        fact_table.optimize.compact()

    def vacuum_table(self, table_name: str, retain_hours: int = 168, retention_check: bool = True):
        fact_table = self.deltars.DeltaTable(
            table_uri=posixpath.join(self.schema_or_working_directory_uri, table_name),
            storage_options=self.storage_options,
        )
        fact_table.vacuum(retain_hours, enforce_retention_duration=retention_check, dry_run=False)