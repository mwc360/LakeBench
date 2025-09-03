from .base import BaseEngine
from .delta_rs import DeltaRs

import os
import posixpath
from typing import Optional
from importlib.metadata import version

class Polars(BaseEngine):
    """
    Polars Engine for ELT Benchmarks.
    """
    SQLGLOT_DIALECT = "duckdb"
    REQUIRED_READ_ENDPOINT = None
    REQUIRED_WRITE_ENDPOINT = "abfss"
    SUPPORTS_ONELAKE = True
    SUPPORTS_SCHEMA_PREP = False

    def __init__(
            self, 
            delta_abfss_schema_path: str,
            cost_per_vcore_hour: Optional[float] = None
            ):
        """
        Initialize the Polars Engine Configs
        """
        super().__init__()
        import polars as pl
        self.pl = pl
        self.delta_abfss_schema_path = delta_abfss_schema_path
        self.deltars = DeltaRs()
        self.catalog_name = None
        self.schema_name = None
        if self.delta_abfss_schema_path.startswith("abfss://"):
            self._validate_and_set_azure_storage_config()
        
        self.sql = pl.SQLContext()

        self.version: str = f"{version('polars')} (deltalake=={version('deltalake')})"
        self.cost_per_vcore_hour = cost_per_vcore_hour or getattr(self, '_FABRIC_USD_COST_PER_VCORE_HOUR', None)

    def load_parquet_to_delta(self, parquet_folder_path: str, table_name: str, table_is_precreated: bool = False, context_decorator: Optional[str] = None):
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

    def execute_sql_query(self, query: str, context_decorator: Optional[str] = None):
        """
        Execute a SQL query using Polars.
        """
        result = self.sql.execute(query).collect(engine='streaming')

    def optimize_table(self, table_name: str):
        fact_table = self.deltars.DeltaTable(
            table_uri=posixpath.join(self.delta_abfss_schema_path, table_name),
            storage_options=self.storage_options,
        )
        fact_table.optimize.compact()

    def vacuum_table(self, table_name: str, retain_hours: int = 168, retention_check: bool = True):
        fact_table = self.deltars.DeltaTable(
            table_uri=posixpath.join(self.delta_abfss_schema_path, table_name),
            storage_options=self.storage_options,
        )
        fact_table.vacuum(retain_hours, enforce_retention_duration=retention_check, dry_run=False)