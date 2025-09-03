from .base import BaseEngine
from  .delta_rs import DeltaRs

import os
import posixpath
from typing import Optional
from importlib.metadata import version

class DuckDB(BaseEngine):
    """
    DuckDB Engine for ELT Benchmarks.
    """
    SQLGLOT_DIALECT = "duckdb"
    REQUIRED_READ_ENDPOINT = None
    REQUIRED_WRITE_ENDPOINT = "abfss"
    SUPPORTS_ONELAKE = True
    SUPPORTS_SCHEMA_PREP = True

    def __init__(
            self, 
            delta_abfss_schema_path: str,
            cost_per_vcore_hour: Optional[float] = None,
            ):
        """
        Initialize the DuckDB Engine Configs
        """
        super().__init__()
        import duckdb
        self.duckdb = duckdb.connect()
        self.deltars = DeltaRs()
        self.delta_abfss_schema_path = delta_abfss_schema_path
        self.catalog_name = None
        self.schema_name = None
        if self.delta_abfss_schema_path.startswith("abfss://"):
            self._validate_and_set_azure_storage_config()
            self.duckdb.sql(f""" CREATE OR REPLACE SECRET onelake ( TYPE AZURE, PROVIDER ACCESS_TOKEN, ACCESS_TOKEN '{os.getenv("AZURE_STORAGE_TOKEN")}') ;""")

        self.version: str = f"{version('duckdb')} (deltalake=={version('deltalake')})"
        self.cost_per_vcore_hour = cost_per_vcore_hour or getattr(self, '_FABRIC_USD_COST_PER_VCORE_HOUR', None)
    
    def _create_empty_table(self, table_name: str, ddl: str):
        if not ddl.strip().startswith("CREATE OR REPLACE TABLE"):
            ddl = ddl.replace("CREATE TABLE", "CREATE OR REPLACE TABLE")
        # Create in memory table
        self.duckdb.sql(ddl)

        arrow_df = self.duckdb.sql(f"FROM {table_name}")

        # Write empty in-memory table as Delta
        self.deltars.write_deltalake(
            posixpath.join(self.delta_abfss_schema_path, table_name),
            arrow_df,
            mode="overwrite"
        )  
        # Drop the in-memory table
        self.duckdb.sql(f"DROP TABLE IF EXISTS {table_name}")

    def load_parquet_to_delta(self, parquet_folder_path: str, table_name: str, table_is_precreated: bool = False, context_decorator: Optional[str] = None):
        arrow_df = self.duckdb.sql(f""" FROM parquet_scan('{posixpath.join(parquet_folder_path, '*.parquet')}') """).record_batch()
        self.deltars.write_deltalake(
            posixpath.join(self.delta_abfss_schema_path, table_name),
            arrow_df,
            mode="overwrite"
        )  

    def register_table(self, table_name: str):
        """
        Register a Delta table in DuckDB.
        """
        self.duckdb.sql(f"""
            CREATE OR REPLACE VIEW {table_name} 
            AS SELECT * FROM delta_scan('{posixpath.join(self.delta_abfss_schema_path, table_name)}')
        """)

    def execute_sql_query(self, query: str, context_decorator: Optional[str] = None):
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