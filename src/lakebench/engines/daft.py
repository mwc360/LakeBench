from .base import BaseEngine
from .delta_rs import DeltaRs

import os
import posixpath
from importlib.metadata import version
from typing import Optional

class Daft(BaseEngine):
    """
    Daft Engine for ELT Benchmarks.
    """
    import daft
    from daft.io import IOConfig, AzureConfig
    SQLGLOT_DIALECT = "mysql"
    REQUIRED_READ_ENDPOINT = "abfss"
    REQUIRED_WRITE_ENDPOINT = "abfss"
    SUPPORTS_ONELAKE = False
    SUPPORTS_SCHEMA_PREP = False

    def __init__(
            self, 
            delta_abfss_schema_path: str,
            cost_per_vcore_hour: Optional[float] = None
            ):
        """
        Initialize the Daft Engine Configs
        """
        super().__init__()
        self.delta_abfss_schema_path = delta_abfss_schema_path
        self.deltars = DeltaRs()
        self.catalog_name = None
        self.schema_name = None
        if self.delta_abfss_schema_path.startswith("abfss://"):
            if self.is_fabric:
                os.environ["AZURE_STORAGE_TOKEN"] = (
                    self.notebookutils.credentials.getToken("storage")
                )
            if not os.getenv("AZURE_STORAGE_TOKEN"):
                raise ValueError(
                    "Please store bearer token as env variable `AZURE_STORAGE_TOKEN`"
                )

        io_config = self.IOConfig(azure=self.AzureConfig(bearer_token=os.getenv("AZURE_STORAGE_TOKEN")))

        self.daft.set_planning_config(default_io_config=io_config)

        if not self.SUPPORTS_ONELAKE:
            if 'onelake.' in self.delta_abfss_schema_path:
                raise ValueError(
                    f"Daft engine does not support OneLake paths. Provide an ADLS Gen2 path instead."
                )
            
        self.version: str = f"{version('daft')} (deltalake=={version('deltalake')})"
        self.cost_per_vcore_hour = cost_per_vcore_hour or getattr(self, '_FABRIC_USD_COST_PER_VCORE_HOUR', None)
        
    def load_parquet_to_delta(self, parquet_folder_path: str, table_name: str, table_is_precreated: bool = False, context_decorator: Optional[str] = None):
        table_df = self.daft.read_parquet(
            posixpath.join(parquet_folder_path)
        )
        table_df.write_deltalake(
            posixpath.join(self.delta_abfss_schema_path, table_name),
            mode="overwrite"
        ) 

    def register_table(self, table_name: str):
        """
        Register a Delta table DataFrame in Daft.
        """
        globals()[table_name] = self.daft.read_deltalake(
            posixpath.join(self.delta_abfss_schema_path, table_name)
        )

    def execute_sql_query(self, query: str, context_decorator: Optional[str] = None):
        """
        Execute a SQL query using Daft.
        """
        result = self.daft.sql(query).collect()

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