from .base import BaseEngine
from .delta_rs import DeltaRs

import posixpath

class Daft(BaseEngine):
    """
    Daft Engine for ELT Benchmarks.
    """
    SQLGLOT_DIALECT = "mysql"
    REQUIRED_READ_ENDPOINT = "abfss"
    REQUIRED_WRITE_ENDPOINT = "abfss"
    SUPPORTS_ONELAKE = False

    def __init__(
            self, 
            delta_abfss_schema_path: str
            ):
        """
        Initialize the Daft Engine Configs
        """
        import daft
        from daft.io import IOConfig, AzureConfig
        self.daft = daft
        self.delta_abfss_schema_path = delta_abfss_schema_path
        self.deltars = DeltaRs()
        self.catalog_name = None
        self.schema_name = None
        access_token = self.notebookutils.credentials.getToken('storage')
        io_config = IOConfig(azure=AzureConfig(bearer_token=access_token))

        self.daft.set_planning_config(default_io_config=io_config)

        if not self.SUPPORTS_ONELAKE:
            if 'onelake.' in self.delta_abfss_schema_path:
                raise ValueError(
                    f"Daft engine does not support OneLake paths. Provide an ADLS Gen2 path instead."
                )


    def load_parquet_to_delta(self, parquet_folder_path: str, table_name: str):
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

    def execute_sql_query(self, query: str):
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