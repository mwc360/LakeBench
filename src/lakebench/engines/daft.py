from .base import BaseEngine
from .delta_rs import DeltaRs

from IPython.core.getipython import get_ipython
notebookutils = get_ipython().user_ns.get("notebookutils")

class Daft(BaseEngine):
    """
    Daft Engine for ELT Benchmarks.
    """

    def __init__(
            self, 
            delta_mount_schema_path: str,
            delta_abfss_schema_path: str
            ):
        """
        Initialize the Daft Engine Configs
        """
        import daft
        self.daft = daft
        self.delta_mount_schema_path = delta_mount_schema_path
        self.delta_abfss_schema_path = delta_abfss_schema_path
        self.read_via = 'mount'
        self.write_via = 'abfss'
        self.sqlglot_dialect = "duckdb"
        self.deltars = DeltaRs()

    def load_parquet_to_delta(self, table_name: str):
        table_df = self.engine.daft.read_parquet(
            f"{self.source_data_mount_path}/{table_name}/*.parquet"
        )
        table_df.write_deltalake(
            f"{self.engine.delta_abfss_schema_path}/{table_name}",
            mode="overwrite"
        ) 


    def optimize_table(self, table_name: str):
        fact_table = self.deltars.DeltaTable(f"{self.delta_abfss_schema_path}/{table_name}/")
        fact_table.optimize.compact()

    def vacuum_table(self, table_name: str, retain_hours: int = 168, retention_check: bool = True):
        fact_table = self.deltars.DeltaTable(f"{self.delta_abfss_schema_path}/{table_name}/")
        fact_table.vacuum({retain_hours}, enforce_retention_duration=retention_check, dry_run=False)