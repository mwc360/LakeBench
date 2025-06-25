from .base import BaseEngine
from  .delta_rs import DeltaRs

class DuckDB(BaseEngine):
    """
    DuckDB Engine for ELT Benchmarks.
    """

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
        self.sqlglot_dialect = "duckdb"
        self.deltars = DeltaRs()

    def load_parquet_to_delta(self, parquet_abfss_folder_path: str, table_name: str):
        arrow_df = self.duckdb.sql(f""" FROM parquet_scan('{parquet_abfss_folder_path}/*.parquet') """).record_batch()
        self.write_deltalake(
            f"{self.delta_abfss_schema_path}/{table_name}",
            arrow_df,
            mode="overwrite",
            engine='pyarrow'
        ) 

    def optimize_table(self, table_name: str):
        fact_table = self.deltars.DeltaTable(f"{self.delta_abfss_schema_path}/{table_name}/")
        fact_table.optimize.compact()

    def vacuum_table(self, table_name: str, retain_hours: int = 168, retention_check: bool = True):
        fact_table = self.deltars.DeltaTable(f"{self.delta_abfss_schema_path}/{table_name}/")
        fact_table.vacuum({retain_hours}, enforce_retention_duration=retention_check, dry_run=False)