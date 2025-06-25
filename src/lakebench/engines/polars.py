from .base import BaseEngine
from .delta_rs import DeltaRs

class Polars(BaseEngine):
    """
    Polars Engine for ELT Benchmarks.
    """

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
        self.sqlglot_dialect = "duckdb"
        self.deltars = DeltaRs()

    def optimize_table(self, table_name: str):
        fact_table = self.deltars.DeltaTable(f"{self.delta_abfss_schema_path}/{table_name}/")
        fact_table.optimize.compact()

    def vacuum_table(self, table_name: str, retain_hours: int = 168, retention_check: bool = True):
        fact_table = self.deltars.DeltaTable(f"{self.delta_abfss_schema_path}/{table_name}/")
        fact_table.vacuum({retain_hours}, enforce_retention_duration=retention_check, dry_run=False)