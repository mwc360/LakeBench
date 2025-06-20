from .base import BaseEngine

class DuckDBEngine(BaseEngine):
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