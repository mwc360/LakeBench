from .base import BaseEngine

class PolarsEngine(BaseEngine):
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