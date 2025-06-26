from abc import ABC

class BaseEngine(ABC):
    SQLGLOT_DIALECT = None
    REQUIRED_READ_ENDPOINT = None
    REQUIRED_WRITE_ENDPOINT = None
    
    def __init__(self):
        """
        Base Engine
        """
                  
    def get_total_cores(self):
        import os
        cores = os.cpu_count()
        return cores
    
    def append_array_to_delta(self, abfss_path: str, array: list):
        """
        Append an array to a Delta table.
        """
        import pyarrow as pa
        from ..engines.delta_rs import DeltaRs
        results_table = pa.Table.from_pylist(array)
        DeltaRs().write_deltalake(
            abfss_path, 
            results_table, 
            mode="append",
            engine="rust"
        )
