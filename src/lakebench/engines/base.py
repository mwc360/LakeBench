from abc import ABC

class BaseEngine(ABC):
    """
    Abstract base class for implementing different engine types.

    Attributes
    ----------
    SQLGLOT_DIALECT : str, optional
        Specifies the SQL dialect to be used by the engine when SQL transpiling
        is required. Default is None.
    REQUIRED_READ_ENDPOINT : str, optional
        Specifies `mount` or `abfss` if the engine only supports one endpoint. Default is None.
    REQUIRED_WRITE_ENDPOINT : str, optional
        Specifies `mount` or `abfss` if the engine only supports one endpoint. Default is None.

    Methods
    -------
    get_total_cores()
        Returns the total number of CPU cores available on the system.
    get_compute_size()
        Returns a formatted string with the compute size.
    append_array_to_delta(abfss_path: str, array: list)
        Appends a list of data to a Delta table at the specified path.
    """
    SQLGLOT_DIALECT = None
    REQUIRED_READ_ENDPOINT = None
    REQUIRED_WRITE_ENDPOINT = None
    
    def __init__(self):
        pass
                  
    def get_total_cores(self) -> int:
        """
        Returns the total number of CPU cores available on the system.
        """
        import os
        cores = os.cpu_count()
        return cores
    
    def get_compute_size(self) -> str:
        """
        Returns a formatted string with the compute size.
        """
        cores = self.get_total_cores()
        return f"{cores}vCore"
    
    def append_array_to_delta(self, abfss_path: str, array: list):
        """
        Appends a list of data to a Delta table at the specified path.

        Parameters
        ----------
        abfss_path : str
            The path to the Delta table where the data will be appended.
        array : list
            A list of data to be appended to the Delta table.
        
        Notes
        -----
        This method uses PyArrow to convert the input list into a table format
        and DeltaRs to write the data to the Delta table. The operation is performed
        in "append" mode using the Rust engine.
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
