from abc import ABC
from typing import Optional
import posixpath
from importlib.metadata import version
from decimal import Decimal

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
    SUPPORTS_SCHEMA_PREP = False
    _FABRIC_USD_COST_PER_VCORE_HOUR = 0.09  # cost in East US 2 as of July 2025
    
    def __init__(self):
        try:
            from IPython.core.getipython import get_ipython
            self.notebookutils = get_ipython().user_ns.get("notebookutils")
            self.is_fabric = True if self.notebookutils.runtime.context['productType'] == 'Fabric' else False
        except:
            self.is_fabric = False

        self.version: str = ''
        self.cost_per_vcore_hour: Optional[float] = None
                  
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
    
    def get_job_cost(self, duration_ms: int) -> Optional[Decimal]:
        """
        Returns the cost per hour for compute as a Decimal.
        
        If `cost_per_vcore_hour` is provided, it calculates the cost based on the total cores.
        Otherwise, it returns None.
        """
        if self.cost_per_vcore_hour is None:
            return None

        cost_per_hour = Decimal(self.get_total_cores()) * Decimal(self.cost_per_vcore_hour)
        job_cost = cost_per_hour * Decimal(duration_ms) / Decimal(3600000)  # Convert ms to hours
        return job_cost.quantize(Decimal('0.0000000000'))  # Ensure precision matches DECIMAL(18,10)
    
    def _convert_generic_to_specific_schema(self, generic_schema: list):
        """
        Convert a generic schema to a specific Spark schema.
        """
        import pyarrow as pa
        type_mapping = {
            'STRING': pa.string(),
            'TIMESTAMP': pa.timestamp('us', tz='UTC'),
            'TINYINT': pa.int8(),
            'SMALLINT': pa.int16(),
            'INT': pa.int32(),
            'BIGINT': pa.int64(),
            'FLOAT': pa.float32(),
            'DOUBLE': pa.float64(),
            'DECIMAL(18,10)': pa.decimal128(18, 10),
            'BOOLEAN': pa.bool_(),
            'MAP<STRING, STRING>': pa.map_(pa.string(), pa.string())
        }
        return pa.schema([(name, type_mapping[data_type]) for name, data_type in generic_schema])
    
    def _append_results_to_delta(self, abfss_path: str, results: list, generic_schema: list):
        """
        Appends a list of result records to an existing Delta table.

        Parameters
        ----------
        abfss_path : str
            The ABFSS URI or where the Delta table resides.
        results : list of dict
            A list of row dictionaries to append. Each dictionary may include an
            'engine_metadata' key, whose contents will be stored as a MAP<STRING,STRING>.
        generic_schema : list of tuple
            A list of (field_name, field_type) tuples defining the generic schema
            for the rows in `results`.

        Notes
        -----
        - Converts `generic_schema` into a PyArrow schema.
        - Extracts 'engine_metadata' from each row and appends it as a
          MAP<STRING,STRING> column.
        - Uses DeltaRs to write the data in "append" mode.
        - If the installed `deltalake` version is 0.x, forces the Rust engine.
        """
        import pyarrow as pa
        from ..engines.delta_rs import DeltaRs

        schema = self._convert_generic_to_specific_schema(generic_schema=generic_schema)

        # Extract engine_metadata and convert to map format, otherwise is interpreted as a Struct
        index = schema.get_field_index("engine_metadata")
        schema = schema.remove(index)
        map_data = []
        for result in results:
            metadata = result.pop('engine_metadata', {})
            if metadata:
                map_items = [(str(k), str(v)) for k, v in metadata.items()]
            else:
                map_items = []

            map_data.append(map_items)

        table = pa.Table.from_pylist(results, schema)
        map_array = pa.array(map_data, type=pa.map_(pa.string(), pa.string()))
        table = table.append_column('engine_metadata', map_array)

        if version('deltalake').startswith('0.'):
            DeltaRs().write_deltalake(
                abfss_path, 
                table, 
                mode="append",
                schema_mode='merge'
                engine='rust'
            )
        else:
            DeltaRs().write_deltalake(
                abfss_path, 
                table, 
                mode="append",
                schema_mode='merge'
            )