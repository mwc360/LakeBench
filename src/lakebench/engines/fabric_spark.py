from .spark import Spark

try:
    from IPython.core.getipython import get_ipython
    utils = get_ipython().user_ns["mssparkutils"]
except Exception as e:
    e

class FabricSpark(Spark):
    """
    Spark Engine for ELT Benchmarks.
    """

    def __init__(
            self,
            lakehouse_workspace_name: str, 
            lakehouse_name: str, 
            lakehouse_schema_name: str,
            spark_measure_telemetry: bool = False
            ):
        """
        Initialize the SparkEngine with a Spark session.
        """
        self.lakehouse_name = lakehouse_name
        self.lakehouse_schema_name = lakehouse_schema_name
        self.lakehouse_workspace_name = lakehouse_workspace_name

        super().__init__(catalog_name=self.lakehouse_name, schema_name=self.lakehouse_schema_name, spark_measure_telemetry=spark_measure_telemetry)

