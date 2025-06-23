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
        super().__init__(spark_measure_telemetry=spark_measure_telemetry)

        self.lakehouse_name = lakehouse_name
        self.lakehouse_schema_name = lakehouse_schema_name
        self.lakehouse_workspace_name = lakehouse_workspace_name
        self.full_catalog_schema_reference = f"`{self.lakehouse_workspace_name}`.`{self.lakehouse_name}`.`{self.lakehouse_schema_name}`"


    def create_schema_if_not_exists(self, drop_before_create: bool = True):
        """
        Prepare an empty schema in the lakehouse.
        """
        if drop_before_create:
            self.spark.sql(f"DROP SCHEMA IF EXISTS {self.full_catalog_schema_reference}")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.full_catalog_schema_reference}")
        self.spark.sql(f"USE {self.full_catalog_schema_reference}")

    def get_total_cores(self):
        cores = (self.spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size()) * int(self.spark.conf.get('spark.executor.cores'))
        return cores