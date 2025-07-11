from .spark import Spark
from typing import Optional

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
            lakehouse_name: str, 
            lakehouse_schema_name: str,
            spark_measure_telemetry: bool = False,
            cost_per_vcore_hour: Optional[float] = None
            ):
        """
        Initialize the SparkEngine with a Spark session.
        """
        self.lakehouse_name = lakehouse_name
        self.lakehouse_schema_name = lakehouse_schema_name

        super().__init__(catalog_name=self.lakehouse_name, schema_name=self.lakehouse_schema_name, spark_measure_telemetry=spark_measure_telemetry, cost_per_vcore_hour=cost_per_vcore_hour)

        self.version: str = f"{self.spark.sparkContext.version} (vhd_name=={self.spark.conf.get('spark.synapse.vhd.name')})"
        self.cost_per_vcore_hour = self._FABRIC_USD_COST_PER_VCORE_HOUR or cost_per_vcore_hour
