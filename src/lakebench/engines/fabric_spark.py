from .spark import Spark
from typing import Optional
from decimal import Decimal

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
            cost_per_vcore_hour: Optional[float] = None,
            compute_stats_all_cols: bool = False
            ):
        """
        Initialize the SparkEngine with a Spark session.
        """
        self.lakehouse_name = lakehouse_name
        self.lakehouse_schema_name = lakehouse_schema_name

        super().__init__(
            catalog_name=self.lakehouse_name, 
            schema_name=self.lakehouse_schema_name, 
            spark_measure_telemetry=spark_measure_telemetry, 
            cost_per_vcore_hour=cost_per_vcore_hour,
            compute_stats_all_cols=compute_stats_all_cols
        )

        self.version: str = f"{self.spark.sparkContext.version} (vhd_name=={self.spark.conf.get('spark.synapse.vhd.name')})"
        self.cost_per_vcore_hour = cost_per_vcore_hour or getattr(self, '_FABRIC_USD_COST_PER_VCORE_HOUR', None)
        self.cost_per_hour = self.get_total_cores() * self.cost_per_vcore_hour

        self.extended_engine_metadata.update({
            'spark_history_url': f"https://{self.spark_configs['spark.trident.pbienv'].lower()}.powerbi.com/workloads/de-ds/sparkmonitor/{self.spark_configs['spark.hadoop.trident.artifact.id']}/{self.spark_configs['spark.hadoop.trident.activity.id']}?ctid={self.spark_configs['spark.hadoop.trident.tenant.id']}",
            'cost_per_hour': Decimal(self.cost_per_hour).quantize(Decimal('0.0000'))
        })

        spark_configs_to_log = {k: v for k, v in self.spark_configs.items() if k in [
            'spark.sql.parquet.vorder.enabled',
            'spark.sql.parquet.vorder.default',
            'spark.microsoft.delta.optimizeWrite.enabled',
            'spark.microsoft.delta.optimizeWrite.binSize',
            'spark.synapse.vegas.useCache',
            'spark.synapse.vegas.cacheSize',
            'spark.native.enabled',
            'spark.gluten.enabled',
            'spark.synapse.vhd.name',
            'spark.synapse.vhd.id',
            'spark.microsoft.delta.stats.collect.extended',
            'spark.microsoft.delta.stats.injection.enabled',
            'spark.microsoft.delta.snapshot.driverMode.enabled',
            'spark.microsoft.delta.stats.collect.extended.property.setAtTableCreation',
            'spark.app.id',
            'spark.cluster.name'
        ]}

        self.extended_engine_metadata.update(spark_configs_to_log)

        self.compute_stats_all_cols = compute_stats_all_cols
        self.run_analyze_after_load = False # Fabric Spark supports auto stats collection
        if self.compute_stats_all_cols:
            # Enable auto stats collection
            self.spark.conf.set("spark.microsoft.delta.stats.collect.extended", "true")
            self.spark.conf.set("spark.microsoft.delta.stats.injection.enabled", "true")
            self.spark.conf.set("spark.microsoft.delta.stats.collect.extended.property.setAtTableCreation", "true")