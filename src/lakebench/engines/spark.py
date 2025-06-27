from .base import BaseEngine

class Spark(BaseEngine):
    """
    Spark Engine for ELT Benchmarks.
    """
    SQLGLOT_DIALECT = "spark"
    REQUIRED_READ_ENDPOINT = None
    REQUIRED_WRITE_ENDPOINT = None

    def __init__(
            self,
            catalog_name: str,
            schema_name: str,
            spark_measure_telemetry: bool = False
            ):
        """
        Initialize the SparkEngine with a Spark session.
        """
        from pyspark.sql import SparkSession
        if spark_measure_telemetry:
            from sparkmeasure import StageMetrics
        self.spark_measure_telemetry = spark_measure_telemetry

        import pyspark.sql.functions as sf
        self.sf = sf
        self.spark = SparkSession.builder.getOrCreate()

        self.full_catalog_schema_reference : str = f"`{self.catalog_name}`.`{self.schema_name}`"
        self.catalog_name = catalog_name
        self.schema_name = schema_name

    def create_schema_if_not_exists(self, drop_before_create: bool = True):
        """
        Prepare an empty schema in the lakehouse.
        """
        if drop_before_create:
            self.spark.sql(f"DROP SCHEMA IF EXISTS {self.full_catalog_schema_reference}")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.full_catalog_schema_reference}")
        self.spark.sql(f"USE {self.full_catalog_schema_reference}")

    def append_array_to_delta(self, abfss_path: str, array: list):
        """
        Append an array to a Delta table.
        """
        df = self.spark.createDataFrame(array)
        df.write.option("mergeSchema", "true").option("delta.enableDeletionVectors", "false").format("delta").mode("append").save(abfss_path)

    def get_total_cores(self):
        import os 
        cores = len(set(executor.host() for executor in self.spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos())) * os.cpu_count()
        return cores
    
    def get_compute_size(self):
        import os 
        sc_conf_dict = {key: value for key, value in self.spark.sparkContext.getConf().getAll()}
        workers = self.spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1
        worker_cores = int(sc_conf_dict.get('spark.executor.cores'))
        executor_hosts = set(executor.host() for executor in self.spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos())
        as_min_workers = sc_conf_dict['spark.dynamicAllocation.initialExecutors'] if sc_conf_dict.get('spark.autoscale.executorResourceInfoTag.enabled', 'false') == 'true' else None
        as_max_workers = sc_conf_dict['spark.dynamicAllocation.maxExecutors'] if sc_conf_dict.get('spark.autoscale.executorResourceInfoTag.enabled', 'false') == 'true' else None
        as_enabled = True if as_min_workers != as_max_workers else False
        type = "SingleNode" if len(executor_hosts) == 1 and not as_enabled else 'MultiNode'

        workers_str = 'Workers' if workers > 1 or int(as_max_workers) > 1  else 'Worker'
        if type == 'SingleNode':
            cluster_config = f"{os.cpu_count()}vCore {type} ({worker_cores} Executor Cores)"
        elif as_enabled:
            cluster_config = f"{as_min_workers}-{as_max_workers} x {worker_cores}vCore {workers_str}"
        else:
            cluster_config = f"{workers} x {worker_cores}vCores {workers_str}"

        return cluster_config
    
    def load_parquet_to_delta(self, parquet_folder_path: str, table_name: str):
        df = self.spark.read.parquet(parquet_folder_path)
        df.write.mode("append").saveAsTable(table_name)
    
    def execute_sql_query(self, query: str):
        execute_sql = self.spark.sql(query).collect()
    
    def execute_sql_statement(self, statement: str):
        """
        Execute a SQL statement.
        """
        self.spark.sql(statement)

    def optimize_table(self, table_name: str):
        self.spark.sql(f"OPTIMIZE {self.full_catalog_schema_reference}.{table_name}")

    def vacuum_table(self, table_name: str, retain_hours: int = 168, retention_check: bool = True):
        self.spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", {retention_check})
        self.spark.sql(f"VACUUM {self.full_catalog_schema_reference}.{table_name} RETAIN {retain_hours} HOURS")
