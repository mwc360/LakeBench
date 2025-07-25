from .base import BaseEngine
import os
from typing import Optional
import posixpath

class Spark(BaseEngine):
    """
    Spark Engine for ELT Benchmarks.
    """
    SQLGLOT_DIALECT = "spark"
    REQUIRED_READ_ENDPOINT = None
    REQUIRED_WRITE_ENDPOINT = None
    SUPPORTS_ONELAKE = True
    SUPPORTS_SCHEMA_PREP = True
    

    def __init__(
            self,
            catalog_name: Optional[str],
            schema_name: str,
            schema_abfss_path: Optional[str] = None,
            spark_measure_telemetry: bool = False,
            cost_per_vcore_hour: Optional[float] = None,
            compute_stats_all_cols: bool = False
            ):
        """
        Initialize the SparkEngine with a Spark session.
        """
        super().__init__()
        from pyspark.sql import SparkSession
        import pyspark.sql.functions as sf
        self.sf = sf
        self.spark = SparkSession.builder.getOrCreate()
        if spark_measure_telemetry:
            try:
                from sparkmeasure import StageMetrics
                self.capture_metrics = StageMetrics(self.spark)
            except ModuleNotFoundError:
                raise ModuleNotFoundError("`sparkmeasure` is not installed, either disable the `spark_measure_telemetry` flag, run `%pip install sparkmeasure==0.24.0`, or install LakeBench with the sparkmeasure option: `%pip install lakebench[sparkmeasure]`.")
        self.spark_measure_telemetry = spark_measure_telemetry

        self.version: str = self.spark.sparkContext.version

        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.schema_abfss_path = schema_abfss_path
        self.full_catalog_schema_reference : str = f"`{self.catalog_name}`.`{self.schema_name}`" if catalog_name else f"`{self.schema_name}`"
        self.cost_per_vcore_hour = cost_per_vcore_hour
        self.spark_configs = self.__get_spark_session_configs()
        self.extended_engine_metadata.update({
            'parquet.block.size': self.spark.sparkContext._jsc.hadoopConfiguration().get("parquet.block.size"),
        })
        spark_configs_to_log = {k: v for k, v in self.spark_configs.items() if k in [
            'spark.executor.memory',
            'spark.databricks.delta.optimizeWrite.enabled',
            'spark.databricks.delta.optimizeWrite.binSize',
            'spark.sql.autoBroadcastJoinThreshold',
            'spark.sql.sources.parallelPartitionDiscovery.parallelism',
            'spark.sql.cbo.enabled',
            'spark.sql.shuffle.partitions',
            'spark.task.cpus'
        ]}

        self.extended_engine_metadata.update(spark_configs_to_log)

        self.compute_stats_all_cols = compute_stats_all_cols
        self.run_analyze_after_load = self.compute_stats_all_cols

    def __get_spark_session_configs(self) -> dict:
        scala_map = self.spark.conf._jconf.getAll()
        spark_conf_dict = {}
 
        iterator = scala_map.iterator()
        while iterator.hasNext():
            entry = iterator.next()
            key = entry._1()
            value = entry._2()
            spark_conf_dict[key] = value
        return spark_conf_dict

    def create_schema_if_not_exists(self, drop_before_create: bool = True):
        """
        Prepare an empty schema in the lakehouse.
        """
        location_str = f"LOCATION '{posixpath.join(self.schema_abfss_path, self.schema_name)}'" if self.schema_abfss_path is not None else ''

        if drop_before_create:
            self.spark.sql(f"DROP SCHEMA IF EXISTS {self.full_catalog_schema_reference} CASCADE")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.full_catalog_schema_reference} {location_str}")
        self.spark.sql(f"USE {self.full_catalog_schema_reference}")

    def _create_empty_table(self, table_name: Optional[str], ddl: str):
        # Explicitly set the table type to Delta if not already specified
        if 'using ' not in ddl.lower():
            # Find the closing parenthesis of the column definitions
            closing_paren_index = ddl.rfind(")")
            if closing_paren_index != -1:
                # Insert 'USING delta' after the closing parenthesis
                ddl = (
                    ddl[:closing_paren_index + 1]
                    + " using delta"
                    + ddl[closing_paren_index + 1:]
                )

        self.execute_sql_statement(ddl)

    def _convert_generic_to_specific_schema(self, generic_schema: list):
        """
        Convert a generic schema to a specific Spark schema.
        """
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType, BooleanType, TimestampType, MapType, ByteType, ShortType, LongType, DecimalType
        type_mapping = {
            'STRING': StringType(),
            'TIMESTAMP': TimestampType(),
            'TINYINT': ByteType(),
            'SMALLINT': ShortType(),
            'INT': IntegerType(),
            'BIGINT': LongType(),
            'FLOAT': FloatType(),
            'DOUBLE': DoubleType(),
            'DECIMAL(18,10)': DecimalType(18,10),  # Spark does not have a specific Decimal type, using DoubleType
            'BOOLEAN': BooleanType(),
            'MAP<STRING, STRING>': MapType(StringType(), StringType())
        }
        return StructType([StructField(name, type_mapping[data_type], True) for name, data_type in generic_schema])

    def _append_results_to_delta(self, abfss_path: str, results: list, generic_schema: list):
        """
        Append an array to a Delta table.
        """
        import pyspark.sql.functions as sf
        schema = self._convert_generic_to_specific_schema(generic_schema)
        # Use default order of columns in dictionary
        columns = list(results[0].keys())
        df = self.spark.createDataFrame(results, schema=schema).select(*columns)
        df.write.format("delta") \
            .option("mergeSchema", "true") \
            .option("delta.enableDeletionVectors", "false") \
            .option("delta.autoOptimize.autoCompact", "true") \
            .option("delta.autoOptimize.optimizeWrite", "true") \
            .mode("append") \
            .save(abfss_path)

    def get_total_cores(self) -> int:
        """
        Returns the total number of CPU cores available in the Spark cluster.
        
        Assumes that the driver and workers nodes are all the same VM size.
        """
        cores = int(len(set(executor.host() for executor in self.spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos())) * os.cpu_count())
        return cores
        
    def get_compute_size(self) -> str:
        """
        Returns a formatted string with the compute size.
        
        Assumes that the driver and workers nodes are all the same VM size.
        """        
        sc_conf_dict = {key: value for key, value in self.spark.sparkContext.getConf().getAll()}
        executor_count = self.spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1
        executor_cores = int(sc_conf_dict.get('spark.executor.cores', os.cpu_count()))
        vm_host_count = len(set(executor.host() for executor in self.spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos()))
        worker_count = vm_host_count - 1
        worker_cores = os.cpu_count()
        as_min_workers = sc_conf_dict['spark.dynamicAllocation.initialExecutors'] if sc_conf_dict.get('spark.autoscale.executorResourceInfoTag.enabled', 'false') == 'true' else None
        as_max_workers = sc_conf_dict['spark.dynamicAllocation.maxExecutors'] if sc_conf_dict.get('spark.autoscale.executorResourceInfoTag.enabled', 'false') == 'true' else None
        as_enabled = True if as_min_workers != as_max_workers and sc_conf_dict.get('spark.dynamicAllocation.minExecutors', None) != sc_conf_dict.get('spark.dynamicAllocation.maxExecutors', None) else False
        type = "SingleNode" if vm_host_count == 1 and not as_enabled else 'MultiNode'
        workers_word = 'Workers' if worker_count > 1 or (as_max_workers is not None and int(as_max_workers) > 1)  else 'Worker'
        executors_per_worker = int(executor_count / worker_count) if worker_count > 0 else 1
        executors_word = 'Executors' if executors_per_worker > 1 else 'Executor'
        executor_str = f"({executors_per_worker} x {executor_cores}vCore {executors_word}{' ea.' if type != 'SingleNode' else ''})"

        if type == 'SingleNode':
            cluster_config = f"{worker_cores}vCore {type} {executor_str}"
        elif as_enabled:
            cluster_config = f"{as_min_workers}-{as_max_workers} x {worker_cores}vCore {workers_word} {executor_str}"
        else:
            cluster_config = f"{worker_count} x {worker_cores}vCore {workers_word} {executor_str}"

        return cluster_config
    
    def load_parquet_to_delta(self, parquet_folder_path: str, table_name: str, table_is_precreated: bool = False, context_decorator: Optional[str] = None):
        df = self.spark.read.parquet(parquet_folder_path)
        if table_is_precreated:
            df.write.insertInto(table_name, overwrite=True)
        else:
            df.write.format('delta').mode("append").saveAsTable(table_name)

        if self.run_analyze_after_load:
            self.spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS FOR ALL COLUMNS;")    

    def execute_sql_query(self, query: str, context_decorator: Optional[str] = None):
        execute_sql = self.spark.sql(query).collect()
    
    def execute_sql_statement(self, statement: str, context_decorator: Optional[str] = None):
        """
        Execute a SQL statement.

        Parameters
        ----------
        statement : str
            The SQL statement to execute.
        context_decorator : Optional[str]
            Not used by Spark, a job description is set instead.

        """
        self.spark.sql(statement)

    def optimize_table(self, table_name: str):
        self.spark.sql(f"OPTIMIZE {self.full_catalog_schema_reference}.{table_name}")

    def vacuum_table(self, table_name: str, retain_hours: int = 168, retention_check: bool = True):
        self.spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", retention_check)
        self.spark.sql(f"VACUUM {self.full_catalog_schema_reference}.{table_name} RETAIN {retain_hours} HOURS")
