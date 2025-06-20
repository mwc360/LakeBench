from .base import BaseEngine

class SparkEngine(BaseEngine):
    """
    Spark Engine for ELT Benchmarks.
    """

    def __init__(
            self,
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

        self.full_catalog_schema_reference : str = None  # This will be set in the subclass or later


    def create_schema_if_not_exists(self, drop_before_create: bool = True):
        """
        Prepare an empty schema in the lakehouse.
        """
        if drop_before_create:
            self.spark.sql(f"DROP SCHEMA IF EXISTS {self.full_catalog_schema_reference}")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.full_catalog_schema_reference}")


    def append_array_to_delta(self, abfss_path: str, array: list):
        """
        Append an array to a Delta table.
        """
        df = self.spark.createDataFrame(array)
        df.write.option("mergeSchema", "true").option("delta.enableDeletionVectors", "false").format("delta").mode("append").save(abfss_path)