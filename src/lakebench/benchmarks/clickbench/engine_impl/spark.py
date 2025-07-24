from ....engines.spark import Spark

class SparkClickBench:
    def __init__(self, engine: Spark):
        
        self.engine = engine

    def load_parquet_to_delta(self, table_name: str, source_data_path: str, table_is_precreated: bool = False, context_decorator: str = None):
        """
        Loads the ClickBench parquet data into Delta format using Spark.

        Parameters
        ----------
        source_data_path : str
            Path to the source parquet files.
        """
        from pyspark.sql import functions as sf
        # Load parquet files
        df = self.engine.spark.read.parquet(source_data_path)

        # ClickBench parquet data doesn't annotate the logical type of binary columns, therefore we cast as string.
        binary_cols = [c for c, t in df.dtypes if t == "binary"]
        df = df.withColumns({c: sf.col(c).cast("string") for c in binary_cols})
        # Datetime columns are stored as UNIX Integers and need to be converted explicity
        df = df.withColumn("EventTime", sf.col("EventTime").cast("timestamp"))
        df = df.withColumn("EventDate", sf.date_add(sf.lit("1970-01-01"), sf.col("EventDate")))
        df = df.withColumn("ClientEventTime", sf.col("ClientEventTime").cast("timestamp"))
        df = df.withColumn("LocalEventTime", sf.col("LocalEventTime").cast("timestamp"))

        df.write.format("delta").mode("append").saveAsTable(table_name)

    def execute_sql_query(self, query: str):
        return self.engine.execute_sql_query(query)