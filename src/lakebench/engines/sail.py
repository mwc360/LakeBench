from .base import BaseEngine
from .delta_rs import DeltaRs

import os
import posixpath
from typing import Optional
from importlib.metadata import version

class Sail(BaseEngine):
    """
    Sail Engine for ELT Benchmarks.

    File system support: https://docs.lakesail.com/sail/main/guide/storage/
    """
    SQLGLOT_DIALECT = "spark"
    REQUIRED_READ_ENDPOINT = None
    REQUIRED_WRITE_ENDPOINT = "abfss"
    SUPPORTS_ONELAKE = True
    SUPPORTS_SCHEMA_PREP = False

    def __init__(
        self,
        delta_abfss_schema_path: str,
        cost_per_vcore_hour: Optional[float] = None,
    ):
        """
        Initialize the Sail Engine Configs
        """
        super().__init__()
        from pysail.spark import SparkConnectServer
        from pyspark.sql import SparkSession
        if not hasattr(self, 'sail_server'):
            # create server
            server = SparkConnectServer(port=50051)
            server.start(background=True)
            self.sail_server = server

        if not hasattr(self, 'spark'):
            sail_server_hostname, sail_server_port = self.sail_server.listening_address
            try:
                spark = SparkSession.builder.remote(
                    f"sc://{sail_server_hostname}:{sail_server_port}"
                ).getOrCreate()
                spark.conf.set("spark.sql.warehouse.dir", delta_abfss_schema_path)
                self.spark = spark
            except ImportError as ex:
                raise RuntimeError(
                    "Python kernel restart is required after package upgrade.\nRun `import sys; sys.exit(0)` in a separate cell before initializing Sail engine."
                ) from ex

        self.delta_abfss_schema_path = delta_abfss_schema_path
        self.deltars = DeltaRs()

        if self.delta_abfss_schema_path.startswith("abfss://"):
            if self.is_fabric:
                os.environ["AZURE_STORAGE_TOKEN"] = (
                    self.notebookutils.credentials.getToken("storage")
                )
            if not os.getenv("AZURE_STORAGE_TOKEN"):
                raise ValueError(
                    "Please store bearer token as env variable `AZURE_STORAGE_TOKEN`"
                )

        self.catalog_name = None
        self.schema_name = None

        self.version: str = (
            f"""{version("pysail")} (deltalake=={version("deltalake")})"""
        )
        self.cost_per_vcore_hour = cost_per_vcore_hour or getattr(
            self, "_FABRIC_USD_COST_PER_VCORE_HOUR", None
        )

    def load_parquet_to_delta(
        self,
        parquet_folder_path: str,
        table_name: str,
        table_is_precreated: bool = False,
        context_decorator: Optional[str] = None,
    ):
        (
            self.spark.read.parquet(parquet_folder_path)
            .write.format("delta")
            .mode("overwrite")
            .save(posixpath.join(self.delta_abfss_schema_path, table_name))
        )

    def register_table(self, table_name: str):
        """
        Register a Delta table as temporary view in Sail.
        """
        self.spark.read.format("delta").load(
            posixpath.join(self.delta_abfss_schema_path, table_name)
        ).createOrReplaceTempView(table_name)

    def execute_sql_query(self, query: str, context_decorator: Optional[str] = None):
        """
        Execute a SQL query using Sail.
        """
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
        fact_table = self.deltars.DeltaTable(
            posixpath.join(self.delta_abfss_schema_path, table_name)
        )
        fact_table.optimize.compact()

    def vacuum_table(
        self, table_name: str, retain_hours: int = 168, retention_check: bool = True
    ):
        fact_table = self.deltars.DeltaTable(
            posixpath.join(self.delta_abfss_schema_path, table_name)
        )
        fact_table.vacuum(
            retain_hours, enforce_retention_duration=retention_check, dry_run=False
        )
