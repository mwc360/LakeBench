from typing import List, Optional
from ..base import BaseBenchmark
from ...utils.query_utils import transpile_and_qualify_query

from ...engines.base import BaseEngine
from ...engines.spark import Spark
from ...engines.duckdb import DuckDB
from ...engines.daft import Daft
from ...engines.polars import Polars

import importlib.resources
import inspect
import posixpath

class _LoadAndQuery(BaseBenchmark):
    """
    Base class for benchmarks that only have a simple Load and Query phase (TPC-H, TPC-DS, ClickBench). 
    PLEASE DO NOT INSTANTIATE THIS CLASS DIRECTLY. Use the subclasses instead. 
    """
    BENCHMARK_IMPL_REGISTRY = {
        Spark: None,
        DuckDB: None,
        Daft: None,
        Polars: None
    }
    MODE_REGISTRY = ['load', 'query', 'power_test', 'load_and_query']
    BENCHMARK_NAME = ''
    TABLE_REGISTRY = [
        'call_center', 'catalog_page', 'catalog_returns', 'catalog_sales',
        'customer', 'customer_address', 'customer_demographics', 'date_dim',
        'household_demographics', 'income_band', 'inventory', 'item',
        'promotion', 'reason', 'ship_mode', 'store', 'store_returns',
        'store_sales', 'time_dim', 'warehouse', 'web_page', 'web_returns',
        'web_sales', 'web_site'
    ]
    QUERY_REGISTRY = [
        'q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8', 'q9', 'q10',
        'q11', 'q12', 'q13', 'q14a', 'q14b', 'q15', 'q16', 'q17', 'q18', 'q19', 'q20',
        'q21', 'q22', 'q23a', 'q23b', 'q24a', 'q24b', 'q25', 'q26', 'q27', 'q28', 'q29', 'q30',
        'q31', 'q32', 'q33', 'q34', 'q35', 'q36', 'q37', 'q38', 'q39a', 'q39b', 'q40',
        'q41', 'q42', 'q43', 'q44', 'q45', 'q46', 'q47', 'q48', 'q49', 'q50',
        'q51', 'q52', 'q53', 'q54', 'q55', 'q56', 'q57', 'q58', 'q59', 'q60',
        'q61', 'q62', 'q63', 'q64', 'q65', 'q66', 'q67', 'q68', 'q69', 'q70',
        'q71', 'q72', 'q73', 'q74', 'q75', 'q76', 'q77', 'q78', 'q79', 'q80',
        'q81', 'q82', 'q83', 'q84', 'q85', 'q86', 'q87', 'q88', 'q89', 'q90',
        'q91', 'q92', 'q93', 'q94', 'q95', 'q96', 'q97', 'q98', 'q99'
    ]
    DDL_FILE_NAME = ''
    VERSION = ''

    def __init__(
            self, 
            engine: BaseEngine, 
            scenario_name: str,
            scale_factor: Optional[int] = None,
            query_list: Optional[List[str]] = None,
            parquet_mount_path: Optional[str] = None,
            parquet_abfss_path: Optional[str] = None,
            result_abfss_path: Optional[str] = None,
            save_results: bool = False,
            run_id: Optional[str] = None
            ):
        self.scale_factor = scale_factor
        super().__init__(engine, scenario_name, result_abfss_path, save_results, run_id=run_id)
        if query_list is not None:
            expanded_query_list = []
            for query in query_list:
                if query == '*':
                    expanded_query_list.extend(self.QUERY_REGISTRY)  # Replace '*' with all queries
                else:
                    expanded_query_list.append(query)
            query_set = set(expanded_query_list)
            if not query_set.issubset(self.QUERY_REGISTRY):
                unsupported_queries = query_set - set(self.QUERY_REGISTRY)
                raise ValueError(f"Query list contains unsupported queries: {unsupported_queries}. Supported queries: {self.QUERY_REGISTRY}.")
            self.query_list = expanded_query_list
        else:
            self.query_list = self.QUERY_REGISTRY

        for base_engine, benchmark_impl in self.BENCHMARK_IMPL_REGISTRY.items():
            if isinstance(engine, base_engine):
                self.benchmark_impl_class = benchmark_impl
                break
        else:
            raise ValueError(
                f"No benchmark implementation registered for engine type: {type(engine).__name__} "
                f"in benchmark '{self.__class__.__name__}'."
            )

        self.engine = engine
        self.scenario_name = scenario_name

        if engine.REQUIRED_READ_ENDPOINT == 'mount':
            if parquet_mount_path is None:
                raise ValueError(f"parquet_mount_path must be provided for {type(engine).__name__} engine.")
            self.source_data_path = parquet_mount_path
        elif engine.REQUIRED_READ_ENDPOINT == 'abfss':
            if parquet_abfss_path is None:
                raise ValueError(f"parquet_abfss_path must be provided for {type(engine).__name__} engine.")
            self.source_data_path = parquet_abfss_path
        else:
            if parquet_mount_path is None and parquet_abfss_path is None:
                raise ValueError(
                    f"Either parquet_mount_path or parquet_abfss_path must be provided for {type(engine).__name__} engine."
                )
            self.source_data_path = parquet_abfss_path or parquet_mount_path

        self.benchmark_impl = self.benchmark_impl_class(self.engine) if self.benchmark_impl_class is not None else None

    def run(self, mode: str = 'power_test'):
        """
        Executes a specific test mode based on the provided mode string.

        Parameters
        ----------
        mode : str, optional
            The mode of the test to run. Supported modes are:
            - 'load': Executes the load test.
            - 'query': Executes the query test.
            - 'power_test': Executes the power test (default).
            - 'load_and_query': Alias for 'power_test', runs both load and query tests.

        Notes
        -----
        The `MODE_REGISTRY` attribute contains the list of supported modes.
        """
        self.mode = 'load_and_query' if mode in ('power_test', 'load_and_query') else mode

        if mode == 'load':
            self._run_load_test()
        elif mode == 'query':
            self._run_query_test()
        elif mode in ('power_test', 'load_and_query'):
            self._run_power_test()
        else:
            raise ValueError(f"Unknown mode '{mode}'. Supported modes: {self.MODE_REGISTRY}.")
    
    def _prepare_schema(self):
        """
        Prepares the database schema for the benchmark.
        This method creates the schema if it does not exist, optionally dropping it before creation.
        It then reads the DDL (Data Definition Language) file associated with the specific benchmark,
        parses the SQL statements, and executes them to set up the schema.

        Parameters
        ----------
        None

        Notes
        -----
        - The DDL file is expected to be located in the `resources.ddl` directory corresponding to the TPC benchmark variant.
        """
        self.engine.create_schema_if_not_exists(drop_before_create=True)
        with importlib.resources.path(f"lakebench.benchmarks.{self.BENCHMARK_NAME.lower()}.resources.ddl", self.DDL_FILE_NAME) as ddl_path:
            with open(ddl_path, 'r') as ddl_file:
                ddl = ddl_file.read()
            
        statements = [s for s in ddl.split(';') if len(s) > 7]
        for statement in statements:
            if 'using ' not in statement.lower():
                # Find the closing parenthesis of the column definitions
                closing_paren_index = statement.rfind(")")
                if closing_paren_index != -1:
                    # Insert 'USING delta' after the closing parenthesis
                    statement = (
                        statement[:closing_paren_index + 1]
                        + " using delta"
                        + statement[closing_paren_index + 1:]
                    )

            prepped_ddl = transpile_and_qualify_query(
                query=statement, 
                from_dialect='spark', 
                to_dialect=self.engine.SQLGLOT_DIALECT, 
                catalog=self.engine.catalog_name,
                schema=self.engine.schema_name
            )
            
            self.engine.execute_sql_statement(prepped_ddl)

    def _run_load_test(self):
        """
        Executes the load test by loading data from Parquet files into Delta tables 
        for all tables registered in the `TABLE_REGISTRY`. This method also measures 
        the time taken for each table load operation and records the results.

        Parameters
        ----------
        None

        Notes
        -----
        - If the engine is an instance of `Spark`, the schema is prepared before 
          loading the data.
        - The method uses a timer to measure the duration of the load operation 
          for each table.
        - Results are posted after all tables have been processed.
        """
        # set the mode if the module is being called directly
        if inspect.currentframe().f_back.f_code.co_name not in ('run', '_run_power_test'):
            self.mode = 'load'

        if self.engine.SUPPORTS_SCHEMA_PREP:
            self._prepare_schema()
        for table_name in self.TABLE_REGISTRY:
            with self.timer(phase="Load", test_item=table_name, engine=self.engine) as tc:
                if self.benchmark_impl is not None:
                    # If a specific benchmark implementation is defined, use it to load the table
                    tc.execution_telemetry = self.benchmark_impl.load_parquet_to_delta(
                        table_name=table_name, 
                        source_data_path=self.source_data_path,
                        table_is_precreated=True
                    )
                else:
                    # Otherwise, use the generic load method
                    tc.execution_telemetry = self.engine.load_parquet_to_delta(
                        parquet_folder_path=posixpath.join(self.source_data_path, f"{table_name}/"), 
                        table_name=table_name,
                        table_is_precreated=True
                    )
        self.post_results()

    def _run_query_test(self):
        """
        Executes a series of SQL queries defined in the `query_list` attribute.
        """
        # set the mode if the module is being called directly
        if inspect.currentframe().f_back.f_code.co_name not in ('run', '_run_power_test'):
            self.mode = 'query'

        if isinstance(self.engine, (DuckDB, Daft, Polars)):
            for table_name in self.TABLE_REGISTRY:
                self.engine.register_table(table_name)
        for query_name in self.query_list:
            prepped_query = self._return_query_definition(query_name)
            with self.timer(phase="Query", test_item=query_name, engine=self.engine) as tc:
                if self.benchmark_impl is not None:
                    # If a specific benchmark implementation is defined, use it to perform the query
                    tc.execution_telemetry = self.benchmark_impl.execute_sql_query(prepped_query)
                else:
                    # Otherwise, use the generic query method
                    tc.execution_telemetry = self.engine.execute_sql_query(prepped_query)
        self.post_results()

    def _run_power_test(self):
        """
        Executes the full benchmark by running both the load and query phases.

        This method orchestrates:
        1. Load phase: Loads data into the target system.
        2. Query phase: Executes configured SQL queries to evaluate performance.
        """
        self.mode = 'load_and_query'

        self._run_load_test()
        self._run_query_test()

    def _return_query_definition(self, query_name: str) -> str:
        """
        Returns the SQL definition for a given query name.

        Parameters
        ----------
        query_name : str
            The name of the query to retrieve.

        Returns
        -------
        str
            The SQL definition for the specified query.
        """
        with importlib.resources.path(f"lakebench.benchmarks.{self.BENCHMARK_NAME.lower()}.resources.queries", f'{query_name}.sql') as query_path:
            with open(query_path, 'r') as query_file:
                query = query_file.read()

        prepped_query = transpile_and_qualify_query(
            query=query, 
            from_dialect='spark', 
            to_dialect=self.engine.SQLGLOT_DIALECT, 
            catalog=self.engine.catalog_name,
            schema=self.engine.schema_name
            )
        return prepped_query