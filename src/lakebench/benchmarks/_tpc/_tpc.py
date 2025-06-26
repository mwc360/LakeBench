from typing import Optional
from ..base import BaseBenchmark
from ...utils.timer import timer
from ...utils.query_utils import transpile_and_qualify_query

from ...engines.base import BaseEngine
from ...engines.spark import Spark
from ...engines.duckdb import DuckDB
from ...engines.daft import Daft
from ...engines.polars import Polars

import importlib.resources

class _TPC(BaseBenchmark):
    """
    """
    BENCHMARK_IMPL_REGISTRY = {
        Spark: None,
        DuckDB: None,
        Daft: None,
        Polars: None
    }
    MODE_REGISTRY = ['load', 'query', 'power_test']
    TPC_BENCHMARK_VARIANT = ''
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

    def __init__(
            self, 
            engine: BaseEngine, 
            scenario_name: str,
            query_list: Optional[list],
            parquet_mount_path: Optional[str],
            parquet_abfss_path: Optional[str],
            result_abfss_path: Optional[str],
            save_results: bool = False
            ):
        super().__init__(engine, scenario_name, result_abfss_path, save_results)
        if query_list is not None:
            query_set = set(query_list)
            if not query_set.issubset(self.QUERY_REGISTRY):
                unsupported_queries = query_set - set(self.QUERY_REGISTRY)
                raise ValueError(f"Query list contains unsupported queries: {unsupported_queries}. Supported queries: {self.QUERY_REGISTRY}.")
            self.query_list = query_list
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
        self.timer = timer

        self.engine = engine
        self.scenario_name = scenario_name

        match engine.REQUIRED_READ_ENDPOINT:
            case 'mount':
                if parquet_mount_path is None:
                    raise ValueError(f"parquet_mount_path must be provided for {type(engine).__name__} engine.")
                self.source_data_path = parquet_mount_path
            case 'abfss':
                if parquet_abfss_path is None:
                    raise ValueError(f"parquet_abfss_path must be provided for {type(engine).__name__} engine.")
                self.source_data_path = parquet_abfss_path
            case _:
                if parquet_mount_path is None and parquet_abfss_path is None:
                    raise ValueError(
                        f"Either parquet_mount_path or parquet_abfss_path must be provided for {type(engine).__name__} engine."
                    )
                self.source_data_path = parquet_abfss_path or parquet_mount_path

    def run(self, mode: str = 'power_test'):
        match mode:
            case 'load':
                self._run_load_test()
            case 'query':
                self._run_query_test()
            case 'power_test':
                raise NotImplementedError("Power test mode is not implemented yet.")
            case _:
                raise ValueError(f"Unknown mode '{mode}'. Supported modes: {self.MODE_REGISTRY}.")
            
        results = self.post_results()
        return results

    def _run_load_test(self):
        self.benchmark_impl._prepare_schema() #TBD
        with self.timer(f"Loading TPC{self.TPC_BENCHMARK_VARIANT} Tables", self.benchmark_impl):
            for table_name in self.TABLE_REGISTRY:
                # TBD: Add test_item logging
                self.engine.load_parquet_to_delta(
                    parquet_folder_path=f"{self.source_data_path}/{table_name}", 
                    table_name=table_name
                )

    def _run_query_test(self):
        with self.timer(f"Running TPC{self.TPC_BENCHMARK_VARIANT} Queries", self.benchmark_impl):
            if isinstance(self.engine, (DuckDB, Daft, Polars)):
                for table_name in self.TABLE_REGISTRY:
                    self.engine.register_table(table_name)
            for query_name in self.query_list:
                with importlib.resources.path(f"lakebench.benchmarks.tpc{self.TPC_BENCHMARK_VARIANT.lower()}.resources.queries", f'{query_name}.sql') as query_path:
                    with open(query_path, 'r') as query_file:
                        query = query_file.read()

                prepped_query = transpile_and_qualify_query(
                    query=query, 
                    from_dialect='spark', 
                    to_dialect=self.engine.SQLGLOT_DIALECT, 
                    catalog=self.engine.catalog_name,
                    schema=self.engine.schema_name
                    )
                #  TBD: Add test_item logging
                execute_query = self.engine.execute_sql_query(prepped_query)

    def _run_power_test(self):
        self._run_load_test()
        self._run_query_test()

    def post_results(self):
        result_array = []
        for phase, duration_ms in self.timer.results:
            result_array.append({
                "phase": phase,
                "duration_sec": duration_ms / 1000,  # Convert ms to seconds
                "duration_ms": duration_ms,
                "engine": type(self.engine).__name__,
                "scenario": self.scenario_name,
                "cores": self.engine.get_total_cores()
            })

        if self.save_results:
            if self.result_abfss_path is None:
                raise ValueError("result_abfss_path must be provided if save_results is True.")
            else:
                self.engine.append_array_to_delta(self.result_abfss_path, result_array)

        self.timer.clear_results()
        return result_array