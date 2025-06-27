from typing import Optional
from ..base import BaseBenchmark

from .engine_impl.spark import SparkELTBench
from .engine_impl.duckdb import DuckDBELTBench
from .engine_impl.daft import DaftELTBench
from .engine_impl.polars import PolarsELTBench

from ...engines.base import BaseEngine
from ...engines.spark import Spark
from ...engines.duckdb import DuckDB
from ...engines.daft import Daft
from ...engines.polars import Polars

import posixpath


class ELTBench(BaseBenchmark):
    """
    LightMode: minimal benchmark for quick comparisons.
    Includes basic ELT actions: load data, simple transforms, incremental processing, maintenance jobs, small query.
    """

    BENCHMARK_IMPL_REGISTRY = {
        Spark: SparkELTBench,
        DuckDB: DuckDBELTBench,
        Daft: DaftELTBench,
        Polars: PolarsELTBench
    }
    MODE_REGISTRY = ['light', 'full']
    TABLE_REGISTRY = [
        'call_center', 'catalog_page', 'catalog_returns', 'catalog_sales',
        'customer', 'customer_address', 'customer_demographics', 'date_dim',
        'household_demographics', 'income_band', 'inventory', 'item',
        'promotion', 'reason', 'ship_mode', 'store', 'store_returns',
        'store_sales', 'time_dim', 'warehouse', 'web_page', 'web_returns',
        'web_sales', 'web_site'
    ]

    def __init__(
            self, 
            engine: BaseEngine, 
            scenario_name: str,
            tpcds_parquet_mount_path: Optional[str] = None,
            tpcds_parquet_abfss_path: Optional[str] = None,
            result_abfss_path: Optional[str] = None,
            save_results: bool = False
            ):
        super().__init__(engine, scenario_name, result_abfss_path, save_results)
        for base_engine, benchmark_impl in self.BENCHMARK_IMPL_REGISTRY.items():
            if isinstance(engine, base_engine):
                self.benchmark_impl_class = benchmark_impl
                if self.benchmark_impl_class is None:
                    raise ValueError(
                        f"No benchmark implementation registered for engine type: {type(engine).__name__} "
                        f"in benchmark '{self.__class__.__name__}'."
                    )
                break
        else:
            raise ValueError(
                f"No benchmark implementation registered for engine type: {type(engine).__name__} "
                f"in benchmark '{self.__class__.__name__}'."
            )
        
        if isinstance(engine, Daft):
            if tpcds_parquet_mount_path is None:
                raise ValueError("parquet_mount_path must be provided for Daft engine.")
        self.source_data_path = tpcds_parquet_mount_path or tpcds_parquet_abfss_path
        self.engine = engine
        self.scenario_name = scenario_name
        self.benchmark_impl = self.benchmark_impl_class(
            self.engine
        )

        match engine.REQUIRED_READ_ENDPOINT:
            case 'mount':
                if tpcds_parquet_mount_path is None:
                    raise ValueError(f"parquet_mount_path must be provided for {type(engine).__name__} engine.")
                self.source_data_path = tpcds_parquet_mount_path
            case 'abfss':
                if tpcds_parquet_abfss_path is None:
                    raise ValueError(f"parquet_abfss_path must be provided for {type(engine).__name__} engine.")
                self.source_data_path = tpcds_parquet_abfss_path
            case _:
                if tpcds_parquet_mount_path is None and tpcds_parquet_abfss_path is None:
                    raise ValueError(
                        f"Either parquet_mount_path or parquet_abfss_path must be provided for {type(engine).__name__} engine."
                    )
                self.source_data_path = tpcds_parquet_abfss_path or tpcds_parquet_mount_path

    def run(self, mode: str = 'light'):

        match mode:
            case 'light':
                self.run_light_mode()
            case 'full':
                raise NotImplementedError("Full mode is not implemented yet.")
            case _:
                raise ValueError(f"Mode '{mode}' is not supported. Supported modes: {self.MODE_REGISTRY}.")

    def run_light_mode(self):
        for table_name in ('store_sales', 'date_dim', 'store', 'item', 'customer'):
            with self.timer(phase="Read parquet, write delta (x5)", test_item=table_name, engine=self.engine):
                self.engine.load_parquet_to_delta(
                    parquet_folder_path=posixpath.join(self.source_data_path, table_name), 
                    table_name=table_name
                )
        with self.timer(phase="Create fact table", test_item='total_sales_fact', engine=self.engine):
            self.benchmark_impl.create_total_sales_fact()

        for _ in range(3):
            with self.timer(phase="Merge 0.1% into fact table (3x)", test_item='total_sales_fact', engine=self.engine):
                self.benchmark_impl.merge_percent_into_total_sales_fact(0.001)

        with self.timer(phase="OPTIMIZE", test_item='total_sales_fact', engine=self.engine):
            self.engine.optimize_table('total_sales_fact')

        with self.timer(phase="VACUUM", test_item='total_sales_fact', engine=self.engine):
            self.engine.vacuum_table('total_sales_fact', retain_hours=0, retention_check=False)

        with self.timer(phase="Ad-hoc query (small result aggregation)", test_item='total_sales_fact', engine=self.engine):
            self.benchmark_impl.query_total_sales_fact()

        self.post_results()

