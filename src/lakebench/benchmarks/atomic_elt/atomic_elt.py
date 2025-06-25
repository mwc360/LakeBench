from typing import Optional
from ..base import BaseBenchmark
from ...utils.timer import timer
from .engines.spark import SparkAtomicELT
from .engines.duckdb import DuckDBAtomicELT
from .engines.daft import DaftAtomicELT
from .engines.polars import PolarsAtomicELT

from ...engines.base import BaseEngine
from ...engines.spark import Spark
from ...engines.duckdb import DuckDB
from ...engines.daft import Daft
from ...engines.polars import Polars


class AtomicELT(BaseBenchmark):
    """
    LightMode: minimal benchmark for quick comparisons.
    Includes basic ELT actions: load data, simple transforms, incremental processing, maintenance jobs, small query.
    """

    BENCHMARK_IMPL_REGISTRY = {
        Spark: SparkAtomicELT,
        DuckDB: DuckDBAtomicELT,
        Daft: DaftAtomicELT,
        Polars: PolarsAtomicELT
    }

    def __init__(
            self, 
            engine: BaseEngine, 
            scenario_name: str,
            tpcds_parquet_mount_path: Optional[str],
            tpcds_parquet_abfss_path: Optional[str],
            result_abfss_path: Optional[str],
            save_results: bool = False
            ):
        super().__init__(engine, scenario_name, result_abfss_path, save_results)
        self.MODE_REGISTRY = ['light', 'full']
        self.TABLE_REGISTRY = [
            'call_center', 'catalog_page', 'catalog_returns', 'catalog_sales',
            'customer', 'customer_address', 'customer_demographics', 'date_dim',
            'household_demographics', 'income_band', 'inventory', 'item',
            'promotion', 'reason', 'ship_mode', 'store', 'store_returns',
            'store_sales', 'time_dim', 'warehouse', 'web_page', 'web_returns',
            'web_sales', 'web_site'
        ] 
        self.benchmark_impl_class = next(
            (benchmark_impl for base_engine, benchmark_impl in self.BENCHMARK_IMPL_REGISTRY.items() if isinstance(engine, base_engine)),
            None
        )
        self.timer = timer

        if self.benchmark_impl_class is None:
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

    def run(self, mode: str = 'light'):

        match mode:
            case 'light':
                self.run_light_mode()
            case 'full':
                raise NotImplementedError("Full mode is not implemented yet.")
            case _:
                raise ValueError(f"Mode '{mode}' is not supported. Supported modes: {self.MODE_REGISTRY}.")

        results = self.post_results()
        return results

    def run_light_mode(self):
        with self.timer('Read parquet, write delta (x5)', self.benchmark_impl):
            for table_name in ('store_sales', 'date_dim', 'store', 'item', 'customer'):
                self.engine.load_parquet_to_delta(
                    parquet_folder_path=f"{self.source_data_path}/{table_name}", 
                    table_name=table_name
                )

        with self.timer('Create fact table', self.benchmark_impl):
            self.benchmark_impl.create_total_sales_fact()

        with self.timer('Merge 0.1% into fact table (3x)', self.benchmark_impl):
            for _ in range(3):
                self.benchmark_impl.merge_percent_into_total_sales_fact(0.001)

        with self.timer('OPTIMIZE', self.benchmark_impl):
            self.engine.optimize_table('total_sales_fact')

        with self.timer('VACUUM', self.benchmark_impl):
            self.engine.vacuum_table('total_sales_fact', retain_hours=0, retention_check=False)
        
        with self.timer('Ad-hoc query (small result aggregation)', self.benchmark_impl):
            self.benchmark_impl.query_total_sales_fact()

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