from typing import Optional
from ..base import BaseBenchmark
from ...utils.timer import timer
from .engines.spark import SparkAtomicELT
from .engines.duckdb import DuckDBAtomicELT
from .engines.daft import DaftAtomicELT
from .engines.polars import PolarsAtomicELT

from ...engines.base import BaseEngine
from ...engines.spark import SparkEngine
from ...engines.duckdb import DuckDBEngine
from ...engines.daft import DaftEngine
from ...engines.polars import PolarsEngine


class AtomicELT(BaseBenchmark):
    """
    LightMode: minimal benchmark for quick comparisons.
    Includes basic ELT actions: load data, simple transforms, incremental processing, maintenance jobs, small query.
    """

    BENCHMARK_IMPL_REGISTRY = {
        SparkEngine: SparkAtomicELT,
        DuckDBEngine: DuckDBAtomicELT,
        DaftEngine: DaftAtomicELT,
        PolarsEngine: PolarsAtomicELT
    }

    def __init__(
            self, 
            engine: BaseEngine, 
            scenario_name: str,
            mode: str,
            tpcds_parquet_mount_path: Optional[str] = None,
            tpcds_parquet_abfss_path: Optional[str] = None,
            save_results: bool = False,
            result_abfss_path: Optional[str] = None
            ):
        super().__init__(engine, scenario_name, save_results, result_abfss_path)
        self.MODE_REGISTRY = ['light', 'full']

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
        
        self.storage_paths = {
            "source_data_mount_path": tpcds_parquet_mount_path,
            "source_data_abfss_path": tpcds_parquet_abfss_path,
        }
        self.engine = engine
        self.scenario_name = scenario_name
        self.benchmark_impl = self.benchmark_impl_class(
            self.storage_paths,
            self.engine
        )

    def run(self, mode: str = 'light'):

        match mode:
            case 'light':
                self.run_light_mode()
            case 'full':
                raise NotImplementedError("Full mode is not implemented yet.")
            case _:
                raise ValueError(f"Unknown mode '{mode}'. Supported: {self.MODE_REGISTRY}.")
            
        results = self.post_results()
        return results

    def run_light_mode(self):
        with self.timer('Read parquet, write delta (x5)', self.benchmark_impl):
            for table_name in ['store_sales', 'date_dim', 'store', 'item', 'customer']:
                self.benchmark_impl.load_parquet_to_delta(table_name)

        with self.timer('Create fact table', self.benchmark_impl):
            self.benchmark_impl.create_total_sales_fact()

        with self.timer('Merge 0.1% into fact table (3x)', self.benchmark_impl):
            for _ in range(3):
                self.benchmark_impl.merge_percent_into_total_sales_fact(0.001)

        with self.timer('OPTIMIZE', self.benchmark_impl):
            self.benchmark_impl.optimize_table('total_sales_fact')

        with self.timer('VACUUM', self.benchmark_impl):
            self.benchmark_impl.vacuum_table('total_sales_fact')
        
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
            self.engine.append_array_to_delta(self.result_abfss_path, result_array)

        self.timer.clear_results()
        return result_array