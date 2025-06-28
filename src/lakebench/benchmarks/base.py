from abc import ABC, abstractmethod
from typing import Dict, Type, Optional
import uuid
from datetime import datetime
from ..utils.timer import timer

class BaseBenchmark(ABC):
    BENCHMARK_IMPL_REGISTRY: Dict[Type, Type] = {}

    def __init__(self, engine, scenario_name: str, result_abfss_path: Optional[str], save_results: bool = False):
        self.engine = engine
        self.scenario_name = scenario_name
        self.result_abfss_path = result_abfss_path
        self.save_results = save_results
        self.header_detail_dict = {
            'run_id': str(uuid.uuid1()),
            'run_datetime': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
            'engine': type(engine).__name__,
            'benchmark': self.__class__.__name__,
            'scenario': scenario_name,
            'total_cores': self.engine.get_total_cores(),
            'compute_size': self.engine.get_compute_size()
        }
        self.timer = timer
        self.results = []

    @abstractmethod
    def run(self):
        pass

    def post_results(self):
        result_array = [
            {
                **self.header_detail_dict,
                'phase': phase,
                'test_item': test_item,
                'start_datetime': start_datetime,
                "duration_sec": duration_ms / 1000,
                'duration_ms': duration_ms,
                'iteration': iteration,
                'success': success,
                'error_message': error_message
            }
            for phase, test_item, start_datetime, duration_ms, iteration, success, error_message in self.timer.results
        ]

        if self.save_results:
            if self.result_abfss_path is None:
                raise ValueError("result_abfss_path must be provided if save_results is True.")
            else:
                self.engine.append_array_to_delta(self.result_abfss_path, result_array)

        self.results.append(result_array)
        self.timer.clear_results()