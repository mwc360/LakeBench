from abc import ABC, abstractmethod
from typing import Dict, Type


class BaseBenchmark(ABC):
    BENCHMARK_IMPL_REGISTRY: Dict[Type, Type] = {}

    def __init__(self, engine, scenario_name: str, save_results: bool = False, result_abfss_path: str = None):
        self.engine = engine
        self.scenario_name = scenario_name
        self.save_results = save_results
        self.result_abfss_path = result_abfss_path

    @abstractmethod
    def run(self):
        pass