from abc import ABC, abstractmethod
from typing import Dict, Type, Optional


class BaseBenchmark(ABC):
    BENCHMARK_IMPL_REGISTRY: Dict[Type, Type] = {}

    def __init__(self, engine, scenario_name: str, result_abfss_path: Optional[str], save_results: bool = False):
        self.engine = engine
        self.scenario_name = scenario_name
        self.result_abfss_path = result_abfss_path
        self.save_results = save_results

    @abstractmethod
    def run(self):
        pass