from abc import ABC, abstractmethod
from typing import Dict, Type, Optional
import uuid
from datetime import datetime
from ..utils.timer import timer
from ..engines.base import BaseEngine

class BaseBenchmark(ABC):
    """
    Abstract base class for defining benchmarks. This class provides a structure for implementing benchmarks
    with a specific engine and scenario, and includes functionality for timing and saving results.

    Attributes
    ----------
    BENCHMARK_IMPL_REGISTRY : Dict[Type, Type]
        A registry for engines that the benchmark supports. If the engine requires a specific implementation
        that doesn't use the engines existing methods, the dictionary will map engines to the specific implementation
        class rather than. If only shared methods are used, the dictionary value will be None.
    engine : object
        The engine used to execute the benchmark.
    scenario_name : str
        The name of the scenario being benchmarked.
    result_abfss_path : Optional[str]
        The path where benchmark results will be saved, if `save_results` is True.
    save_results : bool
        Flag indicating whether to save benchmark results to a Delta table.
    header_detail_dict : dict
        A dictionary containing metadata about the benchmark run, including run ID, datetime, engine type,
        benchmark name, scenario name, total cores, and compute size.
    timer : object
        A timer object used to measure the duration of benchmark phases.
    results : list
        A list to store benchmark results.
        
    Methods
    -------
    run()
        Abstract method that must be implemented by subclasses to define the benchmark logic.
    post_results()
        Processes and saves benchmark results. If `save_results` is True, results are appended to a Delta table
        at the specified `result_abfss_path`. Clears the timer results after processing.
    """
    BENCHMARK_IMPL_REGISTRY: Dict[BaseEngine, Type] = {}

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
        """
        Processes and posts benchmark results, saving them to a specified location if save_results is True.
        This method collects timing results from the benchmark execution, formats them into a 
        structured array, and optionally saves the results to a Delta table. It also clears the timer 
        instance after offloading results to the `self.results` attribute.

        Parameters
        ----------
        None
        
        Notes
        -----
        - If `save_results` is True, the results are appended to the Delta table specified by 
          `result_abfss_path` using the `engine.append_array_to_delta` method.
        - After processing, the results are stored in `self.results` and the timer results are cleared.
        
        Examples
        --------
        >>> benchmark = Benchmark()
        >>> benchmark.post_results()
        # Processes the results and saves them if `save_results` is True.
        # post_results() should be called after each major benchmark phase.
        """
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