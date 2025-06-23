import time
from contextlib import contextmanager
from ..engines.spark import Spark

@contextmanager
def timer(phase: str = "Elapsed time", benchmark_imp: str = None):
    if not hasattr(timer, "results"):
        timer.results = []

    if isinstance(benchmark_imp, Spark):
        benchmark_imp.spark.sparkContext.setJobDescription(phase)
    start = time.time()
    try:
        yield
    finally:
        end = time.time()
        duration = int((end - start) * 1000)
        print(f"{phase}: {(duration / 1000):.2f} seconds")
        if isinstance(benchmark_imp, Spark):
            benchmark_imp.spark.sparkContext.setJobDescription(None)
        timer.results.append((phase, duration))

def _clear_results():
    if hasattr(timer, "results"):
        timer.results = []

timer.clear_results = _clear_results
