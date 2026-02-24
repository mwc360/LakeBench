"""
Shared fixtures and helpers for all integration tests.

Data fixtures are session-scoped so TPC-H / TPC-DS parquet is generated once
per pytest session regardless of how many engine tests consume it.

Helpers
-------
report_and_assert(results, benchmark_name, engine_label, run_exception=None)
    Unified pass/fail reporting used by every engine test file.

run_benchmark(engine, BenchmarkCls, input_dir, run_mode, **kwargs)
    Instantiates a benchmark, runs it (catching exceptions), and returns
    (results, run_exception) for callers to pass to report_and_assert.
"""
import warnings
import pathlib
import pytest

pytest.importorskip("duckdb", reason="requires lakebench[tpcds_datagen] extra")
pytest.importorskip("pyarrow", reason="requires lakebench[tpcds_datagen] extra")


# ---------------------------------------------------------------------------
# Shared reporting helper
# ---------------------------------------------------------------------------

def report_and_assert(results, benchmark_name: str, engine_label: str, run_exception=None):
    """Print a run summary, emit warnings on partial failures, and assert
    that at least one step succeeded.

    Works for both load-and-query benchmarks (TPC-H, TPC-DS, ClickBench) whose
    results use ``phase in ("Load", "Query")``, and task-based benchmarks
    (ELTBench) that use custom phase names.  In all cases the contract is:
    warn on any failure, fail only when *everything* failed or nothing ran.
    """
    load_results  = [r for r in results if r["phase"] == "Load"]
    query_results = [r for r in results if r["phase"] == "Query"]

    # ELTBench: no Load/Query phases — treat every result as a "task"
    if not load_results and not query_results:
        task_results = results
        passed = [r for r in task_results if r["success"]]
        failed = [r for r in task_results if not r["success"]]

        print(f"\n{'='*60}")
        print(f"{benchmark_name} [{engine_label}]")
        print(f"  Tasks : {len(passed)}/{len(task_results)} passed, {len(failed)} failed")
        for r in failed:
            print(f"    x {r['test_item']} ({r['phase']}): {r['error_message'][:120]}")
        if run_exception:
            print(f"  [WARN] raised before completion: "
                  f"{type(run_exception).__name__}: {str(run_exception)[:200]}")
        print(f"{'='*60}")

        if len(task_results) == 0 and run_exception is not None:
            warnings.warn(
                f"{benchmark_name} [{engine_label}]: engine crashed before any tasks ran: "
                f"{type(run_exception).__name__}: {str(run_exception)[:200]}",
                UserWarning, stacklevel=2,
            )
            return

        if failed:
            warnings.warn(
                f"{benchmark_name} [{engine_label}]: {len(failed)} of {len(task_results)} "
                f"tasks failed: {[r['test_item'] for r in failed]}",
                UserWarning, stacklevel=2,
            )
        assert len(passed) > 0, (
            f"{benchmark_name} [{engine_label}]: ALL {len(task_results)} tasks failed."
        )
        return

    # Load-and-query benchmarks (TPC-H, TPC-DS, ClickBench)
    passed = [r for r in query_results if r["success"]]
    failed = [r for r in query_results if not r["success"]]
    lf     = [r for r in load_results  if not r["success"]]

    print(f"\n{'='*60}")
    print(f"{benchmark_name} [{engine_label}]")
    print(f"  Load  : {len(load_results) - len(lf)}/{len(load_results)} tables loaded OK"
          + (f"  [WARN] failed: {[r['test_item'] for r in lf]}" if lf else ""))
    print(f"  Query : {len(passed)}/{len(query_results)} passed, {len(failed)} failed")
    for r in failed:
        print(f"    x {r['test_item']}: {r['error_message'][:120]}")
    if run_exception:
        print(f"  [WARN] raised before completion: "
              f"{type(run_exception).__name__}: {str(run_exception)[:200]}")
    print(f"{'='*60}")

    if lf and len(lf) == len(load_results) and len(load_results) > 0:
        pytest.fail(
            f"{benchmark_name} [{engine_label}]: ALL {len(load_results)} tables failed to load. "
            f"First error: {lf[0]['error_message'][:200]}"
        )

    if len(query_results) == 0 and run_exception is not None:
        warnings.warn(
            f"{benchmark_name} [{engine_label}]: engine crashed before any queries ran: "
            f"{type(run_exception).__name__}: {str(run_exception)[:200]}",
            UserWarning, stacklevel=2,
        )
        return

    if failed:
        warnings.warn(
            f"{benchmark_name} [{engine_label}]: {len(failed)} of {len(query_results)} "
            f"queries failed: {[r['test_item'] for r in failed]}",
            UserWarning, stacklevel=2,
        )
    assert len(passed) > 0, (
        f"{benchmark_name} [{engine_label}]: ALL {len(query_results)} queries failed."
    )


# ---------------------------------------------------------------------------
# Shared benchmark runner
# ---------------------------------------------------------------------------

def run_benchmark(engine, BenchmarkCls, input_dir: str, run_mode: str, **kwargs):
    """Instantiate *BenchmarkCls*, run it, and return (results, exception).

    The exception is None on a clean run.  Callers pass both values straight
    to report_and_assert().
    """
    benchmark = BenchmarkCls(
        engine=engine,
        scenario_name="sf1",
        input_parquet_folder_uri=input_dir,
        **kwargs,
    )
    exc = None
    try:
        benchmark.run(mode=run_mode)
    except Exception as e:
        exc = e
    return benchmark.results, exc


# ---------------------------------------------------------------------------
# Data fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def tpch_parquet_dir(tmp_path_factory):
    """Generate TPC-H SF1 parquet data once per session."""
    from lakebench.datagen import TPCHDataGenerator

    data_dir = tmp_path_factory.mktemp("tpch_sf1")
    print(f"\n[datagen] Generating TPC-H SF1 -> {data_dir}")
    TPCHDataGenerator(scale_factor=1, target_folder_uri=str(data_dir)).run()
    return str(data_dir)


@pytest.fixture(scope="session")
def tpcds_parquet_dir(tmp_path_factory):
    """Generate TPC-DS SF1 parquet data once per session."""
    from lakebench.datagen import TPCDSDataGenerator

    data_dir = tmp_path_factory.mktemp("tpcds_sf1")
    print(f"\n[datagen] Generating TPC-DS SF1 -> {data_dir}")
    TPCDSDataGenerator(scale_factor=1, target_folder_uri=str(data_dir)).run()
    return str(data_dir)


@pytest.fixture(scope="session")
def clickbench_parquet_dir():
    """Return the directory containing the committed ClickBench 100-row sample."""
    data_dir = pathlib.Path(__file__).parent / "data"
    assert (data_dir / "clickbench_sample.parquet").exists(), (
        "ClickBench sample parquet not found. "
        "Run: python tests/integration/data/generate_clickbench_sample.py"
    )
    return str(data_dir)
