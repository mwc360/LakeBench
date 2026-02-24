# Integration Tests

Each test file runs all supported benchmarks for a single engine.  
Tests are marked `@pytest.mark.integration` and are **not** collected by default — you must opt in via the commands below.

> **Scale factor:** Data is generated at SF 0.1 (≈ 10% of SF 1) to keep CI runs fast.

---

## Prerequisites

- [uv](https://docs.astral.sh/uv/) installed
- Java 17+ on `PATH` (Spark / Sail only)

---

## Running a single engine

### DuckDB
```bash
uv sync --group dev --extra duckdb --extra tpch_datagen --extra tpcds_datagen
uv run pytest tests/integration/test_tpc_duckdb.py -v -s
```

### Daft
```bash
uv sync --group dev --extra daft --extra tpch_datagen --extra tpcds_datagen
uv run pytest tests/integration/test_tpc_daft.py -v -s
```

### Polars
```bash
uv sync --group dev --extra polars --extra tpch_datagen --extra tpcds_datagen
uv run pytest tests/integration/test_tpc_polars.py -v -s
```

### Spark
> `spark` and `sail` extras are mutually exclusive — use a separate venv if you need both.
```bash
uv sync --group dev --extra spark --extra tpch_datagen --extra tpcds_datagen
uv run pytest tests/integration/test_tpc_spark.py -v -s
```

### Sail
```bash
uv sync --group dev --extra sail --extra tpch_datagen --extra tpcds_datagen
uv run pytest tests/integration/test_tpc_sail.py -v -s
```

---

## Running all non-JVM engines together

DuckDB, Daft, and Polars share no conflicts and can run in one sync:

```bash
uv sync --group dev --extra duckdb --extra daft --extra polars \
        --extra tpch_datagen --extra tpcds_datagen
uv run pytest tests/integration/test_tpc_duckdb.py \
              tests/integration/test_tpc_daft.py \
              tests/integration/test_tpc_polars.py -v -s
```

---

## Benchmarks per engine

| Benchmark  | DuckDB | Daft | Polars | Spark | Sail |
|------------|:------:|:----:|:------:|:-----:|:----:|
| TPC-H      | ✅     | ✅   | ✅     | ✅    | ✅   |
| TPC-DS     | ✅     | ✅   | ✅     | ✅    | ✅   |
| ClickBench | ✅     | —    | —      | ✅    | ✅   |
| ELTBench   | ✅     | ✅   | ✅     | ✅    | ✅   |

ClickBench uses the committed 100-row sample at `tests/integration/data/clickbench_sample.parquet`.

---

## Pass / fail semantics

- **Individual query failures** → `UserWarning`, test still passes.  
- **All queries fail** or **all tables fail to load** → test fails.  
- **Engine crash before any results** → `UserWarning`, test still passes (graceful degradation).
