# LakeBench

🌊 LakeBench is the first Python-based, multi-modal benchmarking framework designed to evaluate performance across multiple lakehouse compute engines and ELT scenarios. Supporting a variety of engines and both industry-standard and novel benchmarks, LakeBench enables comprehensive, apples-to-apples comparisons in a single, extensible Python library.

Most existing benchmarks (like TPC-DS and TPC-H) are too query-heavy and miss the reality that data engineers build complex **ELT pipelines** — not just run analytic queries. While these traditional benchmarks are helpful for testing bulk loading and complex SQL execution, they do not reflect the broader data lifecycle that lakehouse systems must support.

**LakeBench bridges this gap by introducing novel benchmarks that aim to capture the growing spectrum of ELT workflows.** In addition to supporting industry standards like TPC-DS and TPC-H, LakeBench includes scenarios that measure not only query performance, but also data loading, transformation, incremental processing, and maintenance operations. This holistic approach enables you to benchmark engines on the real-world tasks that matter most for modern data engineering.

> LakeBench proposes that **the entire end-to-end data lifecycle managed by data engineers is relevant**: data loading, bulk and incremental transformations, maintenance jobs, and ad-hoc analytical queries. By benchmarking these stages, LakeBench delivers actionable insights into engine efficiency, performance, and operational trade-offs across the full data pipeline.

---

## 🧱 Key Features

- **Modular engine support** (Spark, DuckDB, Polars, Daft)
- **Benchmark scenarios** that reflect real-world ELT workflows
- **Atomic units of work** that benchmark discrete lifecycle stages
- **Configurable execution** to isolate engine behaviors
- COMING SOON: **Custom result logging** and metrics capture (e.g. SparkMeasure)

---

## 🔍 Benchmark Scenarios

LakeBench currently supports one benchmark with more to come:

- **ELTBench**: An benchmark with various modes (`light`, `full`) that simulates typicaly ELT workloads:
  - Raw data load (Parquet → Delta)
  - Fact table generation
  - Incremental merge processing
  - Table maintenance (e.g. OPTIMIZE/VACUUM)
  - Ad-hoc analytical queries
- **AtomicELT** (_COMING SOON)_: A derivative of _ELTBench_ that focuses on the performance of individual ELT operations. Each operation type is executed only once, allowing for granular comparison of engine performance on specific tasks. Results should be interpreted per operation, not as a cumulative runtime.
- **[TPC-DS](https://www.tpc.org/tpcds/)**: An industry-standard benchmark for complex analytical queries, featuring 24 source tables and 99 queries. Designed to simulate decision support systems and analytics workloads.
- **[TPC-H](https://www.tpc.org/tpch/)**: Focuses on ad-hoc decision support with 8 tables and 22 queries, evaluating performance on business-oriented analytical workloads.

---

## 🛠️ Engine Support Matrix

LakeBench supports multiple lakehouse compute engines. Each benchmark scenario declares which engines it supports via `<BenchmarkClassName>.BENCHMARK_IMPL_REGISTRY`.

| Benchmark   | Spark (Fabric) | DuckDB | Polars | Daft |
|-------------|:--------------:|:------:|:------:|:----:|
| ELTBench    |      ✅        |   ✅   |   ✅   |  ✅  |
| AtomicELT   |      🔜        |   🔜   |   🔜   |  🔜  |
| TPC-DS      |      ✅        |   ✅   |   ✅   |  ✅  |
| TPC-H       |      ✅        |   ✅   |   ✅   |  ✅  |

> **Legend:**  
> ✅ = Supported  
> 🔜 = Coming Soon  
> (Blank) = Not currently supported 

LakeBench is designed to be _extensible_—new engines can be added via subclassing an existing engine class, and benchmarks can register support for additional engines as they are implemented.

---

## 📦 Installation

Install from PyPi:

```bash
pip install lakebench[duckdb,polars,daft]
```

_Note: in this initial beta version, all engines have only been tested inside Microsoft Fabric Python and Spark Notebooks._

## Example Usage

### Fabric Spark
```python
from lakebench.benchmarks.elt_bench import ELTBench
from lakebench.engines.fabric_spark import FabricSpark

engine = FabricSpark(
    lakehouse_workspace_name="workspace",
    lakehouse_name="lakehouse",
    lakehouse_schema_name="schema"
)

benchmark = ELTBench(
    engine=engine,
    scenario_name="sf10",
    mode="light",
    tpcds_parquet_abfss_path="abfss://...",
    save_results=True,
    result_abfss_path="abfss://..."
)

benchmark.run()
```

### Polars
```python
from lakebench.benchmarks.elt_bench import ELTBench
from lakebench.engines.polars import Polars

engine = Polars( 
    delta_abfss_schema_path = 'abfss://...'
)

benchmark = ELTBench(
    engine=engine,
    scenario_name="sf10",
    mode="light",
    tpcds_parquet_abfss_path="abfss://...",
    save_results=True,
    result_abfss_path="abfss://..."
)

benchmark.run()
```

## 🔌 Extensibility by Design

LakeBench is built to be **plug-and-play** for both benchmark types and compute engines:

- You can register **new engines** without modifying core benchmark logic.
- You can add **new benchmarks** that reuse existing engines and shared engine methods.
- LakeBench extension libraries can be created to extend core LakeBench capabilities with additional custom benchmarks and engines (i.e. `MyCustomSynapseSpark(Spark)`, `MyOrgsELT(BaseBenchmark)`).

This architecture encourages experimentation, benchmarking innovation, and easy adaptation to your needs.

_Example:_
```python
# Automatically maps benchmark implementation to your custom engine class
from lakebench.engines.spark import Spark

class MyCustomSynapseSpark(Spark):
    ...

benchmark = AtomicELT(engine=MyCustomSynapseSpark(...))
```
All you need to do is subclass the relevant base class and it will auto-register provided that the referenced benchmark supports the base class. No changes to the framework internals required.

# 🔍 Philosophy
LakeBench is designed to host a suite of benchmarks that cover E2E data engineering and consumption workloads:
- Loading data from raw storage
- Transforming and enriching data
- Applying incremental module building logic
- Maintaining and optimizing datasets
- Running complex analytical queries

The core aim is provide transparency into engine efficiency, performance, and costs across the data lifecycle..

# 📬 Feedback / Contributions
Got ideas? Found a bug? Want to contribute a benchmark or engine wrapper? PRs and issues are welcome!


# Acknowledgement of Other _LakeBench_ Projects
The **LakeBench** name is also used by two unrelated academic and research efforts:
- **[RLGen/LAKEBENCH](https://github.com/RLGen/LAKEBENCH)**: A benchmark designed for evaluating vision-language models on multimodal tasks.
- **LakeBench: Benchmarks for Data Discovery over Lakes** ([paper link](https://www.catalyzex.com/paper/lakebench-benchmarks-for-data-discovery-over)):
    A benchmark suite focused on improving data discovery and exploration over large data lakes.

While these projects target very different problem domains — such as machine learning and data discovery — they coincidentally share the same name. This project, focused on ELT benchmarking across lakehouse engines, is not affiliated with or derived from either.