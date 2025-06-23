# LakeBench

🌊 **LakeBench** is a modular, extensible benchmark framework for evaluating performance across lakehouse platforms, engines, and ELT scenarios.

Most existing benchmarks (like TPC-DS and TPC-H) are too query-heavy and miss the reality that data engineers build complex **ELT pipelines** — not just run analytic queries. While these traditional benchmarks are helpful for testing bulk loading and complex SQL execution, they do not reflect the broader data lifecycle that lakehouse systems must support.

> My lightweight benchmark proposes that **the entire end-to-end data lifecycle which data engineers manage or encounter is relevant**: data loading, bulk transformations, incrementally applying transformations, maintenance jobs, and ad-hoc aggregative queries.

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

- **AtomicELT**: A minimal ELT pipeline to evaluate:
  - Raw data load (Parquet → Delta)
  - Fact table generation
  - Incremental merge processing
  - Table maintenance (e.g. OPTIMIZE/VACUUM)
  - Ad-hoc analytical queries

More advanced benchmarks (e.g. [TPC-DS](https://www.tpc.org/tpcds/) + [TPC-H](https://www.tpc.org/tpch/)) will soon be supported to evaluate scale and complexity of query workloads but are **not the default focus** of LakeBench.

---

## 🛠️ Engines Supported

LakeBench supports multiple lakehouse compute engines. Each benchmark declares its own supported engines.

- ✅ Apache Spark (Fabric)
- ✅ DuckDB
- ✅ Polars
- ✅ Daft
- 🛠️ Extensible via engine wrappers

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
from lakebench.benchmarks.atomic_elt.atomic_elt import AtomicELT
from lakebench.engines.fabric_spark import FabricSpark

engine = FabricSpark(
    lakehouse_workspace_name="workspace",
    lakehouse_name="lakehouse",
    lakehouse_schema_name="schema"
)

benchmark = AtomicELT(
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
from lakebench.benchmarks.atomic_elt.atomic_elt import AtomicELT
from lakebench.benchmarks.atomic_elt.engines.polars import Polars

engine = Polars( 
    delta_abfss_schema_path = 'abfss://...'
)

benchmark = AtomicELT(
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