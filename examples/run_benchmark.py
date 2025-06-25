##### NAME IDEAS
name = "BenchFactory"
name = "LakeBench"
name = "BenchLake"
#####


from lakebench.benchmarks.atomic_elt import AtomicELT
from lakebench.engines.fabric_spark import FabricSpark

engine = FabricSpark(
    lakehouse_workspace_name = 'mcole_scenario_repl', 
    lakehouse_name = 'mcole_benchmarks', 
    lakehouse_schema_name = 'spark_atomic_elt_100_8core',
    spark_measure_telemetry = False
)

benchmark = AtomicELT(
    engine=engine,
    scenario_name="4vCores",
    tpcds_parquet_abfss_path='abfss://........./Files/tpcds/source/sf1_parquet',
    save_results=False,
    result_abfss_path='abfss://......../Tables/dbo/results'
    )
benchmark.run(mode="light")

###################
from lakebench.benchmarks.atomic_elt import AtomicELT
from lakebench.engines.polars import Polars

engine = Polars( 
    delta_abfss_schema_path = 'abfss://.........../Tables/polars_atomic_elt_100_8core'
)

benchmark = AtomicELT(
    engine=engine,
    scenario_name="4vCores",
    tpcds_parquet_abfss_path='abfss://........./Files/tpcds/source/sf1_parquet',
    save_results=False,
    result_abfss_path='abfss://........../Tables/dbo/results'
    )
benchmark.run(mode="light")

###################
from lakebench.benchmarks.atomic_elt import AtomicELT
from lakebench.engines.duckdb import DuckDB

engine = DuckDB( 
    delta_abfss_schema_path = 'abfss://.........../Tables/polars_atomic_elt_100_8core'
)

benchmark = AtomicELT(
    engine=engine,
    scenario_name="4vCores",
    tpcds_parquet_abfss_path='abfss://........./Files/tpcds/source/sf1_parquet',
    save_results=False,
    result_abfss_path='abfss://............./Tables/dbo/results'
    )
benchmark.run(mode="light")


###################
from lakebench.benchmarks.atomic_elt import AtomicELT
from lakebench.engines.daft import Daft

engine = Daft( 
    delta_abfss_schema_path = 'abfss://............./Tables/polars_atomic_elt_100_8core',
    delta_mount_schema_path = '/lakehouse/default/Tables/polars_atomic_elt_100_8core'
)

benchmark = AtomicELT(
    engine=engine,
    scenario_name="4vCores",
    tpcds_parquet_mount_path='/lakehouse/default/Files/tpcds/source/sf1_parquet',
    save_results=False,
    result_abfss_path='abfss://.............../Tables/dbo/results'
    )
benchmark.run(mode="light")