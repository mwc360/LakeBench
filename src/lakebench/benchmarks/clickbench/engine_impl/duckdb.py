from ....engines.duckdb import DuckDB
import posixpath

class DuckDBClickBench:
    def __init__(self, engine: DuckDB):
        
        self.engine = engine

    def load_parquet_to_delta(self, table_name: str, source_data_path: str, table_is_precreated: bool = False, context_decorator: str = None):
        """
        Loads the ClickBench parquet data into Delta format using Spark.

        Parameters
        ----------
        source_data_path : str
            Path to the source parquet files.
        """
        arrow_df = self.engine.duckdb.sql(f"""
            SELECT * REPLACE (make_date(EventDate) AS EventDate) 
            FROM parquet_scan('{posixpath.join(source_data_path, '*.parquet')}')
        """).record_batch()
        
        self.engine.deltars.write_deltalake(
            posixpath.join(self.engine.delta_abfss_schema_path, table_name),
            arrow_df,
            mode="append"
        ) 

    def execute_sql_query(self, query: str):
        return self.engine.execute_sql_query(query)