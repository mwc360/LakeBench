import posixpath
import os
import importlib.util


class _TPCDataGenerator:
    """
    TPC Data Generator class.
    This class is a wrapper of the DuckDB TPC[DS/H] data generation utility.
    """
    GEN_UTIL = ''

    def __init__(self, scale_factor: int, target_mount_folder_path: str = None, target_row_group_size_mb: int = 128):
        """
        Initialize the TPC data generator with a scale factor.

        :param scale_factor: The scale factor for the data generation.
        """
        self.scale_factor = scale_factor
        self.target_mount_folder_path = target_mount_folder_path
        self.target_row_group_size_mb = target_row_group_size_mb

        if importlib.util.find_spec("duckdb") is None:
            raise ImportError(
                "DuckDB is used for data generation but is not installed. Install using `%pip install lakebench[duckdb]` or `%pip install lakebench[datagen]`"
            )

    def run(self):
        """
        Generate TPC[DS/H] parquet data based on the input scale factor.
        """
        import duckdb
        import pyarrow.parquet as pq

        con = duckdb.connect()
        print(f"Generating in-memory tables")
        con.execute(f"CALL {self.GEN_UTIL}(sf={self.scale_factor})")
        tables = [row[0] for row in con.execute("SHOW TABLES").fetchall()]
        print(f"Generated in-memory tables: {tables}")

        for table in tables:
            sample_file = posixpath.join(self.target_mount_folder_path, f"{table}_sample.parquet")
            full_folder_path = posixpath.join(self.target_mount_folder_path, table)
            os.makedirs(self.target_mount_folder_path, exist_ok=True)
            # Write a sample for row size estimation
            print(f"\nSampling {table} to evaluate row count to target {self.target_row_group_size_mb}mb row groups...")
            con.execute(f"""
                COPY (SELECT * FROM {table} LIMIT 1000000)
                TO '{sample_file}'
                (FORMAT 'parquet')
            """)

            pf = pq.ParquetFile(sample_file)
            rg = pf.metadata.row_group(0)
            avg_row_size = rg.total_byte_size / rg.num_rows
            #print(f"{table} sample: {rg.num_rows} rows, {rg.total_byte_size / (1024*1024):.2f} MB")
            #print(f"Avg row size: {avg_row_size:.2f} bytes")
            target_size_bytes = self.target_row_group_size_mb * 1024 * 1024
            target_rows = int(target_size_bytes / avg_row_size)
            #print(f"Target ROW_GROUP_SIZE for ~{self.target_row_group_size_mb} MB: {target_rows} rows")

            # Write full table
            print(f"Writing {table} to {full_folder_path} with ROW_GROUP_SIZE {target_rows}...")
            con.execute(f"""
                COPY {table} TO '{full_folder_path}'
                (FORMAT 'parquet', ROW_GROUP_SIZE {target_rows}, PER_THREAD_OUTPUT)
            """)

            con.execute(f"DROP TABLE {table}")
            os.remove(sample_file)