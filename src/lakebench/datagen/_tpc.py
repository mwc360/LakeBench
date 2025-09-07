import posixpath
import importlib.util
import fsspec
from fsspec import AbstractFileSystem

class _TPCDataGenerator:
    """
    Base class for TPC data generation. PLEASE DO NOT INSTANTIATE THIS CLASS DIRECTLY. Use the TPCHDataGenerator and TPCDSDataGenerator
    subclasses instead.
    """
    GEN_UTIL = ''

    def __init__(self, scale_factor: int, target_mount_folder_path: str, target_row_group_size_mb: int = 128) -> None:
        """
        Initialize the TPC data generator with a scale factor.

        Parameters
        ----------
        scale_factor: int
            The scale factor for the data generation.
        target_mount_folder_path: str
            Test data will be written to this location where tables are represented as folders containing parquet files.
        target_row_group_size_mb: int, default=128
            Desired row group size for the generated parquet files.

        """
        self.scale_factor = scale_factor
        self.target_mount_folder_path = target_mount_folder_path
        self.target_row_group_size_mb = target_row_group_size_mb

        if importlib.util.find_spec("duckdb") is None:
            raise ImportError(
                "DuckDB is used for data generation but is not installed. Install using `%pip install lakebench[duckdb]` or `%pip install lakebench[datagen]`"
            )
        
        if target_mount_folder_path.startswith("abfss://"):
            raise ValueError("abfss path currently not supported. DuckDB is used for data generation and DuckDB is not able to write to Azure remote storage as of now.")
            # self.fs: FsspecStore = FsspecStore(protocol=urlparse(target_mount_folder_path).scheme)
        else:
            # workaround: use original fsspec until obstore bugs are fixes:
            # * https://github.com/developmentseed/obstore/issues/555
            self.fs: AbstractFileSystem = fsspec.filesystem("file")
        
    def run(self) -> None:
        """
        This method uses DuckDB to generate in-memory tables based on the specified 
        scale factor and writes them to Parquet files. It estimates the average row 
        size in MB using a sample of the data since DuckDB only supports specifying 
        the number of rows per row group. The generated tables are written to the 
        specified target folder with optimized row group sizes.
       
        Notes
        -----
        - The method creates a sample Parquet file for each table to estimate row sizes.
        - The full table is then written as Parquet files with optimized row group sizes.
        - Temporary files and in-memory tables are cleaned up after processing.
        """
        import duckdb
        import pyarrow.parquet as pq

        # cleanup target directory
        if self.fs.exists(self.target_mount_folder_path):
            self.fs.rm(self.target_mount_folder_path, recursive=True)
        self.fs.mkdirs(self.target_mount_folder_path, exist_ok=True)

        with duckdb.connect() as con:
            print("Generating in-memory tables")
            con.execute(f"CALL {self.GEN_UTIL}(sf={self.scale_factor})")
            tables = [row[0] for row in con.execute("SHOW TABLES").fetchall()]
            print(f"Generated in-memory tables: {tables}")

            for table in tables:
                sample_file = posixpath.join(self.target_mount_folder_path, f"{table}_sample.parquet")
                full_folder_path = posixpath.join(self.target_mount_folder_path, table)
                # Write a sample for row size estimation
                print(f"\nSampling {table} to evaluate row count to target {self.target_row_group_size_mb}mb row groups...")
                con.execute(f"""
                    COPY (SELECT * FROM {table} LIMIT 1000000)
                    TO '{sample_file}'
                    (FORMAT 'parquet', OVERWRITE)
                """)

                with pq.ParquetFile(sample_file) as pf:
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
                    (FORMAT 'parquet', ROW_GROUP_SIZE {target_rows}, PER_THREAD_OUTPUT, OVERWRITE)
                """)

                con.execute(f"DROP TABLE {table}")

                self.fs.rm(sample_file)