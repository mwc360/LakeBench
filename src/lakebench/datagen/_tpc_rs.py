import posixpath
import importlib.util
import fsspec
from fsspec import AbstractFileSystem
import subprocess
import threading
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from lakebench.utils.path_utils import to_unix_path
from urllib.parse import urlparse

class _TPCRsDataGenerator:
    """
    Base class for TPC Rust based data generation. PLEASE DO NOT INSTANTIATE THIS CLASS DIRECTLY. Use the TPCHDataGenerator and TPCDSDataGenerator
    subclasses instead.
    """
    GEN_UTIL = ''
    GEN_TYPE = ''
    GEN_SF1000_FILE_COUNT_MAP = {
        'table1': 10,
        'table2': 40
    }
    GEN_TABLE_REGISTRY = [
        'table1', 'table2'
    ]
    
    # Class-level lock for thread-safe printing
    _print_lock = threading.Lock()

    def __init__(self, scale_factor: int, target_folder_uri: str, target_row_group_size_mb: int = 128, compression: str = "ZSTD(3)") -> None:
        """
        Initialize the TPC data generator with a scale factor.

        Parameters
        ----------
        scale_factor: int
            The scale factor for the data generation.
        target_folder_uri: str
            Test data will be written to this location where tables are represented as folders containing parquet files.
        target_row_group_size_mb: int, default=128
            Desired row group size for the generated parquet files.
        compression: str, default="ZSTD"
            Compression codec to use for the generated parquet files.
            Supports codecs: "UNCOMPRESSED", "SNAPPY", "GZIP(compression_level)", "BROTLI(compression_level)", "LZ4", "LZ4_RAW", "LZO", "ZSTD(compression_level)"
        """
        self.scale_factor = scale_factor
        uri_scheme = urlparse(target_folder_uri).scheme
        
        # Allow local file systems: no scheme, file://, or Windows drive letters
        cloud_schemes = {'s3', 'gs', 'gcs', 'abfs', 'abfss', 'adl', 'wasb', 'wasbs'}
        
        if uri_scheme in cloud_schemes:
            raise ValueError(f"{uri_scheme} protocol is not currently supported for TPC-RS data generation. Please use a local file system path or mount the storage location.")
        
        if compression.split('(')[0] not in ["UNCOMPRESSED", "SNAPPY", "GZIP", "BROTLI", "LZ4", "LZ4_RAW", "LZO", "ZSTD"]:
            raise ValueError(f"Unsupported compression codec: {compression}")
        
        self.fs: AbstractFileSystem = fsspec.filesystem("file")
        self.target_folder_uri = to_unix_path(target_folder_uri)
        self.target_row_group_size_mb = target_row_group_size_mb
        self.compression = compression

        def get_tpcgen_path():
            import shutil
            # Try shutil.which first (most reliable)
            path = shutil.which(f"{self.GEN_TYPE}gen-cli")
            if path:
                return path

            # Fallback to user Scripts directory
            from pathlib import Path
            import sys
            user_scripts = Path.home() / "AppData" / "Roaming" / "Python" / f"Python{sys.version_info.major}{sys.version_info.minor}" / "Scripts" / "tpchgen-cli.exe"
            if user_scripts.exists():
                return str(user_scripts)

            raise ImportError(f"{self.GEN_TYPE}gen-cli is used for data generation but is not installed. Install using `%pip install {self.GEN_TYPE}gen-cli`")

        self.tpcgen_exe = get_tpcgen_path()
        
    
    def run(self) -> None:
        """
        This method uses multithreading to generate individual tables in parallel using
        a rust-based TPC data generation utility. Each table is generated with an optimal
        number of parts (based on the GEN_SF1000_FILE_COUNT_MAP) to target having files around 1GB.
        """
        
        # cleanup target directory
        if self.fs.exists(self.target_folder_uri):
            self.fs.rm(self.target_folder_uri, recursive=True)
        self.fs.mkdirs(self.target_folder_uri, exist_ok=True)
        
        tables = self.GEN_TABLE_REGISTRY
        
        print(f"🚀 Starting parallel generation of {len(tables)} tables with multithreading...")
        print(f"📊 Scale Factor: {self.scale_factor}")
        print(f"📁 Output Directory: {self.target_folder_uri}")
        
        with ThreadPoolExecutor() as executor:
            future_to_table = {
                executor.submit(self._generate_table, table_name): table_name 
                for table_name in tables
            }
            
            completed_tables = []
            failed_tables = []
            
            for future in as_completed(future_to_table):
                table_name = future_to_table[future]
                try:
                    result = future.result()
                    if result:
                        completed_tables.append(table_name)
                        print(f"✅ {table_name} - Generation completed successfully")
                    else:
                        failed_tables.append(table_name)
                        print(f"❌ {table_name} - Generation failed")
                except Exception as exc:
                    failed_tables.append(table_name)
                    print(f"❌ {table_name} - Generation failed with exception: {exc}")
        
        print(f"\n📋 Generation Summary:")
        print(f"   ✅ Successfully generated: {len(completed_tables)} tables")
        if completed_tables:
            print(f"      Tables: {', '.join(completed_tables)}")
        
        if failed_tables:
            print(f"   ❌ Failed to generate: {len(failed_tables)} tables")
            print(f"      Tables: {', '.join(failed_tables)}")
            raise RuntimeError(f"Failed to generate {len(failed_tables)} tables: {', '.join(failed_tables)}")
        else:
            print(f"🎉 All {len(tables)} tables generated successfully!")
    
    def _generate_table(self, table_name: str) -> bool:
        """
        Generate a single table using the optimal number of parts.
        
        Parameters
        ----------
        table_name: str
            Name of the table to generate
            
        Returns
        -------
        bool
            True if generation was successful, False otherwise
        """
        
        # Calculate optimal parts for this table based on scale factor
        sf1000_parts = self.GEN_SF1000_FILE_COUNT_MAP.get(table_name)
        
        # Scale the parts based on the scale factor
        optimal_parts = max(1, math.ceil(sf1000_parts * (self.scale_factor / 1000.0))) if sf1000_parts is not None else 1
        
        print(f"🔧 {table_name} - Using {optimal_parts} parts")
        
        cmd = [
            self.tpcgen_exe,
            "--scale-factor", str(self.scale_factor),
            "--output-dir", self.target_folder_uri,
            "--parts", str(optimal_parts),
            "--format", "parquet",
            "--parquet-row-group-bytes", str(self.target_row_group_size_mb * 1024 * 1024),
            "--parquet-compression", self.compression,
            "--tables", table_name 
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            if result.stdout:
                with self._print_lock:
                    print(f"📝 {table_name} output:")
                    for line in result.stdout.strip().split('\n'):
                        if line.strip():
                            print(f"   {line}")
            return True
            
        except subprocess.CalledProcessError as e:
            with self._print_lock:
                print(f"❌ {table_name} failed:")
                if e.stdout:
                    print(f"   stdout: {e.stdout}")
                if e.stderr:
                    print(f"   stderr: {e.stderr}")
            return False
        except Exception as e:
            with self._print_lock:
                print(f"❌ {table_name} failed with exception: {e}")
            return False