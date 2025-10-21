import posixpath
import importlib.util
import fsspec
from fsspec import AbstractFileSystem
import subprocess
from lakebench.utils.path_utils import to_unix_path
from urllib.parse import urlparse

class _TPCRsDataGenerator:
    """
    Base class for TPC Rust based data generation. PLEASE DO NOT INSTANTIATE THIS CLASS DIRECTLY. Use the TPCHDataGenerator and TPCDSDataGenerator
    subclasses instead.
    """
    GEN_UTIL = ''
    GEN_TYPE = ''

    def __init__(self, scale_factor: int, target_folder_uri: str, target_row_group_size_mb: int = 128) -> None:
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

        """
        self.scale_factor = scale_factor
        uri_scheme = urlparse(target_folder_uri).scheme
        
        # Allow local file systems: no scheme, file://, or Windows drive letters
        cloud_schemes = {'s3', 'gs', 'gcs', 'abfs', 'abfss', 'adl', 'wasb', 'wasbs'}
        
        if uri_scheme in cloud_schemes:
            raise ValueError(f"{uri_scheme} protocol is not currently supported for TPC-RS data generation. Please use a local file path.")
        
        self.fs: AbstractFileSystem = fsspec.filesystem("file")
        self.target_folder_uri = to_unix_path(target_folder_uri)
        self.target_row_group_size_mb = target_row_group_size_mb

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
        This method uses a rust based TPC data generation utility to generate Parquet files
        based on the specified scale factor. The generated tables are written to the target folder.
        """
        
        # cleanup target directory
        if self.fs.exists(self.target_folder_uri):
            self.fs.rm(self.target_folder_uri, recursive=True)
        self.fs.mkdirs(self.target_folder_uri, exist_ok=True)

        cmd = [
            self.tpcgen_exe,
            "--scale-factor", str(self.scale_factor),
            "--output-dir", self.target_folder_uri,
            "--parts", "1",
            "--format", "parquet",
            "--parquet-row-group-bytes", str(self.target_row_group_size_mb * 1024 * 1024),
            "--parquet-compression", "SNAPPY"
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            if result.stdout:
                print(result.stdout)
        except subprocess.CalledProcessError as e:
            print(f"stdout: {e.stdout}")
            print(f"stderr: {e.stderr}")