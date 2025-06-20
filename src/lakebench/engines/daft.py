from .base import BaseEngine

from IPython.core.getipython import get_ipython
notebookutils = get_ipython().user_ns.get("notebookutils")

class DaftEngine(BaseEngine):
    """
    Daft Engine for ELT Benchmarks.
    """

    def __init__(
            self, 
            delta_mount_schema_path: str,
            delta_abfss_schema_path: str
            ):
        """
        Initialize the Daft Engine Configs
        """
        import daft
        self.daft = daft
        self.delta_mount_schema_path = delta_mount_schema_path
        self.delta_abfss_schema_path = delta_abfss_schema_path