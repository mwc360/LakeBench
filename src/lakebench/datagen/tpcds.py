from ._tpc import _TPCDataGenerator
class TPCDSDataGenerator(_TPCDataGenerator):
    """
    TPC-DS Data Generator class.
    This class is a wrapper of the DuckDB TPC-DS data generation utility.
    """
    GEN_UTIL = 'dsdgen'