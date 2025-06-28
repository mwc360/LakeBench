from ._tpc import _TPCDataGenerator
class TPCHDataGenerator(_TPCDataGenerator):
    """
    TPC-H Data Generator class.
    This class is a wrapper of the DuckDB TPC-H data generation utility.
    """
    GEN_UTIL = 'dbgen'