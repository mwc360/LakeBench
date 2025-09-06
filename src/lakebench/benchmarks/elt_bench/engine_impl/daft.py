from ....engines.daft import Daft
from ....engines.delta_rs import DeltaRs
import posixpath

class DaftELTBench:
    def __init__(self, engine : Daft):
        self.engine = engine
        
        import numpy as np
        self.np = np
        self.delta_rs = DeltaRs()
        self.write_deltalake = self.delta_rs.write_deltalake
        self.DeltaTable = self.delta_rs.DeltaTable

    def create_total_sales_fact(self):
        fact_table_df = (
            self.engine.daft.read_deltalake(posixpath.join(self.engine.delta_abfss_schema_path, 'store_sales/'))
            .join(
                self.engine.daft.read_deltalake(posixpath.join(self.engine.delta_abfss_schema_path, 'date_dim/')), 
                left_on="ss_sold_date_sk", 
                right_on="d_date_sk"
            )
            .join(
                self.engine.daft.read_deltalake(posixpath.join(self.engine.delta_abfss_schema_path, 'store/')), 
                left_on="ss_store_sk", 
                right_on="s_store_sk"
            )
            .join(
                self.engine.daft.read_deltalake(posixpath.join(self.engine.delta_abfss_schema_path, 'item/')), 
                left_on="ss_item_sk", 
                right_on="i_item_sk"
            )
            .join(
                self.engine.daft.read_deltalake(posixpath.join(self.engine.delta_abfss_schema_path, 'customer/')), 
                left_on="ss_customer_sk", 
                right_on="c_customer_sk"
            )
            .with_columns({
                "sale_date": self.engine.daft.col("d_date")
            })
            .where(self.engine.daft.col("d_year") == 2001)
            .groupby(["s_store_id", "i_item_id", "c_customer_id", "sale_date"])
            .agg([
                self.engine.daft.col("ss_quantity").sum().alias("total_quantity"),
                self.engine.daft.col("ss_net_paid").sum().cast(self.engine.daft.DataType.decimal128(38, 2)).alias("total_net_paid"),
                self.engine.daft.col("ss_net_profit").sum().cast(self.engine.daft.DataType.decimal128(38, 2)).alias("total_net_profit")
            ])
            .sort(["s_store_id", "sale_date"])
        )

        fact_table_df.write_deltalake(
            table_or_uri=posixpath.join(self.engine.delta_abfss_schema_path, 'total_sales_fact'),
            mode="overwrite",
            storage_options=self.engine.storage_options,
        ) 


    def merge_percent_into_total_sales_fact(self, percent: float):

        seed = self.np.random.randint(1, high=1000, size=None, dtype=int)
        modulo = int(1 / percent)

        
        sampled_fact_data = (
            self.engine.daft.read_deltalake(posixpath.join(self.engine.delta_abfss_schema_path, 'store_sales/'))
            .join(
                self.engine.daft.read_deltalake(posixpath.join(self.engine.delta_abfss_schema_path, 'date_dim/')), 
                left_on="ss_sold_date_sk", 
                right_on="d_date_sk"
            )
            .join(
                self.engine.daft.read_deltalake(posixpath.join(self.engine.delta_abfss_schema_path, 'store/')), 
                left_on="ss_store_sk", 
                right_on="s_store_sk"
            )
            .join(
                self.engine.daft.read_deltalake(posixpath.join(self.engine.delta_abfss_schema_path, 'item/')), 
                left_on="ss_item_sk", 
                right_on="i_item_sk"
            )
            .join(
                self.engine.daft.read_deltalake(posixpath.join(self.engine.delta_abfss_schema_path, 'customer/')), 
                left_on="ss_customer_sk", 
                right_on="c_customer_sk"
            )
            .with_columns({
                # Use hash for pseudo-random values
                "new_uid_val": (self.engine.daft.col("ss_customer_sk") + self.engine.daft.col("ss_sold_date_sk") + seed),
                "s_store_id": self.engine.daft.col("s_store_id"),
                "i_item_id": self.engine.daft.col("i_item_id"),
                "sale_date": self.engine.daft.col("d_date"),
            })
            .filter(
                (self.engine.daft.col("new_uid_val") % modulo) == 0
            )
            .with_columns({
                "c_customer_id": (
                    (self.engine.daft.col("new_uid_val") % 2 == 0)
                    .if_else(
                        self.engine.daft.col("c_customer_id"),
                        'NEW_' + self.engine.daft.col("new_uid_val").cast(self.engine.daft.DataType.string())
                    )
                ),
                "total_quantity": self.engine.daft.col("ss_quantity") + (self.engine.daft.col("new_uid_val") % 5 + 1),
                "total_net_paid": (self.engine.daft.col("ss_net_paid") + ((self.engine.daft.col("new_uid_val") % 5000) / 100.0 + 5)).cast(self.engine.daft.DataType.decimal128(38, 2)),
                "total_net_profit": (self.engine.daft.col("ss_net_profit") + ((self.engine.daft.col("new_uid_val") % 2000) / 100.0 + 1)).cast(self.engine.daft.DataType.decimal128(38, 2))
            })
            .select(
                "s_store_id",
                "i_item_id", 
                "c_customer_id",
                "sale_date",
                "total_quantity",
                "total_net_paid",
                "total_net_profit"
            )
            .to_arrow()
        )

        fact_table = self.DeltaTable(
            table_uri=posixpath.join(self.engine.delta_abfss_schema_path, 'total_sales_fact'),
            storage_options=self.engine.storage_options,
        )
    
        fact_table.merge(
            source=sampled_fact_data,
            predicate="""
            target.s_store_id = source.s_store_id AND 
            target.i_item_id = source.i_item_id AND 
            target.c_customer_id = source.c_customer_id AND 
            target.sale_date = source.sale_date
            """,
            source_alias="source",
            target_alias="target"
        ) \
        .when_matched_update({
            "total_quantity": "target.total_quantity + source.total_quantity",
            "total_net_paid": "target.total_net_paid + source.total_net_paid",
            "total_net_profit": "target.total_net_profit + source.total_net_profit",
        }) \
        .when_not_matched_insert({
            "s_store_id": "source.s_store_id",
            "i_item_id": "source.i_item_id",
            "c_customer_id": "source.c_customer_id",
            "sale_date": "source.sale_date",
            "total_quantity": "source.total_quantity",
            "total_net_paid": "source.total_net_paid",
            "total_net_profit": "source.total_net_profit",
        }).execute()

    def query_total_sales_fact(self):
        query_df = (
            self.engine.daft.read_deltalake(posixpath.join(self.engine.delta_abfss_schema_path, 'total_sales_fact/'))
                .groupby(self.engine.daft.col("sale_date").dt.year())
                .agg(self.engine.daft.col("total_net_profit").sum().alias("sum_net_profit"))
                .to_pandas()
        )