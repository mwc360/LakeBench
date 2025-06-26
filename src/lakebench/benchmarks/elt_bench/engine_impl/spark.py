from ....engines.spark import Spark

class SparkELTBench:
    def __init__(self, engine: Spark):
        
        self.engine = engine
        self.engine.create_schema_if_not_exists(drop_before_create=True)

    def create_total_sales_fact(self):
        self.engine.spark.sql("""
            CREATE OR REPLACE TABLE total_sales_fact AS
            SELECT 
                s.s_store_id,
                i.i_item_id,
                c.c_customer_id,
                d.d_date AS sale_date,
                SUM(ss.ss_quantity) AS total_quantity,
                SUM(ss.ss_net_paid) AS total_net_paid,
                SUM(ss.ss_net_profit) AS total_net_profit
            FROM 
                store_sales ss
            JOIN 
                date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
            JOIN 
                store s ON ss.ss_store_sk = s.s_store_sk
            JOIN 
                item i ON ss.ss_item_sk = i.i_item_sk
            JOIN 
                customer c ON ss.ss_customer_sk = c.c_customer_sk
            WHERE 
                d.d_year = 2001
            GROUP BY 
                s.s_store_id, i.i_item_id, c.c_customer_id, d.d_date
            ORDER BY 
                s.s_store_id, d.d_date;
        """)

    def merge_percent_into_total_sales_fact(self, percent: float):
        store_sales = self.engine.spark.table("store_sales")
        date_dim = self.engine.spark.table("date_dim")
        store = self.engine.spark.table("store")
        item = self.engine.spark.table("item")
        customer = self.engine.spark.table("customer")

        sampled_data = store_sales.sample(fraction=percent)

        sampled_fact_data = (
            sampled_data.join(date_dim, store_sales.ss_sold_date_sk == date_dim.d_date_sk)
            .join(store, store_sales.ss_store_sk == store.s_store_sk)
            .join(item, store_sales.ss_item_sk == item.i_item_sk)
            .join(customer, store_sales.ss_customer_sk == customer.c_customer_sk)
            .select(
                store.s_store_id,
                item.i_item_id,
                customer.c_customer_id,
                date_dim.d_date.alias("sale_date"),
                store_sales.ss_quantity.alias("total_quantity"),
                store_sales.ss_net_paid.alias("total_net_paid"),
                store_sales.ss_net_profit.alias("total_net_profit")
            )
        )

        synthetic_merge_data = sampled_fact_data \
            .withColumn("c_customer_id", self.engine.sf.when(self.engine.sf.rand() > 0.5, self.engine.sf.expr("CAST(rand() * 100000 AS INT)")).otherwise(sampled_fact_data.c_customer_id)) \
            .withColumn("total_quantity", sampled_fact_data.total_quantity + self.engine.sf.expr("floor(rand() * 5 + 1)")) \
            .withColumn("total_net_paid", sampled_fact_data.total_net_paid + self.engine.sf.expr("rand() * 50 + 5")) \
            .withColumn("total_net_profit", sampled_fact_data.total_net_profit + self.engine.sf.expr("rand() * 20 + 1"))

        # Register as a temporary view for SQL-based merge
        synthetic_merge_data.createOrReplaceTempView("synthetic_merge_data")

        self.engine.spark.sql("""
            MERGE INTO total_sales_fact AS target
            USING (
                SELECT 
                    s_store_id, 
                    i_item_id, 
                    c_customer_id, 
                    sale_date, 
                    total_quantity, 
                    total_net_paid, 
                    total_net_profit
                FROM synthetic_merge_data
            ) AS source
            ON 
                target.s_store_id = source.s_store_id AND 
                target.i_item_id = source.i_item_id AND 
                target.c_customer_id = source.c_customer_id AND 
                target.sale_date = source.sale_date
            WHEN MATCHED THEN 
                UPDATE SET 
                    target.total_quantity = target.total_quantity + source.total_quantity,
                    target.total_net_paid = target.total_net_paid + source.total_net_paid,
                    target.total_net_profit = target.total_net_profit + source.total_net_profit
            WHEN NOT MATCHED THEN 
                INSERT (s_store_id, i_item_id, c_customer_id, sale_date, total_quantity, total_net_paid, total_net_profit)
                VALUES (source.s_store_id, source.i_item_id, source.c_customer_id, source.sale_date, source.total_quantity, source.total_net_paid, source.total_net_profit);
        """)
        
    def query_total_sales_fact(self):
        df = self.engine.spark.sql(f"""
                            select sum(total_net_profit), year(sale_date) 
                            from total_sales_fact group by year(sale_date)
                            """)
        result = df.collect()