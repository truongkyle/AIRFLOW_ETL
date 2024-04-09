class GoldLayer():
    def __init__(self, minio_io_manager):
        self.minio_io_manager = minio_io_manager
        self.context = {
                "layer": "gold",
                "schema": "ecom",
                "table": "gold_sales_value_by_category"
            }
    
    def gold_sales_value_by_category(self, fact_sales, dim_products):
        print("columns fact_sales", fact_sales.columns)
        print("-----------------------------")
        print("columns dim_products", dim_products.columns)
        fact_sales["order_purchase_timstamp"] = fact_sales[
            "order_purchase_timestamp"
        ].apply(lambda x: x.split()[0])

        daily_sales_products = (
            fact_sales.query("order_status == 'delivered'")
            .groupby(["order_purchase_timestamp", "product_id"])
            .agg({"payment_value": "sum", "order_id":"unique"})
            .reset_index()
        )

        monthly_sales_categories = daily_sales_products.merge(dim_products, on="product_id")
        monthly_sales_categories["monthly"] = monthly_sales_categories[
            "order_purchase_timestamp"
        ].apply(lambda x: x[:-2] +"01")

        sales = (
            monthly_sales_categories.groupby(["monthly", "product_category_name_english"])
            .agg({"payment_value":"sum", "order_id": "count"})
            .reset_index()
            .rename(
                columns={
                    "product_category_name_english": "category",
                    "payment_value": "sales",
                    "order_id": "bills",
                }
            )
        )
        print("----------------------------------------------------------------")
        print(sales.info())
        print("----------------------------------------------------------------")
        sales["value_per_bill"] = sales["sales"]/ sales["bills"]
        

        self.minio_io_manager.handle_output(self.context, sales)

        return self.context


