class SilverLayer():
    def __init__(self, minio_io_manager):
        self.minio_io_manager = minio_io_manager

    def dim_products(self, bronze_olist_products_dataset, bronze_product_category_name_traslations, context):
        pd_dim_products = bronze_olist_products_dataset.merge(
            bronze_product_category_name_traslations, on = "product_category_name"
            )
        pd_dim_products = pd_dim_products[["product_id", "product_category_name_english"]]

        self.minio_io_manager.handle_output(context, pd_dim_products)
        return pd_dim_products
    def fact_sales(self, bronze_olist_orders_dataset, bronze_olist_order_items_dataset, bronze_olist_order_payments_dataset, context):

        pd_fact_sales = bronze_olist_orders_dataset.merge(
            bronze_olist_order_items_dataset, on = "order_id"
            ).merge(bronze_olist_order_payments_dataset, on = "order_id")
        
        select_columns = [
            "order_id",
            "customer_id",
            "order_purchase_timestamp",
            "product_id",
            "payment_value",
            "order_status",
        ]
        pd_fact_sales = pd_fact_sales[select_columns]

        self.minio_io_manager.handle_output(context, pd_fact_sales)
        return pd_fact_sales