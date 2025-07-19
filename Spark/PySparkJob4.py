from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, desc


# Создаем SparkSession
spark = SparkSession.builder.appName("DataFrameJoining").getOrCreate()

#Не меняйте названия DataFrames. Мы ожидаем при проверке увидеть именно эти назватия DF. Если сменить название DF, задание не пройдет проверку грейдером.
customers_data = [
    (1, "Иван Петров", "ivan@example.com"),
    (2, "Анна Сидорова", "anna@example.com"),
    (3, "Елена Иванова", "elena@example.com"),
    (4, "Петр Смирнов", "petr@example.com"),
    (5, "Мария Кузнецова", "maria@example.com")
]
customers_schema = ["customer_id", "customer_name", "email"]
customers_df = spark.createDataFrame(customers_data, customers_schema)

orders_data = [
    (101, 1, "2023-09-15", 999.99),
    (102, 2, "2023-09-16", 1199.00),
    (103, 3, "2023-09-17", 239.97),
    (104, 2, "2023-09-18", 699.99),
    (105, 4, "2023-09-19", 449.99)
]
orders_schema = ["order_id", "customer_id", "order_date", "total_amount"]
orders_df = spark.createDataFrame(orders_data, orders_schema)

products_data = [
    (201, "Ноутбук", 999.99, 10),
    (202, "Смартфон", 599.50, 20),
    (203, "Наушники", 79.99, 50),
    (204, "Планшет", 449.99, 15)
]
products_schema = ["product_id", "product_name", "price", "stock_quantity"]
products_df = spark.createDataFrame(products_data, products_schema)

order_items_data = [
    (101, 201, 1),
    (102, 202, 2),
    (103, 203, 3),
    (104, 202, 1),
    (105, 204, 1)
]
order_items_schema = ["order_id", "product_id", "quantity"]
order_items_df = spark.createDataFrame(order_items_data, order_items_schema)

df_customer_orders = customers_df.alias('cu'). \
    join(orders_df.alias('or'), 'customer_id'). \
    select('cu.customer_id', 'cu.customer_name', 'cu.email', 'or.order_id', 'or.order_date', 'or.total_amount'). \
    sort(desc('order_date'))
df_customer_orders.show()

df_order_details = orders_df.alias('odf'). \
    join(order_items_df.alias('oi'), 'order_id').\
    join(products_df.alias('pr'), 'product_id'). \
    filter(col('total_amount') > 500)
df_order_details.show()

df_all_customers = customers_df. \
    join(orders_df, 'customer_id', 'left')
df_all_customers.show()


df_customer_total = customers_df.join(orders_df, "customer_id") \
                                .join(order_items_df, "order_id") \
                                .join(products_df, "product_id") \
                                .groupBy("customer_id", "customer_name") \
                                .agg(sum(col("price") * col("quantity")).alias("total_purchases")) \
                                .orderBy(desc("total_purchases"))

df_top_products = order_items_df.join(products_df, "product_id") \
                                .groupBy("product_id", "product_name") \
                                .agg(sum("quantity").alias("total_quantity")) \
                                .orderBy(desc("total_quantity"), desc("product_id")) \
                                .limit(3)
print("Топ-3 самых продаваемых продукта:")
df_top_products.show()

spark.stop()
