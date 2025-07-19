from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, desc, round, percent_rank, lit


# Создаем SparkSession
spark = SparkSession.builder.appName("DataFrameGroupingAggregation").getOrCreate()

# Создаем расширенный DataFrame с данными о заказах
data = [
    (1, "Иван Петров", "2023-09-15", "Ноутбук", "Электроника", 1, 999.99, "Курьер"),
    (2, "Анна Сидорова", "2023-09-16", "Смартфон", "Электроника", 2, 599.50, "Самовывоз"),
    (3, "Елена Иванова", "2023-09-17", "Наушники", "Аксессуары", 3, 79.99, "Почта"),
    (4, "Анна Козлова", "2023-09-18", "Смартфон", "Электроника", 1, 699.99, "Курьер"),
    (5, "Петр Смирнов", "2023-09-19", "Планшет", "Электроника", 1, 449.99, "Самовывоз"),
    (6, "Иван Петров", "2023-09-20", "Чехол", "Аксессуары", 2, 19.99, "Почта"),
    (7, "Анна Сидорова", "2023-09-20", "Зарядное устройство", "Аксессуары", 1, 29.99, "Самовывоз"),
    (8, "Елена Иванова", "2023-09-21", "Смартфон", "Электроника", 1, 799.99, "Курьер")
]

schema = ["order_id", "customer_name", "order_date", "product_name", "category", "quantity", "price", "delivery_method"]
df = spark.createDataFrame(data, schema)

df_orders_per_customer = df.groupBy('customer_name').agg(count("order_id").alias('total_orders'))
df_orders_per_customer.show()

df_total_by_category = df.groupBy('category').agg(sum(col("price")*col("quantity")).alias('total_value'))
df_total_by_category.show()

df.createOrReplaceTempView("etm1")

df_avg_check_per_day = df. \
    groupBy('order_date'). \
    agg(round(avg(col("price")*col("quantity")),2).alias('average_check'))    
df_avg_check_per_day.show()

df_top_products = df. \
    groupBy('product_name'). \
    agg(sum(col("quantity")). \
    alias("total_quantity")).\
    sort(desc(col('total_quantity'))). \
    limit(3)
df_top_products.show()

df.createOrReplaceTempView("etm")

sql_str = '''
select distinct delivery_method,
count(order_id) OVER(partition by delivery_method) as order_count,
round(count(order_id) OVER(partition by delivery_method)/count(order_id) over(),4)*100 as apercentage
from etm
order by delivery_method''' 
 
df_delivery_percentage = spark.sql(sql_str)
df_delivery_percentage.show()
