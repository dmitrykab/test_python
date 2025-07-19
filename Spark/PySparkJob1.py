from datetime import datetime
from pyspark.sql.functions import col, split
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DateType,
    DoubleType,
)


# Создаем SparkSession
spark = SparkSession.builder.appName("DataFrameFilteringPractice").getOrCreate()

schema = StructType(
    [
        StructField("order_id", IntegerType(), False),
        StructField("customer_name", StringType(), False),
        StructField("order_date", DateType(), False),
        StructField(
            "items",
            StructType(
                [
                    StructField("product_name", StringType(), False),
                    StructField("quantity", IntegerType(), False),
                    StructField("price", DoubleType(), False),
                ]
            ),
            False,
        ),
    ]
)

data = [
    (
        1,
        "Иван Петров",
        datetime.strptime("2023-09-15", "%Y-%m-%d").date(),
        ("Ноутбук", 1, 999.99),
    ),
    (
        2,
        "Анна Сидорова",
        datetime.strptime("2023-09-16", "%Y-%m-%d").date(),
        ("Смартфон", 2, 599.50),
    ),
    (
        3,
        "Елена Иванова",
        datetime.strptime("2023-09-17", "%Y-%m-%d").date(),
        ("Наушники", 3, 79.99),
    ),
    (
        4,
        "Анна Козлова",
        datetime.strptime("2023-09-18", "%Y-%m-%d").date(),
        ("Смартфон", 1, 699.99),
    ),
    (
        5,
        "Петр Смирнов",
        datetime.strptime("2023-09-19", "%Y-%m-%d").date(),
        ("Планшет", 1, 449.99),
    ),
]

df = spark.createDataFrame(data, schema)

df_after_15sep = df.filter(col('order_date') > '2023-09-15')
df_after_15sep.show()
df_quantity_gt_1 = df.filter(col('items')['quantity'] >1)
df_quantity_gt_1.show()
df_price_gt_500 = df.filter(col('items')['price'] >500)
df_price_gt_500.show()
df_anna_orders = df.filter(split("customer_name", " ")[0].cast("String") == 'Анна')
df_anna_orders.show()
df_smartphone_orders = df.filter(col('items')['product_name'] == 'Смартфон')
df_smartphone_orders.show()

spark.stop()
