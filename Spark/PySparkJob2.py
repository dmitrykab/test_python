from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DateType,
    DoubleType,
)

from pyspark.sql.functions import col, desc, split

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

df_sorted_by_date = df.sort(desc('order_date'))
df_sorted_by_date.show()
df_sorted_by_price = df.sort(col('items')['price'])
df_sorted_by_price.show()
#df_sorted_by_name_quantity = df.sort(split("customer_name", " ")[0].cast("String"),desc(col('items')['quantity']))
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DateType,
    DoubleType,
)

from pyspark.sql.functions import col, desc, split

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

df_sorted_by_date = df.sort(desc('order_date'))
df_sorted_by_date.show()
df_sorted_by_price = df.sort(col('items')['price'])
df_sorted_by_price.show()
df_sorted_by_name_quantity = df.sort(col("customer_name"), desc(col('items')['quantity']))
#df_sorted_by_name_quantity = df.sort(split("customer_name", " ")[0].cast("String"),desc(col('items')['quantity']))
df_sorted_by_name_quantity.show()
df_sorted_by_total_cost = df.sort(desc(col('items')['price']*col('items')['quantity']))
df_sorted_by_total_cost.show()
df_sorted_by_product_date = df.sort(col('items')['product_name'], desc('order_date'))
df_sorted_by_product_date.show()

spark.stop()
