from pyspark.sql import SparkSession 

spark = SparkSession.builder.getOrCreate()

from pyspark.sql.types import *
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("age", IntegerType(), False),
    StructField("course", StringType(), False)
])

data = [
    {
        "id": 1,
        "name": "Tom",
        "age": 22,
        "course": "A"
    },
    {
        "id": 2,
        "name": "Cat",
        "age": 22,
        "course": "B"
    },
    {
         "id": 3,
        "name": "Piter",
        "age": 20,
        "course": "C"
    },
    {
        "id": 4,
        "name": "Sony",
        "age": 21,
        "course": "D"
    },
    {
        "id": 5,
        "name": "Mac",
        "age": 19,
        "course": "F"
    }
]

df = spark.createDataFrame(data, schema=schema)
df.printSchema()
df.show(3)
spark.stop()