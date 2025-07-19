from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, col, when, coalesce, split, explode, array

# Создаем SparkSession
spark = SparkSession.builder.appName("ColumnOperations").getOrCreate()

# Создаем DataFrame
data = [
    (1, "Иван Петров", "Москва,Санкт-Петербург", 30, 50000, None),
    (2, "Анна Сидорова", "Казань", 25, 60000, "Менеджер"),
    (3, "Елена Иванова", "Новосибирск,Екатеринбург,Омск", 35, 70000, "Инженер"),
    (4, "Петр Смирнов", None, 40, 80000, "Директор"),
    (5, "Мария Кузнецова", "Владивосток", 28, 55000, None)
]
schema = ["id", "full_name", "cities", "age", "salary", "position"]
df = spark.createDataFrame(data, schema)

print("Исходный DataFrame:")
df.show()

df_greeting = df. \
    select(col("*"), concat(lit("Привет, "),col("full_name"), lit("! Ваш ID: "), col("id")).alias("greeting"))
df_greeting.show(truncate=False)

df_salary_grade = df. \
     withColumn(
        'salary_grade',
        when(col('salary').between(50000, 60000), 'Средняя').
        when(col('salary') > 60000, 'Высокая').
        otherwise('Низкая')
    )
df_salary_grade.show(truncate=False)

df_employee_info = df. \
    withColumn('employee_info', coalesce('position', lit('Сотрудник')))
df_employee_info.show(truncate=False)

df_exploded_cities = df.\
    select('full_name', 'cities').\
    withColumn('cities', explode(split('cities', ',')))
df_exploded_cities.show()

df_age_group = df. \
    withColumn(
    'age_group',
    when(col('age').between(18, 30), '18-30').
    when(col('age').between(31, 50), '31-50').
    otherwise('50+')
    )
df_age_group.show()
