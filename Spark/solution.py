#Spark connection with S3 options
import os
import socket
from pyspark.sql import SparkSession

# Замените следующие значения на свои credentials
aws_access_key = "grt8Zk6FILI9oP2QLByo"
aws_secret_key = "ZEqMUAfJzrvQSsQPcS63lLubOpAA2HUnVJr6nr3d"
s3_bucket = "startde-datasets"
s3_endpoint_url = "https://s3.lab.karpov.courses" 
 
APACHE_MASTER_IP = socket.gethostbyname("apache-spark-master-0.apache-spark-headless.apache-spark.svc.cluster.local")
APACHE_MASTER_URL = f"spark://{APACHE_MASTER_IP}:7077"
POD_IP = os.environ["MY_POD_IP"]


SPARK_APP_NAME = f"spark-{os.environ['HOSTNAME']}"  
JARS = """/nfs/env/lib/python3.8/site-packages/pyspark/jars/clickhouse-native-jdbc-shaded-2.6.5.jar, /nfs/env/lib/python3.8/site-packages/pyspark/jars/hadoop-aws-3.3.4.jar,  
/nfs/env/lib/python3.8/site-packages/pyspark/jars/aws-java-sdk-bundle-1.12.433.jar  
"""  
MEM = "512m"  
CORES = 1  

spark = SparkSession \
    .builder \
    .appName(SPARK_APP_NAME). \
    master("local"). \
    config("spark.executor.memory", MEM). \
    config("spark.jars", JARS). \
    config("spark.executor.cores", CORES). \
    config("spark.hadoop.fs.s3a.endpoint", s3_endpoint_url). \
    config("spark.hadoop.fs.s3a.access.key", aws_access_key). \
    config("spark.hadoop.fs.s3a.secret.key", aws_secret_key). \
    config("fs.s3a.endpoint", s3_endpoint_url). \
    config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"). \
    config("spark.hadoop.fs.s3a.path.style.access", True). \
    config("spark.hadoop.fs.s3a.committer.name", "directory"). \
    config("spark.hadoop.fs.s3a.aws.credentials.provider",
           "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"). \
    config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false"). \
    getOrCreate()


    s3_file_name = "orders.csv"  

s3_file_key = s3_bucket + "/shared/" + s3_file_name  

#print(s3_file_key)  
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType  

schema = StructType([  
    StructField('order_id', IntegerType()),  
    StructField('order_date', TimestampType()),  
    StructField('order_customer_id', IntegerType()),  
    StructField('order_status', StringType())  
])  

df_csv = spark.read.csv("s3a://startde-datasets/shared/orders.cs", schema=schema)  

df_csv.printSchema()
df_csv.count()

from pyspark.sql.functions import col

df_f = df_csv.filter(col('order_status') == 'PENDING_PAYMENT')
df_f.show()
df_f.coalesce(1).write.parquet("s3a://dmitrij-kabanov-lnj6487/orders_filtered", mode='overwrite')
spark.stop()