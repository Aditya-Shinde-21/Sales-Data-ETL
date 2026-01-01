from pyspark.sql import *
from resources.dev import config
from scripts.main.utility.encrypt_decrypt import decrypt

def spark_session():
    spark = SparkSession.builder.master("local[4]")\
        .appName("test_run") \
        .config("spark.driver.extraClassPath",
        "<filepath-to-mysql-connector-jar>") \
        .config("spark.hadoop.fs.s3a.access.key", decrypt(config.aws_access_key))\
        .config("spark.hadoop.fs.s3a.secret.key", decrypt(config.aws_secret_key))\
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{config.region}.amazonaws.com")\
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.path.style.access", "true")\
        .config("spark.driver.memory", "4g") \
        .config("spark.driver.memoryOverhead", "512m")\
        .config("spark.sql.shuffle.partitions", "8")\
        .config("spark.default.parallelism", "8")\
        .getOrCreate()

    return spark

if __name__ == "__main__":
    spark = spark_session()
    print("Created spark session:", spark)

    spark.stop()
