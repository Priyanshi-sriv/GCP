from pyspark.sql import SparkSession 
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys


def init():
    spark=SparkSession.builder.appName("Pyspark transfer").enableHiveSupport().getOrCreate()
    return spark

spark=init()
spark.sparkContext.setLogLevel("ERROR")
query='select * from <table_name>'
print(query)

source_df=spark.sql(query)
source_df.show()

source_df.write.mode("overwrite") \
    .format("jdbc") \
    .option("url", f"jdbc:sqlserver://<>;databaseName=<>;") \ #add sql server and databaseName
    .option("dbtable", '<table>') \ #add table name
    .option("user", '<>') \ #add username  
    .option("password", '<>') \ #add password
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .save()

