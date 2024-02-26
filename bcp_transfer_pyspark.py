#!/usr/bin/env python 
#bcp automated code to transfer tables from GCP env to any SQL Server
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
import subprocess
separator = "="
keys = {}
properties=sys.argv[1]
def init():
    spark=SparkSession.builder.appName("BCP Module").enableHiveSupport().getOrCreate()
    return spark

if __name__ == '__main__':
    with open('{}'.format(properties),'r') as f:
        for line in f:
            if separator in line:
                name, value = line.split(separator, 1)
                keys[name.strip()] = value.strip()
    print(keys)
    spark=init()
    spark.sparkContext.setLogLevel("ERROR")
    target_list=keys["target_table"].split(",")
    db="{}".format(keys["database"])
    print(db)
    print(target_list)
    for i in range(len(target_list)):
        print(target_list[i])
        targettable="<table_schema>.{}".format(target_list[i]) #add table schema
        data_file="</path/>{}/{}.txt".format(target_list[i],target_list[i]) #replace the path, it'll take the list of target table
#        userid="{}".format(keys["username"])
        userid="<>" #add userid
        passwd="<>" #add password
        sqlserver="-S<>" #add server
        try:
            sqlcommand="/opt/mssql-tools/bin/sqlcmd " + sqlserver + " -d " + db + " -Q " + chr(34) + "truncate table {}".format(targettable) + chr(34) + " -U " + userid +" -P " + passwd
            subprocess.call(sqlcommand, shell=True)
            print(sqlcommand)
            command="/opt/mssql-tools/bin/bcp " + chr(34) + targettable + chr(34) + " IN " + data_file + " -c -q -t0x07 -U " + userid + " -P " + passwd + " " + sqlserver
            subprocess.call(command, shell=True)
            print(command)
            print(target_list[i]+ " has been transferred to sql server")
        except RuntimeError as e:
            print(e.message)
