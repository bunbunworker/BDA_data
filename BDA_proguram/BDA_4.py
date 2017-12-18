#---------------------------------------------------#
# 2017/11/29 Yukihiro Suzuki #
#---------------------------------------------------#
# coding:utf-8



#--------------------------------------------------------#
# Import the modules. #
#--------------------------------------------------------#
import csv
from pyspark.ml.linalg import Vectors
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql import SQLContext, Row
from pyspark.mllib.feature import Word2Vec
from pyspark.sql.functions import col
import re

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

#--------------------------------------------------------#
# Set SparkSession.  #
#--------------------------------------------------------#
spark = SparkSession \
.builder \
.appName("App example") \
.config("master", "yarn") \
.getOrCreate()


#--------------------------------------------------------#
# Read .json data from HDFS. #
#--------------------------------------------------------#
df = spark.read.json("hdfs://bda-ns/user/yukihiro-su/tweet_20171004.json")
#------------------------------------------------------------------------------------#


#---------------------------------------------------------------------------------------------------------------------------#
# MAIN PROCESS #
#---------------------------------------------------------------------------------------------------------------------------#
# Open the file.
f1 = open('devil.csv', 'w')
writer1 = csv.writer(f, lineterminator='\n')
f2 = open('devil.csv', 'w')
writer2 = csv.writer(f, lineterminator='\n')


tweet1 = df.filter("lang='en'").filter("possibly_sensitive='true'").select("text").distinct() \
        .rdd.map(lambda x: x[0]).take(1000)
tweet2 = df.filter("lang='en'").filter("possibly_sensitive='false'").select("text").distinct() \
        .rdd.map(lambda x: x[0]).take(1000)

# output
writer1.writerow(tweet1)
writer2.writerow(tweet2)

f1.close()
f2.close()
