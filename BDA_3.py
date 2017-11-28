#-------------------------------------------------------#
# 2017/11/29 Yukihiro Suzukiが作成
#-------------------------------------------------------#
#!/usr/bin/env python
#coding:utf-8

#--------------------------------------------------------#
# Import the modules. #
#--------------------------------------------------------#
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import explode,split
from string import join
import re

import sys
reload(sys)
sys.setdefaultencoding('utf-8')
#------------------------------------------------------------------------------------#

#--------------------------------------------------------#
# Set SparkSession.  #
#--------------------------------------------------------#
spark = SparkSession \
.builder \
.appName("App example") \
.config("master", "yarn") \
.getOrCreate()
#------------------------------------------------------------------------------------#


#--------------------------------------------------------#
# Read .json data from HDFS. #
#--------------------------------------------------------#
df = spark.read.json("hdfs://bda-ns/user/yukihiro-su/tweet_20171004.json")
#------------------------------------------------------------------------------------#


#---------------------------------------------------------------------------------------------------------------------------#
# MAIN PROCESS #
#---------------------------------------------------------------------------------------------------------------------------#
text = df.where(col('text').like("%選挙%")).groupBy("text").count().sort("count",ascending=False).show(20,False)
ID = df.where(col('text').like("%選挙%")).groupBy("user.id").count().sort("count",ascending=False).show(20,False)
NAME = df.where(col('text').like("%選挙%")).groupBy("user.name").count().sort("count",ascending=False).show(20,False)
F_a = df.where(col('text').like("%選挙%")).groupBy("user.followers_count").count().sort("count",ascending=False)
F_b = df.where(col('text').like("%選挙%")).select("user.name","user.followers_count")
F_name = F_a.join(F_b, F_a.followers_count == F_b.followers_count,'inner').select("name","count").show(1)


Gi = df.where(col('text').like("%自民党%")).select("user.created_at").distinct().select(explode(split(’user.created_at’,’ ’)[2])).groupBy("col").count().sort("count",ascending=False).show(10,False)
Min = df.where(col('text').like("%民主党%")).select("user.created_at").distinct().select(explode(split(’user.created_at’,’ ’)[2])).groupBy("col").count().sort("count",ascending=False).show(10,False)
Kou = df.where(col('text').like("%公明党%")).select("user.created_at").distinct().select(explode(split(’user.created_at’,’ ’)[2])).groupBy("col").count().sort("count",ascending=False).show(10,False)
Kyo = df.where(col('text').like("%共産党%")).select("user.created_at").distinct().select(explode(split(’user.created_at’,’ ’)[2])).groupBy("col").count().sort("count",ascending=False).show(10,False)
Sya = df.where(col('text').like("%社民党%")).select("user.created_at").distinct().select(explode(split(’user.created_at’,’ ’)[2])).groupBy("col").count().sort("count",ascending=False).show(10,False)


Gi_loc = df.where(col('text').like("%自民党%")).groupBy("user.location").count().sort("count",ascending=False).show(10,False)
Min_loc = df.where(col('text').like("%民主党%")).groupBy("user.location").count().sort("count",ascending=False).show(10,False)
#--------------------------------------------------------------------------------------------------------------------------------------#

print(text)
print(ID)
print(NAME)
print(F_name)
print(Gi,Min,Kou,Kyo,Sya)
print(Gi_loc,Min_loc)
