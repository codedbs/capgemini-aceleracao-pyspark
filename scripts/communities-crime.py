import re
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
import pyspark.sql

import numpy as np
from pyspark.sql.functions import from_unixtime, unix_timestamp,expr
from pyspark.sql.functions import isnan, when, count, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

###  data cleaning ###

def question_1_police_operating_budget (df):
	(df.where(F.col("PolicOperBudg")!='?')
	.groupBy('communityname')
	.agg(
			F.sum(
				F.col("PolicOperBudg"))
	.alias("MaxSumPolicOperBudg"))
	.orderBy(F.col("MaxSumPolicOperBudg")
	.desc())
	.show())

def question_2_violentcrimes_percommunity (df):
	(df.where(F.col("ViolentCrimesPerPop")!='?')
	.groupBy('communityname')
	.agg(
		F.round(
			F.sum(
				F.col("ViolentCrimesPerPop")),2)
	.alias("MaxSumViolentCrimeperCommunity"))
	.orderBy(F.col("MaxSumViolentCrimeperCommunity")
	.desc())
	.show())


def question_3_community_largest_population (df):
	(df.where(F.col("population")!='?')
	.groupBy('communityname')
	.agg(
			F.sum(
				F.col("population"))
	.alias("Community_largest_population"))
	.orderBy(F.col("Community_largest_population")
	.desc())
	.show())	

def question_4_community_black_people (df):
(df.where(F.col("racepctblack")!='?')
	.groupBy('communityname')
	.agg(
			F.sum(
				F.col("racepctblack"))
	.alias("Community_black_people"))
	.orderBy(F.col("Community_black_people")
	.desc())
	.show())	


if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Communities & Crime]"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          #.schema(schema_communities_crime)
		          .load("/home/spark/capgemini-aceleracao-pyspark-old/data/communities-crime/communities-crime.csv"))

#df.orderBy('communityname').agg({'PolicBudgPerPop': 'max'}).show()




# checks how many null values ​​there are - considering null by the label '?'
#df.select([count(when(col(c)=='?', c)).alias(c) for c in df.columns]).show(vertical=True)
question_1_police_operating_budget(df)
question_2_violentcrimes_percommunity(df)
question_3_community_largest_population(df)