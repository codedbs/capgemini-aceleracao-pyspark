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

### defining schema ###

schema_communities_crime = StructType([ 
    StructField('state', IntegerType(), True),
    StructField('county', IntegerType(), True),
    StructField('community', IntegerType(), True),
    StructField('communityname', StringType(), True),
    StructField('fold', IntegerType(), True),
    StructField('population', FloatType(), True),
    StructField('householdsize', FloatType(), True),
    StructField('racepctblack', FloatType(),  True),
    StructField('racePctWhite',FloatType(), True),
    StructField('racePctAsian', FloatType(), True),
    StructField('racePctHisp', FloatType(), True),
    StructField('agePct12t21', FloatType(), True),
    StructField('agePct12t29', FloatType(), True),
    StructField('agePct16t24', FloatType(), True),
    StructField('agePct65up', FloatType(), True),
    StructField('numbUrban', FloatType(), True),
    StructField('pctUrban', FloatType(), True),
    StructField('medIncome', FloatType(), True),
    StructField('pctWWage', FloatType(), True),
    StructField('pctWFarmSelf', FloatType(), True),
    StructField('pctWInvInc', FloatType(), True),
    StructField('pctWSocSec', FloatType(), True),
    StructField('pctWPubAsst', FloatType(), True),
    StructField('pctWRetire', FloatType(), True),
    StructField('medFamInc', FloatType(), True),
    StructField('perCapInc', FloatType(), True),
    StructField('whitePerCap', FloatType(), True),
    StructField('blackPerCap', FloatType(), True),
    StructField('indianPerCap', FloatType(), True),
    StructField('AsianPerCap', FloatType(), True),
    StructField('OtherPerCap', FloatType(), True),
    StructField('HispPerCap', FloatType(), True),
    StructField('NumUnderPov', FloatType(), True),
    StructField('PctPopUnderPov', FloatType(), True),
    StructField('PctLess9thGrade', FloatType(), True),
    StructField('PctNotHSGrad', FloatType(), True),
    StructField('PctBSorMore', FloatType(), True),
    StructField('PctUnemployed', FloatType(), True),
    StructField('PctEmploy', FloatType(), True),
    StructField('PctEmplManu', FloatType(), True),
    StructField('PctEmplProfServ', FloatType(), True),
    StructField('PctOccupManu', FloatType(), True),
    StructField('PctOccupMgmtProf', FloatType(), True),
    StructField('MalePctDivorce', FloatType(), True),
    StructField('MalePctNevMarr', FloatType(), True),
    StructField('FemalePctDiv', FloatType(), True),
    StructField('TotalPctDiv', FloatType(), True),
    StructField('PersPerFam', FloatType(), True),
    StructField('PctFam2Par', FloatType(), True),
    StructField('PctKids2Par', FloatType(), True),
    StructField('PctYoungKids2Par', FloatType(), True),
    StructField('PctTeen2Par', FloatType(), True),
    StructField('PctWorkMomYoungKids', FloatType(), True),
    StructField('PctWorkMom', FloatType(), True),
    StructField('NumIlleg', FloatType(), True),
    StructField('PctIlleg', FloatType(), True),
    StructField('NumImmig', FloatType(), True),
    StructField('PctImmigRecent', FloatType(), True),
    StructField('PctImmigRec5', FloatType(), True),
    StructField('PctImmigRec8', FloatType(), True),
    StructField('PctImmigRec10', FloatType(), True),
    StructField('PctRecentImmig', FloatType(), True),
    StructField('PctRecImmig5', FloatType(), True),
    StructField('PctRecImmig8', FloatType(), True),
    StructField('PctRecImmig10', FloatType(), True),
    StructField('PctSpeakEnglOnly', FloatType(), True),
    StructField('PctNotSpeakEnglWell', FloatType(), True),
    StructField('PctLargHouseFam', FloatType(), True),
    StructField('PctLargHouseOccup', FloatType(), True),
    StructField('PersPerOccupHous', FloatType(), True),
    StructField('PersPerOwnOccHous', FloatType(), True),
    StructField('PersPerRentOccHous', FloatType(), True),
    StructField('PctPersOwnOccup', FloatType(), True),
    StructField('PctPersDenseHous', FloatType(), True),
    StructField('PctHousLess3BR', FloatType(), True),
    StructField('MedNumBR', FloatType(), True),
    StructField('HousVacant', FloatType(), True),
    StructField('PctHousOccup', FloatType(), True),
    StructField('PctHousOwnOcc', FloatType(), True),
    StructField('PctVacantBoarded', FloatType(), True),
    StructField('PctVacMore6Mos', FloatType(), True),
    StructField('MedYrHousBuilt', FloatType(), True),
    StructField('PctHousNoPhone', FloatType(), True),
    StructField('PctWOFullPlumb', FloatType(), True),
    StructField('OwnOccLowQuart', FloatType(), True),
    StructField('OwnOccMedVal', FloatType(), True),
    StructField('OwnOccHiQuart', FloatType(), True),
    StructField('RentLowQ', FloatType(), True),
    StructField('RentMedian', FloatType(), True),
    StructField('RentHighQ', FloatType(), True),
    StructField('MedRent', FloatType(), True),
    StructField('MedRentPctHousInc', FloatType(), True),
    StructField('MedOwnCostPctInc', FloatType(), True),
    StructField('MedOwnCostPctIncNoMtg', FloatType(), True),
    StructField('NumInShelters', FloatType(), True),
    StructField('NumStreet', FloatType(), True),
    StructField('PctForeignBorn', FloatType(), True),
    StructField('PctBornSameState', FloatType(), True),
    StructField('PctSameHouse85', FloatType(), True),
    StructField('PctSameCity85', FloatType(), True),
    StructField('PctSameState85', FloatType(), True),
    StructField('LemasSwornFT', FloatType(), True),
    StructField('LemasSwFTPerPop', FloatType(), True),
    StructField('LemasSwFTFieldOps', FloatType(), True),
    StructField('LemasSwFTFieldPerPop', FloatType(), True),
    StructField('LemasTotalReq', FloatType(), True),
    StructField('LemasTotReqPerPop', FloatType(), True),
    StructField('PolicReqPerOffic', FloatType(), True),
    StructField('PolicPerPop', FloatType(), True),
    StructField('RacialMatchCommPol', FloatType(), True),
    StructField('PctPolicWhite', FloatType(), True),
    StructField('PctPolicBlack', FloatType(), True),
    StructField('PctPolicHisp', FloatType(), True),
    StructField('PctPolicAsian', FloatType(), True),
    StructField('PctPolicMinor', FloatType(), True),
    StructField('OfficAssgnDrugUnits', FloatType(), True),
    StructField('NumKindsDrugsSeiz', FloatType(), True),
    StructField('PolicAveOTWorked', FloatType(), True),
    StructField('LandArea', FloatType(), True),
    StructField('PopDens', FloatType(), True),
    StructField('PctUsePubTrans', FloatType(), True),
    StructField('PolicCars', FloatType(), True),
    StructField('PolicOperBudg', FloatType(), True),
    StructField('LemasPctPolicOnPatr', FloatType(), True),
    StructField('LemasGangUnitDeploy', FloatType(), True),
    StructField('LemasPctOfficDrugUn', FloatType(), True),
    StructField('PolicBudgPerPop', FloatType(), True),
    StructField('ViolentCrimesPerPop', FloatType(), True),
     ])

# checking how many null values ​​there are - considering null by the label '?'
def quantity_null(df):
	return df.select([count(when(col(c)=='?', c)).alias(c) for c in df.columns]).show(vertical=True)

### searching null values ###

def check_is_null(df):
    return (F.col(df).isNull())


###  answering some business questions ###

def question_1_police_budget(df):

	df = (df.withColumn("PolicOperBudg", F.when(check_is_null('PolicOperBudg'), 0)
							.otherwise(F.col("PolicOperBudg")))
			)
	(df.groupBy(F.col('state'), F.col('communityname'))
	.agg(F.round(F.sum(F.col('PolicOperBudg')), 2).alias('PoliceBudget'))
	.orderBy(F.col('PoliceBudget').desc())
	.show())

def question_2_violentcrimes_percommunity(df):

	df = (df.withColumn('ViolentCrimesPerPop', F.when(check_is_null('ViolentCrimesPerPop'), 0)
							.otherwise(F.col('ViolentCrimesPerPop')))
		)
	(df.groupBy(F.col('state'), F.col('communityname'))
	.agg(F.round(F.sum(F.col('ViolentCrimesPerPop')), 2).alias('ViolentCrimes'))
	.orderBy(F.col('ViolentCrimes').desc())
	.show())


def question_3_community_largest_population(df):

	df = (df.withColumn('population', F.when(check_is_null('population'), 0)
							.otherwise(F.col('population')))
		)
	(df.groupBy(F.col('state'), F.col('communityname'))
	.agg(F.round(F.sum(F.col('population')), 2).alias('Community_largest_population'))
	.orderBy(F.col('Community_largest_population').desc())
	.show())

def question_4_community_black_people(df):

	df = (df.withColumn('racepctblack', F.when(check_is_null('racepctblack'), 0)
							.otherwise(F.col('racepctblack')))
		)
	(df.groupBy(F.col('state'), F.col('communityname'))
	.agg(F.round(F.sum(F.col('racepctblack')), 2).alias('Community_black_people'))
	.orderBy(F.col('Community_black_people').desc())
	.show())

def question_5_salaried_community(df):

	df = (df.withColumn('pctWWage', F.when(check_is_null('pctWWage'), 0)
								.otherwise(F.col('pctWWage')))
		)
	(df.groupBy(F.col('state'), F.col('communityname'))
	.agg(F.round(F.sum(F.col('pctWWage')), 2).alias('salaried_community'))
	.orderBy(F.col('salaried_community').desc())
	.show())
		
def question_6_young_community(df):

	df = (df.withColumn('agePct12t21', F.when(check_is_null('agePct12t21'), 0)
							.otherwise(F.col('agePct12t21')))
		)
	(df.groupBy(F.col('state'), F.col('communityname'))
	.agg(F.round(F.sum(F.col('agePct12t21')), 2).alias('young_between_12_21'))
	.orderBy(F.col('young_between_12_21').desc())
	.limit(1)
	.show())

def question_7_correlationI(df):
   
    df.agg(F.round(F.corr('PolicOperBudg', 'ViolentCrimesPerPop'), 2).alias('Correlation_PolicOperBudg-ViolentCrimesPerPop')).show()


def question_8_correlationII(df):
    
    df.agg(F.round(F.corr('PctPolicWhite', 'PolicOperBudg'), 2).alias('Correlation_PctPolicWhite-PolicOperBudg')).show()


def question_9_correlationIII(df):
    
    df.agg(F.round(F.corr('population', 'PolicOperBudg'), 2).alias('Correlation_Population-PolicOperBudg')).show()


def question_10_correlationIV(df):
  
    df.agg(F.round(F.corr('population', 'ViolentCrimesPerPop'), 2).alias('Correlation_Population-ViolentCrimesPerPop')).show()

def question_11_correlationV(df):
    
    df.agg(F.round(F.corr('medIncome', 'ViolentCrimesPerPop'), 2).alias('Correlation_medIncome-ViolentCrimesPerPop')).show()

def question_12_predominant_race(df):
   
    (df.select('state', 'communityname', 'racepctblack', 'racePctWhite', 'racePctAsian', 'racePctHisp', 'ViolentCrimesPerPop')
	.orderBy(F.col('ViolentCrimesPerPop').desc())
    .limit(10)
    .show())




if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Communities & Crime]"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          .schema(schema_communities_crime)
		          .load("/home/spark/capgemini-aceleracao-pyspark-old/data/communities-crime/communities-crime.csv"))


# calling previous functions #


quantity_null(df)
question_1_police_budget(df)
question_2_violentcrimes_percommunity(df)
question_3_community_largest_population(df)
question_4_community_black_people(df)
question_5_salaried_community(df)
question_6_young_community(df)
question_7_correlationI(df)
question_8_correlationII(df)
question_9_correlationIII(df)
question_10_correlationIV(df)
question_11_correlationV(df)
question_12_predominant_race(df)