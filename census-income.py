from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType



### defining schema ###


schema_census_income = StructType([
	StructField("age", IntegerType(), True),
	StructField("workclass", StringType(), True),
	StructField("fnlwgt", FloatType(), True),
	StructField("education", StringType(), True),
	StructField("education-num", IntegerType(), True),
	StructField("marital-status", StringType(), True),
	StructField("occupation", StringType(), True),
	StructField("relashionship", StringType(), True),
	StructField("race", StringType(), True),
	StructField("sex", StringType(), True),
	StructField("capital-gain", FloatType(), True),
	StructField("capital-loss", FloatType(), True),
	StructField("hours-per-week", IntegerType(), True),
	StructField("native-country", StringType(), True),
	StructField("income", StringType(), True),

])


### transforming data for answer some business questions ###

# all information in specifics columns like '\?' will be replaced to Unknown 
# by convention all marital-status startswith 'Married' will be replaced to 'married' 
# for specific business question, all information like 'White' will be replaced to 'white'

def census_income_tr(df):
    df = (df.withColumn('workclass',
                        F.when(F.col('workclass').rlike('\?'), 'Unknown')
                         .otherwise(F.col('workclass')))
            .withColumn('occupation',
                        F.when(F.col('occupation').rlike('\?'), 'Unknown')
                         .otherwise(F.col('occupation')))
            .withColumn('native-country',
                        F.when(F.col('native-country').rlike('\?'), 'Unknown')
                         .otherwise(F.col('native-country')))
            .withColumn('married_status',
                        F.when(F.col('marital-status').startswith('Married'), 'married')
                         .otherwise('no-married'))
            .withColumn('white-ratio', 
                        F.when(F.col('race').contains('White'), 'white')
                         .otherwise('no-white')))
    
    return df

###  answering some business questions ###


def question_1_workers_over50k(df):
	df.groupBy(['workclass','income']).count().sort(['income','count'],ascending=False).show()
	return df

def question_2_avgweekly_workbyrace(df):
	df.groupBy('race').agg(F.round(F.avg(F.col('hours-per-week')), 2).alias('Average Weekly work by race')).show()
	return df

def question_3_4_ratio_gender(df):
	(df.groupBy('sex')
       .count()
       .withColumn('Ratio by Gender', F.round((F.col('count')/df.count()), 2))
       .show())
	return df

def question_5_avg_weeklywork_byoccupation(df):
	df.groupBy('occupation').agg(F.round(F.avg(F.col('hours-per-week')), 2).alias('Average Weekly work by occupation')).show()
	return df

def question_6_mostcommon_occupation_byleveleducation(df):
	(df.groupBy('education', 'occupation')
       .count()
       .orderBy(F.col('count').desc())
       .dropDuplicates(['education'])
       .show())
	return df

def question_7_mostcommon_occupation_bygender(df):
	(df.groupBy('occupation', 'sex')
       .count()
       .orderBy(F.col('count').desc())
       .dropDuplicates(['sex'])
       .show())
	return df

def question_8_highleveleducation_byrace(df):
	(df.groupBy('race', 'education')
       .count()
       .orderBy(F.col('count').desc())
       .dropDuplicates(['race'])
       .show())
	return df

def question_9_selfemployed_characteristics(df):
	(df.where(F.col('workclass').contains('Self-emp'))
       .groupBy('workclass', 'education', 'sex', 'race')
       .count()
       .orderBy(F.col('count').desc())
       .dropDuplicates(['workclass'])
       .show())
	return df

def question_10_maritalstatus_ratio(df):
	(df.groupBy('marital-status')
       .count()
       .withColumn('marital-status-ratio', F.round((F.col('count')/df.count()), 2))
       .show())
	return df

def question_11_mostcommon_race_notmarried(df):
	(df.where(F.col('married_status') == 'no-married')
       .groupBy('married_status', 'race')
       .count()
       .orderBy(F.col('count').desc())
       .limit(1)
       .show())

	return df

def question_12_mostcommon_income_bymaritalstatus(df):
	(df.groupBy('married_status', 'income')
       .count()
       .orderBy(F.col('count').desc())
       .dropDuplicates(['married_status'])
       .show())
	return df

def question_13_mostcommon_income_bygender(df):
	(df.groupBy('sex', 'income')
    .count()
    .orderBy(F.col('count').desc())
    .dropDuplicates(['sex'])
    .show())

	return df

def question_14_mostcommon_income_bynationality(df):
	(df.where(F.col('native-country').isNotNull())
       .groupBy('native-country', 'income')
       .count()
       .orderBy(F.col('count').desc())
       .dropDuplicates(['native-country'])
       .select('native-country', 'income')
       .show())
	return df

def question_15_ratio_whitepeople(df):
	(df.groupBy('white-ratio')
       .count()
       .withColumn('ratio', F.round((F.col('count')/df.count()), 2))
       .show())
	return df






if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Census Income]"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          .schema(schema_census_income)
		          .load("/home/spark/capgemini-aceleracao-pyspark-old/data/census-income/census-income.csv"))


# calling previous functions #


df_tr = census_income_tr(df)

question_1_workers_over50k(df_tr)
question_2_avgweekly_workbyrace(df_tr)
question_3_4_ratio_gender(df_tr)
question_5_avg_weeklywork_byoccupation(df_tr)
question_6_mostcommon_occupation_byleveleducation(df_tr)
question_7_mostcommon_occupation_bygender(df_tr)
question_8_highleveleducation_byrace(df_tr)
question_9_selfemployed_characteristics(df_tr)
question_10_maritalstatus_ratio(df_tr)
question_11_mostcommon_race_notmarried(df_tr)
question_12_mostcommon_income_bymaritalstatus(df_tr)
question_13_mostcommon_income_bygender(df_tr)
question_14_mostcommon_income_bynationality(df_tr)
question_15_ratio_whitepeople(df_tr)

