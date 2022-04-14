# importing libs needed for development #

import re
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
import numpy as np
from pyspark.sql.functions import from_unixtime, unix_timestamp,expr
from pyspark.sql.functions import isnan, when, count, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

### analyzing data integrity ###

# checking if there is data in the dataframe #
def check_empty_df (col):
    return print(df.count() > 0)

# if there are columns with negative values, it will show the amount#
### ----coming soon ---- ###

# if there are null columns, it will show the amount#
def check_exist_null(col):
    return df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# regex to validate empty string and non-numeric columns #
REGEX_EMPTY_STR = r'[\t ]+$'
REGEX_ISNOT_NUM = r'[^0-9]*'

# checking for null or empty columns#
def check_empty_column(col):
    return (F.col(col).isNull() | (F.col(col) == '')) | F.col(col).rlike(REGEX_EMPTY_STR))

### ---------------------------------------------- ###

# checking the quality on specific columns #

def col_quantity_qa(df):
	df = df.withColumn('Quantity_qa', 
					F.when(F.col('Quantity').isNull(), 'M'))
	
    df.groupBy('Quantity_qa').count().show()

	return df

def col_invoicedate_qa(df):
	df = df.withColumn('InvoiceDate_qa', F.when(check_is_empty('InvoiceDate'), 'M')) # M = Missing values #
    
    df = df.withColumn('Quantity_qa', 
					F.when(F.col('Quantity').isNull(), 'M')) # M = Missing values #

    df.groupBy('Quantity_qa').count().show()

	df.groupBy('InvoiceDate_qa').count().show() 

    return df

def col_unitprice_qa(df):
	df = df.withColumn("UnitPrice_qa", 
					F.when(check_is_empty('UnitPrice'), 'M') # M = Missing values #
					.when(F.col('UnitPrice').contains(','), 'F')  # F = Float type values #
					.when(F.col('UnitPrice').rlike('[^0-9]'), 'A') # A = Number type values #
	)
	df = df.withColumn('Quantity_qa',
					F.when(F.col('Quantity').isNull(), 'M')) # M = Missing values #

	df = df.withColumn('InvoiceDate_qa', F.when(check_is_empty('InvoiceDate'), 'M')) # M = Missing values #

	df.groupBy('UnitPrice_qa').count().show()

	df.groupBy('Quantity_qa').count().show()	

	df.groupBy('InvoiceDate_qa').count().show() 

	return df

### ---------------------------------------------- ###

# transforming the columns to meet the business rule #

def col_quantity_tf(df):
	df = df.withColumn('Quantity',
				F.when(F.col('Quantity_qa') == 'M', 0) # M = Missing values #
				.otherwise(F.col('Quantity')))

	df.filter(F.col('Quantity').isNull()).show()

	return df

def col_invoicedate_tf(df):
	df = df.withColumn('Quantity',
				F.when(F.col('Quantity_qa') == 'M', 0) # M = Missing values #
				.otherwise(F.col('Quantity')))

	df = df.withColumn('InvoiceDate', 
					F.to_timestamp(F.col('InvoiceDate'), 'd/M/yyyy H:m')) # Column changed to timestamp type ## 

	df.filter(F.col('Quantity').isNull()).show()
	df.filter(F.col('InvoiceDate').isNull()).show()

	return df

def col_unitprice_tf(df):
	df = df.withColumn('InvoiceDate', 
					F.to_timestamp(F.col('InvoiceDate'), 'd/M/yyyy H:m')) # Column changed to timestamp type ## 
	df.filter(F.col('InvoiceDate').isNull()).show()
	df = df.withColumn('UnitPrice', 
				F.when(df['UnitPrice_qa'] == 'F', # F = Float type values #
					F.regexp_replace('UnitPrice', ',','\\.'))
				.otherwise(F.col('UnitPrice'))
				)
	df = df.withColumn('UnitPrice', F.col('UnitPrice').cast('double'))
	df.filter(F.col('UnitPrice').isNull()).show()
	df = df.withColumn('Quantity',
				F.when(F.col('Quantity_qa') == 'M', 0)  # M = Missing values #
				.otherwise(F.col('Quantity')))
	df.filter(F.col('Quantity.').isNull()).show()
	df = df.withColumn('valor_de_venda', F.col('UnitPrice') * F.col('Quantity'))
	return df

### ---------------------------------------------- ###


if __name__ == "__main__":
    sc = SparkContext()
    spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Online Retail]"))

    df = (spark.getOrCreate().read
                  .format("csv")
                  .option("header", "true")
                  #.schema(schema_online_retail)
                  .load("/home/spark/capgemini-aceleracao-pyspark-old/data/online-retail/online-retail.csv"))
    
    

#check_exist_null(col)
#cancelled(col)


#check_empty_df (col)
check_exist_negative(col)