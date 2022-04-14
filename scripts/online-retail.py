# importing libs needed for development #

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
    return (F.col(col).isNull() | (F.col(col) == '')) | F.col(col).rlike(REGEX_EMPTY_STR)

### ---------------------------------------------- ###

# checking the quality on specific columns #

def col_quantity_qa(df):
    df = df.withColumn('Quantity_qa', 
                        F.when(F.col('Quantity').isNull(), 'M')) # M = Missing values #
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

# answering some business questions #

def question_1(df):
	(df
	.filter((F.col('StockCode').startswith('gift_0001')) & # Only gift cards starts with 'gift_001' #
			(~F.col('InvoiceNo').startswith('C')) & # Disregarding canceled sales #
			(F.col('Quantity') > 0)) # only completed sales #
	.agg({'Quantity' : 'sum'})
	.show())

def question_2(df):
	
	(df
	.filter((F.col('StockCode').startswith('gift_0001')) & # Only gift cards starts with 'gift_001' #
			(~F.col('InvoiceNo').startswith('C')) & # Disregarding canceled sales #
			(F.col('Quantity') > 0)) # only completed sales #
	.groupBy(F.month('InvoiceDate'))
	.sum('Quantity')
	.orderBy('month(InvoiceDate)')
	.show())

def question_3(df):

	(df
	.filter((F.col('StockCode') == 'S') & # Only samples #
			(~F.col('InvoiceNo').startswith('C')) & # Disregarding canceled sales #
			(F.col('Quantity') > 0))  # only completed sales #
	.groupBy('StockCode').sum('Quantity')
	.show())
	
def question_4(df):

	(df
	.filter((~F.col('InvoiceNo').startswith('C')) & # Disregarding canceled sales #
			(F.col('Quantity') > 0) &  # only completed sales #
			(F.col('StockCode') != 'PADS')) # Disregarding PADS code #
	.groupBy('StockCode')
	.sum('Quantity')
	.orderBy(F.col('sum(Quantity)').desc())
	.show())


def question_5(df):

	df = (
		df
		.filter((~F.col('InvoiceNo').startswith('C')) & # Disregarding canceled sales #
				(F.col('Quantity') > 0) &  # only completed sales #
				(F.col('StockCode') != 'PADS')) # Disregarding PADS code #
		.groupBy('StockCode', F.month('InvoiceDate'))
		.sum('Quantity')
		.orderBy(F.col('sum(Quantity)').desc())
	)


	df = df.select('StockCode',
				F.col('month(InvoiceDate)').alias('month'),
				F.col('sum(Quantity)').alias('sum_quantity'))
	
	df_max_per_month = df.groupBy('month').max('sum_quantity')

	df_max_per_month = df_max_per_month.join(df.alias('b'), 
								F.col('b.sum_quantity') == F.col('max(sum_quantity)'),
								"left").select('b.month','StockCode','sum_quantity')
	
	df_max_per_month.orderBy('month').show()


def question_6(df):
	
	(df
	.filter(F.col('valor_de_venda') > 0 &  # only completed sales #
	 		(F.col('StockCode') != 'PADS')) # Disregarding PADS code #
	.groupBy(F.hour('InvoiceDate'))
	.sum('valor_de_venda')
	.orderBy(F.col('sum(valor_de_venda)').desc())
	.show())




def question_7(df):
	
	(df
	.filter(F.col('valor_de_venda') > 0 &   # only completed sales #
			(F.col('StockCode') != 'PADS')) # Disregarding PADS code #
	.groupBy(F.month('InvoiceDate'))
	.sum('valor_de_venda')
	.orderBy(F.col('sum(valor_de_venda)').desc())
	.show())




def question_8(df):

	(df
	.filter((F.col('valor_de_venda') > 0) &  # only completed sales #
			(F.col('StockCode') != 'PADS')) # Disregarding PADS code #
	.groupBy(F.month('InvoiceDate'), F.col('StockCode'))
	.sum('valor_de_venda')
	.orderBy(F.col('sum(valor_de_venda)').desc())
	.show(1))



def question_9(df):

	(df
	.filter((F.col('valor_de_venda') > 0) & # only completed sales #
			(F.col('StockCode') != 'PADS')) # Disregarding PADS code #
	.groupBy('Country')
	.sum('valor_de_venda')
	.orderBy(F.col('sum(valor_de_venda)').desc())
	.show(1))



def question_10(df):
		
	(df
	.filter((F.col('valor_de_venda') > 0) & # only completed sales #
			(F.col('StockCode') != 'PADS') & # Disregarding PADS code #
			(F.col('StockCode') == 'M')) # Only manual sales #
	.groupBy('Country')
	.sum('valor_de_venda')
	.orderBy(F.col('sum(valor_de_venda)').desc())
	.show(1))





def question_11(df):

	maior_valor_de_venda = (df
				.filter(F.col('StockCode') != 'PADS') # Disregarding PADS code #
				.agg({'valor_de_venda' : 'max'})
				.first()[0])

	(df
	.select('InvoiceNo','valor_de_venda')
	.filter(F.col('valor_de_venda') == maior_valor_de_venda)
	.show())


def question_12(df):

	maior_n_de_itens = (df
				.filter(F.col('StockCode') != 'PADS') # Disregarding PADS code #
				.agg({'Quantity' : 'max'})
				.first()[0])

	(df
	.select('InvoiceNo','Quantity')
	.filter(F.col('Quantity') == maior_n_de_itens)
	.show())

def question_13(df):

	mais_frequente = (df
					.filter(
						(F.col('StockCode') != 'PADS') & # Disregarding PADS code #
						(F.col('CustomerID').isNotNull()) #  Disregarding empty info from CustomerID #
						)
					.groupBy('CustomerID')
					.count()
					.orderBy(F.col('count').desc())
					.first()[1])

	(df
	.filter(
		(F.col('StockCode') != 'PADS') & # Disregarding PADS code #
		(F.col('CustomerID').isNotNull()) #  Disregarding empty info from CustomerID #
		)
	.groupBy('CustomerID')
	.count()
	.filter(F.col('count') == mais_frequente)
	.show())

### ---------------------------------------------- ###

# main session #

if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Online Retail]"))

	schema_online_retail = StructType([
		StructField("InvoiceNo", StringType(), True),
		StructField("StockCode", StringType(), True),
		StructField("Description", StringType(), True),
		StructField("Quantity", IntegerType(), True),
		StructField("InvoiceDate", StringType(), True),
		StructField("UnitPrice", StringType(), True),
		StructField("CustomerID", StringType(), True),
		StructField("Country", StringType(), True)
	])

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          .schema(schema_online_retail)
                  .load("/home/spark/capgemini-aceleracao-pyspark-old/data/online-retail/online-retail.csv"))
    
    
### ---------------------------------------------- ###

# calling previous functions #

#df = col_quantity_qa(df)
#df = col_quantity_tf(df)
#question_1(df)

#df = col_quantity_qa(df)
#df = pergunta_2_tr(df)
#pergunta_2(df)

#df = pergunta_3_qa(df)
#df = pergunta_3_tr(df)
#pergunta_3(df)

#df = pergunta_4_qa(df)
#df = pergunta_4_tr(df)
#pergunta_4(df)

#df = pergunta_5_qa(df)
#df = pergunta_5_tr(df)
#pergunta_5(df)

#df = pergunta_6_qa(df)
#df = pergunta_6_tr(df)
#pergunta_6(df)

#df = pergunta_7_qa(df)
#df = pergunta_7_tr(df)
#pergunta_7(df)

#df = pergunta_8_qa(df)
#df = pergunta_8_tr(df)
#pergunta_8(df)

#df = pergunta_9_qa(df)
#df = pergunta_9_tr(df)
#pergunta_9(df)

#df = pergunta_10_qa(df)
#df = pergunta_10_tr(df)
#pergunta_10(df)

#df = pergunta_11_qa(df)
#df = pergunta_11_tr(df)
#pergunta_11(df)

#df = pergunta_12_qa(df)
#df = pergunta_12_tr(df)
#pergunta_12(df)

#pergunta_13_qa(df)
#pergunta_13(df)






