# importação de bibliotecas necessárias ao desenvolvimento

import re
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
import numpy as np
from pyspark.sql.functions import from_unixtime, unix_timestamp,expr
from pyspark.sql.functions import isnan, when, count, col

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

### análise de integridade dos dados ###

# verifica se existem dados no dataframe 
def check_empty_df (col):
    return print(df.count() > 0)

# se existirem campos com valores negativos, mostrará a quantidade
def check_exist_negative(col):
        df.sign = np.where(F.col("Quantity").isnull(), np.nan,
        np.where(F.col("Quantity") > 0,   'Positive', 'Negative'))
        return df




# se existirem campos nulos, mostrará a quantidade
def check_exist_null(col):
    return df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# regex para validar string vazia e campos numéricos
REGEX_EMPTY_STR = r'[\t ]+$'
REGEX_ISNOT_NUM = r'[^0-9]*'

# verificação de colunas nulas ou vazias
def check_empty_column(col):
    return (F.col(col).isNull() | (F.col(col) == ''))

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