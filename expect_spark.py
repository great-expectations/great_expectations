from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql.types import *
import pandas as pd
import json
from dateutil.parser import parse

spark = SparkSession.builder.getOrCreate()
data_path = 'tests/test_definitions/column_map_expectations/expect_column_values_to_be_increasing.json'
spark.sparkContext.setLogLevel('ERROR')

with open(data_path) as dp:
    dp = json.load(dp)
    

df = dp['datasets'][0]['data']
df = pd.DataFrame(df)
df = spark.createDataFrame(df)
# #assert isinstance(df.schema['x'].dataType,DoubleType) | isinstance(df.schema['x'].dataType,IntegerType)

# #need to move 

        
# def expect_column_values_to_be_increasing(df,column,strictly=False,parse_strings_as_datetimes=None):
    
#     na_types = [isinstance(df.schema[column[0]].dataType,typ) for typ in [LongType,DoubleType,IntegerType]]
#     if any(na_types):
#         df = df.withColumn('na', when(isnan(col(column[0])),1).otherwise(0))\
#             .filter(col('na')==0)
   
#     df = df.withColumn('constant',lit('constant'))

#     if parse_strings_as_datetimes:
#          #_udf = udf(parse, TimestampType())
#          df = df\
#             .withColumn(column[0],col(column[0]).cast(TimestampType()))\
#             .withColumn('lag',lag(column[0]).over(Window.orderBy(col('constant'))))\
#             .withColumn('diff',datediff(col(column[0]),col('lag')))

    
    
#     else:
#          df = df\
#             .withColumn('lag',lag(column[0]).over(Window.orderBy(col('constant'))))\
#             .withColumn('diff',col(column[0])-col('lag'))

#     # replace first row null lag with 1 so that it is not flagged as fail
#     df = df.withColumn('diff',when(col('diff').isNull(),1).otherwise(col('diff')))
#     #print(df.show())
#     if strictly:
#         df = df.withColumn('__success',when(col('diff')>=1,1).otherwise(0))

#     else:
#         df = df.withColumn('__success',when(col('diff')>=0,1).otherwise(0))
        
#     print(column)
#     print(df.show())
#     return df.filter(col('__success')==0).count() == 0
    
# print(df.printSchema())
# assert True == expect_column_values_to_be_increasing(df, ['x'],strictly=False)
# assert True == expect_column_values_to_be_increasing(df, ['y'],strictly=False)
# assert False == expect_column_values_to_be_increasing(df, ['y'],strictly=True)
# assert False == expect_column_values_to_be_increasing(df, ['w'],strictly=False)
# assert True == expect_column_values_to_be_increasing(df, ['a'],strictly=False)
# assert False == expect_column_values_to_be_increasing(df, ['b'],strictly=False)
# assert True == expect_column_values_to_be_increasing(df,['zz'],strictly=False,parse_strings_as_datetimes=True)


