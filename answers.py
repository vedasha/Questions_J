
#1)	Create a python functionality to do logging to hdfs
#given info
#hdfs_name_node_ip='x.x.x.x'
#hdfs_name_ndoe_port='1234'
#hdfs_user_name='test'
#ANSWER
import logging
logging.basicConfig(filename='hdfs://x.x.x.x:1234/user/test/Log ',level=logging.DEBUG)
logging.basicConfig(format='%(asctime)s %(message)s')
logging.info('++++Started My Program++++')



#QUESTION 2
#2)	Create a hive context in pyspark and set the below properties
#hive.merge.mapredfiles=false
#hive.merge.smallfiles.avgsize=16000000
#hive.execution.engine=mr

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('pyspark-read-and-write-from-hive').enableHiveSupport().getOrCreate()
spark.sql("SET hive.merge.mapredfiles=false")
spark.sql("SET hive.merge.smallfiles.avgsize=16000000")
spark.sql("SET hive.execution.engine=mr")

#QUESTION 3
#3)	Assume there is a column called email in a dataframe
#create a code snippet to generate a new column email_valid_flag which flags if the email is valid or not
#in pandas
#in pyspark
from validate_email import validate_email
import pandas as pd

df = pd.DataFrame({'email': ['yyyyyy','xxxx@ggg.com', 'gggg@gmail.com', 'a.b@hotmail.com', 'a.b.c@112.com','*&aghg@gh.com']})
df.assign(email_valid_flag = df['email'].apply(lambda x:validate_email(x)))

#Using Regular expression
import pandas as pd
import re

pattern = re.compile(r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)")  # this is the regex expression to search on
df['email_valid_flag'] = df['email'].apply(lambda x: True if pattern.match(x) else False)

#QUESTION 4
#4)	Convert a dataframe to list of dicts and read the same into a another dataframe with prefix of "a_" apended to the original column name (assume some data)
#a)	Code using pandas
#b)	Code using pyspark

#PANDAS
df_to_dic= pd.DataFrame.to_dict(df,orient= 'record')
#[{'email': 'yyyyyy', 'email_valid_flag': False}, {'email': 'xxxx@ggg.com', 'email_valid_flag': True},
# {'email': 'gggg@gmail.com', 'email_valid_flag': True}, {'email': 'a.b@hotmail.com', 'email_valid_flag': True}]
dict_to_df = pd.DataFrame.from_dict(df_to_dic)
dict_to_df.rename(columns=lambda x: "a_"+x , inplace=True)

#PYSPARK
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('pyspark-dict').getOrCreate()
 email = spark.read.csv('/xxx/xxx/python/email.csv',header=True,inferSchema=True)




 #QUESTION 5
 #5)	Using pyspark, create snippet to generate a sequential id column in a dataframe
 from pyspark.sql.functions import monotonically_increasing_id
 res = email.withColumn("seq_id", monotonically_increasing_id())

 
 #QUESTION6
 #6)	Create a  sample smartsheet using below link
#(https://www.smartsheet.com/(need to create a account first)
#Read the data from this smartsheet using its api and post it to  AWS bucket
#with bucketname='test_bucket'(note the bucket has server-side AES256 encryption enabled)
#key='test_key'
#using python
import boto3
import boto3.s3
from simple_smartsheet import Smartsheet
TOKEN = "XXXXXXX"
smartsheet = Smartsheet(TOKEN)
my_sheet = smartsheet.sheets.get("sample_data")
s3 = boto3.client('s3')

#QUESTION7
#Write a code snippet to do pivot table of a dataframe using pandas(assume some data)
df = pd.DataFrame({'name': ['mmm','xxxx', 'gggg'],'age':[23,45,67],'yob':['1990','1990','1991']})
pd.pivot_table(df, 'age', 'yob')

    #age
#yob
#1990	34
#1991	67

#QUESTION 8
#Write a code snippet to read a table from sqlitedb  into pandas dataframe(assume some data)
import sqlite3
import pandas as pd
# Create your connection.
cnx = sqlite3.connect('xxx.db')

df = pd.read_sql_query("SELECT * FROM table_name", cnx)

#QUESTION 9
#9) Assume that there is directory with mix of csv files delimited by comma(,),tab(\t) and pipe(|)
#they all have the same number of columns.
#create a python code snippet to read the files from above directory into a pandas dataframe
#and create a spark dataframe of the above pandas dataframe
import glob
from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark = SparkSession.builder.appName('read-csv-files').getOrCreate()
mySchema = StructType([ StructField("Col1", StringType(), True),StructField("Col2", IntegerType(), True),StructField("Col2", StringType(), True)])


for file in glob.glob('/Users/xxx/python/file/*.csv'):

    df = pd.read_csv(file, sep=',|[|]|[\\t]',
                  engine='python', header=None)
    print(df)
    spark_df = spark.createDataFrame(df,schema=mySchema)
    spark_df.printSchema
    print(spark_df)



#    0   1    2
#0   w  78  opo
#1  df  89  hjh
#DataFrame[Col1: string, Col2: int, Col2: string]
#   0  1  2
#0  m  3  e
#1  k  3  a
#DataFrame[Col1: string, Col2: int, Col2: string]
#    0   1    2
#0  qw  89  uoo
#1  tr  88   rr
#DataFrame[Col1: string, Col2: int, Col2: string]
