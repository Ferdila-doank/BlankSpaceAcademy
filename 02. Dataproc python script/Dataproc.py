from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,BooleanType,DoubleType,DateType  
import datetime

spark = SparkSession.builder.getOrCreate()

date = datetime.datetime.now()
strdate = datetime.datetime.now().strftime('%Y-%m-%d')

newDateFile = datetime.datetime.strptime(strdate, '%Y-%m-%d') 
newDateFile += datetime.timedelta(days=23)

filename = newDateFile.strftime('%Y-%m-%d') + '.json'

# Define custom schema
schema = StructType([
      StructField("flight_date",DateType(),True),
      StructField("airline_code",StringType(),True),
      StructField("flight_num",IntegerType(),True),
      StructField("source_airport",StringType(),True),
      StructField("destination_airport",StringType (),True),
      StructField("departure_time",StringType(),True),
      StructField("departure_delay",IntegerType(),True),
      StructField("arrival_time",StringType(),True),
      StructField("arrival_delay",IntegerType(),True),
      StructField("airtime",IntegerType (),True),
      StructField("distance",IntegerType (),True),
      StructField("id",IntegerType (),True)
  ])

df_with_schema = spark.read.schema(schema) \
        .json("gs://week_3_bucket/Data/" + filename)
df_with_schema.printSchema()
df_with_schema.show(5)

df_with_schema.groupBy("source_airport","destination_airport") \
              .avg("departure_delay","arrival_delay","airtime") \
              .show()

df_with_schema.write.csv("gs://week_3_bucket/Output/Summary.csv",header=True,mode="append")
