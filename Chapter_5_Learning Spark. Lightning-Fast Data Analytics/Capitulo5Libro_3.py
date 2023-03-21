# -*- coding: utf-8 -*-
"""
Created on Mon Mar 13 12:00:11 2023

@author: laura.cabrera
"""

from pyspark.sql import SparkSession 
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F

from pyspark.sql.types import *



if __name__ == "__main__":
 # Create a SparkSession
    spark = (SparkSession
              .builder
              .appName("Example-3_6")
              .getOrCreate())


    schema = StructType([StructField("celsius", ArrayType(IntegerType()))])
    t_list = [[35, 36, 32, 30, 40, 42, 38]], [[31, 32, 34, 55, 56]]
    t_c = spark.createDataFrame(t_list, schema)
    t_c.createOrReplaceTempView("tC")
# Show the DataFrame
    t_c.show()
    
    spark.sql("""SELECT celsius, transform(celsius, t -> ((t * 9) div 5) + 32) as fahrenheit FROM tC """).show()


# Filter temperatures > 38C for array of temperatures
spark.sql("""SELECT celsius, filter(celsius, t -> t > 38) as high FROM tC""").show()

#Is there a temperature of 38C in the array of temperatures
spark.sql("""SELECT celsius, exists(celsius, t -> t = 38) as thresholdFROM tC""").show()



#Calculate average temperature and convert to F
spark.sql("""SELECT celsius, reduce(celsius, 0, (t, acc) -> t + acc, acc -> (acc div size(celsius) * 9 div 5) + 32) as avgFahrenheit FROM tC""").show()




