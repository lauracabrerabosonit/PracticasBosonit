# -*- coding: utf-8 -*-
"""
Created on Mon Mar 13 10:03:42 2023

@author: laura.cabrera
"""

from pyspark.sql import SparkSession 
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F

from pyspark.sql.types import LongType

import pandas as pd

# Declare the cubed function 
def cubed(a: pd.Series) -> pd.Series:
 return a * a * a


# Create the pandas UDF for the cubed function 
cubed_udf = pandas_udf(cubed, returnType=LongType())


# Create a Pandas Series
x = pd.Series([1, 2, 3])

# The function for a pandas_udf executed with local Pandas data
#print(cubed(x))


if __name__ == "__main__":
 # Create a SparkSession
    spark = (SparkSession
              .builder
              .appName("Example-3_6")
              .getOrCreate())

    df = spark.range(1, 4)
    
    df.select("id", cubed_udf(col("id"))).show()

