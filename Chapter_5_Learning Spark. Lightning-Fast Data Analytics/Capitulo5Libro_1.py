# -*- coding: utf-8 -*-
"""
Created on Mon Mar 13 09:42:23 2023

@author: laura.cabrera
"""

from pyspark.sql import SparkSession 
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F

from pyspark.sql.types import LongType



if __name__ == "__main__":
 # Create a SparkSession
    spark = (SparkSession
              .builder
              .appName("Example-3_6")
              .getOrCreate())



# Create cubed function
    def cubed(s):
        return s * s * s





# Register UDF
    spark.udf.register("cubed", cubed, LongType())

# Generate temporary view
    spark.range(1, 9).createOrReplaceTempView("udf_test")

    spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()
