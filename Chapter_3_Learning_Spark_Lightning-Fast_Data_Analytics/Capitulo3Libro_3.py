# -*- coding: utf-8 -*-
"""
Created on Wed Mar  8 12:11:45 2023

@author: laura.cabrera
"""
from pyspark.sql import Row
from pyspark.sql import SparkSession

if __name__ == "__main__":
 # Create a SparkSession
    spark = (SparkSession
              .builder
              .appName("Example-3_6")
              .getOrCreate())

    rows = [Row("Matei Zaharia", "CA"), Row("Reynold Xin", "CA")]

    authors_df = spark.createDataFrame(rows, ["Authors", "State"])

    authors_df.show()
    
    print(authors_df.printSchema())

