# -*- coding: utf-8 -*-
"""
Created on Tue Mar 21 11:33:58 2023

@author: laura.cabrera
"""

from pyspark.sql.window import Window
from pyspark.sql import SparkSession 
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F

from pyspark.sql.types import LongType
import pandas as pd


# File location and type
file_location ="C:/Practica_Bosonit/Ejercicios_Spark/reto1/trade_details.csv"
file_location2 = "C:/Practica_Bosonit/Ejercicios_Spark/reto1/trade_details_snapshot.csv"
file_type = "csv"

infer_schema = "true"
first_row_is_header = "true"
delimiter = ";"





"""Importamos el csv"""

if __name__ == "__main__":
 # Create a SparkSession
    spark = (SparkSession
              .builder
              .appName("Reto_1")
              .getOrCreate())
    
    
    
    """DF"""
    
# The applied options are for CSV files. For other file types, these will be ignored.
    df = spark.read.format(file_type) \
        .option("inferSchema", infer_schema) \
        .option("header", first_row_is_header) \
        .option("sep", delimiter) \
        .load(file_location) 
        
    #df.show()

    """DF2"""        
        
    df2 = spark.read.format(file_type) \
        .option("inferSchema", infer_schema) \
        .option("header", first_row_is_header) \
        .option("sep", delimiter) \
        .load(file_location2) 
        
    #df2.show()
    
    
    """Creamos DF3 (con las columnas maturity2 y origin_contract_number2)"""
        
    df3 = df2.select(col("maturity").alias("maturity2"), col("origin_contract_number").alias("origin_2"))
    #df3.show()
    
    
    
    
    """Join de DF / DF3"""
    df_join = df.join(df3, df.origin_contract_number == df3.origin_2)
   # df_join.show()
    
    
    
    """FUNCIONES DE VENTANA (una para los nulos y otra para los no nulos)"""
    windowSpecNulos = Window.partitionBy("origin_contract_number").orderBy(desc("maturity2"))
    windowSpecNormal = Window.partitionBy("origin_contract_number").orderBy(desc("maturity"))

   
    
    df4 = df_join.withColumn("rank", 
                              when(df_join.maturity.isNull(), rank().over(windowSpecNulos))
                              .otherwise(rank().over(windowSpecNormal)))
    
    """Filter"""

    df5 = df4.filter((df4.mfamily != "CURR") | (df4.mgroup != "FXD") | (df4.mtype != "SWLEG") | (df4.rank == 1))

    """Quitamos las columnas sobrantes"""

    df_final = df5.drop("maturity2", "rank", "origin_2")

    df_final.show()  
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    