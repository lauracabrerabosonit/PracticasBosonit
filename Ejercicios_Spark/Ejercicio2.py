# -*- coding: utf-8 -*-


from pyspark.sql.window import Window
from pyspark.sql import SparkSession 
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.types import LongType
import pandas as pd


# File location and type
file_location ="C:/Practica_Bosonit/Ejercicios_Spark/reto2/udfs.csv"
file_type = "csv"

infer_schema = "true"
first_row_is_header = "true"
delimiter = ";"


"""Importamos el csv"""

if __name__ == "__main__":
 # Create a SparkSession
    spark = (SparkSession
              .builder
              .appName("Reto_2")
              .getOrCreate())
    
    
    
    """DF"""
    
# The applied options are for CSV files. For other file types, these will be ignored.
    df = spark.read.format(file_type) \
        .option("inferSchema", infer_schema) \
        .option("header", first_row_is_header) \
        .option("sep", delimiter) \
        .load(file_location) 
        
   # df.show()
    
    
    """Primer pivot (string)"""
    
    pivotDF = (df.groupBy("nb")
               .pivot("udf_name", ["M_CLIENT", "M_SELLER", "M_CCY", "M_SUCURSAL"])
               .agg(first("string_value"))
               .withColumnRenamed("M_SELLER", "M_seller"))
    
    
    
    """Segundo pivot (numerico)"""
    
    df2 = (df.groupBy("nb")
               .pivot("udf_name", ["M_DISCMARGIN", "M_DIRECTIAV", "M_LIQDTYCHRG", "M_CRDTCHRG", "M_MVA", "M_RVA"])
               .agg(first("num_value"))
               .withColumnRenamed("M_DISCMARGIN", "M_discmargin")
               .withColumnRenamed("M_DIRECTIAV", "M_directiav")
               .withColumnRenamed("M_LIQDTYCHRG", "M_liqdtychrg")
               .withColumnRenamed("M_CRDTCHRG", "M_crdtchrg")
               .withColumnRenamed("M_MVA", "M_mva")
               .withColumnRenamed("M_RVA", "M_rva")
               
               )
    
       
    
    
    DF2_final= df2.filter((df2.M_discmargin != 0) & (df2.M_discmargin.isNotNull()) | (df2.M_directiav != 0) & (df2.M_directiav.isNotNull())
                    & (df2.M_liqdtychrg != 0) & (df2.M_liqdtychrg.isNotNull()) & (df2.M_crdtchrg != 0) & (df2.M_crdtchrg.isNotNull())
                    & (df2.M_mva != 0) & (df2.M_mva.isNotNull()) & (df2.M_rva != 0) & (df2.M_rva.isNotNull()))
    
               
    
    
    DF1_final = pivotDF.filter(pivotDF.M_seller != "")
    
    
    df_final = DF1_final.join(DF2_final, DF1_final.nb == DF2_final.nb).show()
    
    
    
    
