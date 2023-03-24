# -*- coding: utf-8 -*-
"""
Created on Thu Mar 23 12:17:08 2023

@author: laura.cabrera
"""


from pyspark.sql.window import Window
from pyspark.sql import SparkSession 
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F

from pyspark.sql.types import LongType
import pandas as pd
import re

"""File location and type """

file_location ="C:/PracticasBosonit/Ejercicios_Spark/datosSpark.csv"
file_type = "csv"



infer_schema = "true"
first_row_is_header = "true"

"""Nuestro delimiter van a aser tabulaciones"""

delimiter = "\t"




"""Creamos SparkSession"""
if __name__ == "__main__":
 # Create a SparkSession
    spark = (SparkSession
              .builder
              .appName("Ejercicio_2")
              .getOrCreate())
        
    """Creamos DF"""
# The applied options are for CSV files. For other file types, these will be ignored.
    df = spark.read.format(file_type) \
        .option("inferSchema", infer_schema) \
        .option("header", first_row_is_header) \
        .option("sep", delimiter) \
        .load(file_location) 
        
   # df.show()
    
    """Limpiamos la tabla: categoria y precio"""      
    df2 = df.replace("tecnologia", "tecnología").replace("Oficina", "oficina").withColumn("precio", regexp_replace("precio", ",", ".")) 
    
    df3 = df2.withColumn("precio", df2.precio.cast('decimal(36,18)'))
    
    df3.show()
    
    
    """Calcular precio de los productos por categorias"""
    """Para saber el tipo de cada columna df.dtypes"""
    
    df4 = df3.groupBy("categoría").sum("precio")
    df4.show()
    
    
    """Para el siguiente apartado os creamos otro archivo CSV 
    que sea la tabla de los productos PREMIUM y NO PREMIUM"""
    
    
    file_location2 ="C:/PracticasBosonit/Ejercicios_Spark/datosSparkTabla.csv"
    
    df_tablaextra = spark.read.format(file_type) \
        .option("inferSchema", infer_schema) \
        .option("header", first_row_is_header) \
        .option("sep", delimiter) \
        .load(file_location2) \
        .withColumnRenamed("categoría", "categoría2")
    
   # df_tablaextra.show()
    
    
    df_join = df3.join(df_tablaextra, df_tablaextra.categoría2 == df3.categoría).drop("categoría2")
    
   # df_join.show()
    
    
    df5 = df_join.filter(col("Premium") == "YES").groupBy("categoría").sum("precio")
    
    df5.show()
    
 
    """Importamos un nuevo csv"""
    file_location3 ="C:/PracticasBosonit/Ejercicios_Spark/datosSparkTabla2.csv"

    df_final = spark.read.format(file_type) \
        .option("inferSchema", infer_schema) \
        .option("header", first_row_is_header) \
        .option("sep", delimiter) \
        .load(file_location3) 
        
   # df_final.show()
    
    """Limpiamos la tabla: categoria y precio"""      
    df_final2 = df_final.replace("tecnologia", "tecnología").replace("Oficina", "oficina").withColumn("precio", regexp_replace("precio", ",", ".")) 
    
    df_final3 = df_final2.withColumn("precio", df_final2.precio.cast('decimal(36,18)')).withColumn("Precio_premium", df_final2.Precio_premium.cast('decimal(36,18)'))
    
   # df_final3.show()
    
    
    
    df_final4 = df_final3.filter(col("Campo_precio") == "precio").groupBy("categoría").sum("precio")
    df_final5 = df_final3.filter(col("Campo_precio") == "Precio_premium").groupBy("categoría").sum("Precio_premium")
    #df_final4.show()
    
    #df_final5.show()
   
    df_final6 = df_final4.join(df_final5, df_final4.categoría == df_final5.categoría, "outer")

    df_final6.show()
    
    
    
    
    



