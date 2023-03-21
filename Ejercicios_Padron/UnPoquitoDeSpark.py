# -*- coding: utf-8 -*-
from pyspark.sql.window import Window
from pyspark.sql import SparkSession 
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F

from pyspark.sql.types import LongType
import pandas as pd

# File location and type
file_location ="C:/Practica_Bosonit/Ejercicios_Padron/estadisticas202212.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ";"

""" Importamos el csv """

if __name__ == "__main__":
 # Create a SparkSession
    spark = (SparkSession
              .builder
              .appName("Ejercicio_6")
              .getOrCreate())
# The applied options are for CSV files. For other file types, these will be ignored.
    df = spark.read.format(file_type) \
        .option("inferSchema", infer_schema) \
        .option("header", first_row_is_header) \
        .option("sep", delimiter) \
        .load(file_location) \
        .na.fill(value=0) \
        .withColumn("DESC_DISTRITO",F.trim(F.col("DESC_DISTRITO"))) \
        .withColumn("DESC_BARRIO",F.trim(F.col("DESC_BARRIO")))
        
        
    #df.show(n=5)
    #display(df.limit(5))
    
    
    
    
    
    """ Creamos una vista temportal y a través de ella cuenta el número de barrios diferentes que hay.  """

    

    temp_table_name = "estadisticas202212.csv"
    df.createOrReplaceTempView(temp_table_name)



    df1 = df.select(df.DESC_BARRIO).distinct()
    
    #df1.show()
    #display(df1.limit(5))
    


    """ Creamos una nueva columna llamada longitud que nos de la longitud de los campos DESC_DISTRITO"""
    
    
    df2 = df.withColumn("longitud",F.length(df.DESC_DISTRITO))
    
    df2.show()
    
    
    """ Creamos una nueva columna que muestre siempre el valor 5"""
    df3 = df.withColumn("valor_5", F.lit(5))
    #df3.show(n=5)
    
    
    
   
    """ Borra la columna """

    df4 = df3.drop("valor_5")
    #df4.show(n=5)
    
    
    
    """Particiona el DataFrame por las variables DESC_DISTRITO y DESC_BARRIO"""

    df5 = df4.repartition(F.col("DESC_BARRIO"), F.col("DESC_DISTRITO")).cache()
    
    #df5.show()
    

    """Lanza una consulta contra el DF resultante en la que muestre el número total de 
     "espanoleshombres", "espanolesmujeres", extranjeroshombres" y "extranjerosmujeres" 
     para cada barrio de cada distrito """

    
    df6 = df5.groupBy("DESC_BARRIO", "DESC_DISTRITO").agg(F.sum(F.col('espanoleshombres').cast('int')).alias('espanoleshombres'), F.sum(F.col('espanolesmujeres').cast('int')).alias('espanolesmujeres'),  F.sum(F.col('extranjeroshombres').cast('int')).alias('extranjeroshombres'), F.sum(F.col('extranjerosmujeres').cast('int')).alias('extranjerosmujeres') ).orderBy("extranjerosmujeres", "extranjeroshombres").cache()
    #df6.show()

    #df6.unpersist()


    """ Crea un nuevo DataFrame a partir del original que muestre únicamente una columna con 
    DESC_BARRIO, otra con DESC_DISTRITO y otra con el número total de "espanoleshombres" 
    residentes en cada distrito de cada barrio."""
    

    df7 = df.groupBy("DESC_BARRIO", "DESC_DISTRITO").agg(F.sum(F.col('espanoleshombres').cast('int')).alias('espanoleshombres')).cache()
    
    
    
    """Únelo (con un join) con el DataFrame original a 
         través de las columnas en común"""

    
    
    df_join = df7.join( df6  , (df6.DESC_BARRIO  == df7.DESC_BARRIO) & (df6.DESC_DISTRITO  == df7.DESC_DISTRITO)) 
    #df_join.show()
    
    
    """Repite la función anterior utilizando funciones de ventana. (over(Window.partitionBy.....))."""
    
    window = Window.partitionBy( F.col("DESC_BARRIO"), F.col("DESC_DISTRITO"))

    df_window = df4.withColumn( "espanoleshombres", F.sum(F.col('espanoleshombres').cast('int')).over(window))

    #jdbcDF_window = df4.select(F.col("DESC_BARRIO"), F.col("DESC_DISTRITO"), F.col("espanoleshombres"), F.col("espanolesmujeres") , F.col("extranjeroshombres"), F.col("extranjerosmujeres") , F.col("extranjerosmujeres")).agg(F.sum(F.col('espanoleshombres').cast('int')).alias('espanoleshombres'), F.sum(F.col('espanolesmujeres').cast('int')).alias('espanolesmujeres'), F.sum(F.col('extranjeroshombres').cast('int')).alias('extranjeroshombres'), F.sum(F.col('extranjerosmujeres').cast('int')).alias('extranjerosmujeres') ).withColumn( "rn", row_number().over(window)).cache()

   # display(df_window.limit(5))
   
   
   
    """  Mediante una función Pivot muestra una tabla (que va a ser una tabla de contingencia) que
       contenga los valores totales ()la suma de valores) de espanolesmujeres para cada distrito y 
        en cada rango de edad (COD_EDAD_INT). Los distritos incluidos deben ser únicamente 
        CENTRO, BARAJAS y RETIRO y deben figurar como columnas . El aspecto debe ser similar a 
        este:"""
        
        
    pivotDF = df.where( F.col("DESC_DISTRITO").isin('CENTRO', 'BARAJAS' , 'RETIRO')).groupBy("COD_EDAD_INT").pivot("DESC_DISTRITO").sum("espanolesmujeres").orderBy("COD_EDAD_INT")
    #pivotDF.show()
    
    
    nuevo_pivotDF = pivotDF.withColumn("%BARAJAS", (pivotDF.BARAJAS / (pivotDF.CENTRO + pivotDF.BARAJAS + pivotDF.RETIRO)) * 100).withColumn("%CENTRO", (pivotDF.CENTRO / (pivotDF.CENTRO + pivotDF.BARAJAS + pivotDF.RETIRO)) * 100).withColumn("%RETIRO", (pivotDF.RETIRO / (pivotDF.CENTRO + pivotDF.BARAJAS + pivotDF.RETIRO)) * 100)
    nuevo_pivotDF.show()
    
    
    
    """ Guarda archivo CSV """
    
    df.write.option("header", True) \
        .partitionBy("DESC_BARRIO", "DESC_DISTRITO") \
        .mode("overwrite") \
        .saveAsTable("datos_padron")
        
        
        
    """Hacer lo mismo pero con el formato PARQUET """
    
    df.write.format("parquet").mode("overwrite").partitionBy("DESC_BARRIO", "DESC_DISTRITO").save("/tmp/datos_padron_parquet")
    
    
    """Ver el tamaño del directorio"""





""" SPARK Y HIVE """

import pyspark
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

data = spark.range(0, 5)
data.write.format("delta").save("/tmp/delta-table")


#mostramos la tabla delta creada previamente

df = spark.read.format("delta").load("/tmp/delta-table")
df.show()




        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    





    
    