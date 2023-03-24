# -*- coding: utf-8 -*-
"""
Created on Tue Mar 21 13:29:05 2023

@author: laura.cabrera
"""
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.types import LongType



spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
sparkContext=spark.sparkContext


"""Abstraer una lista a un RDD
Un RDD se define como una colección de elementos que es tolerante a fallos 
y que es capaz de operar en paralelo."""

"""collect() es una operación de acción que se usa 
para recuperar todos los elementos del conjunto de datos (de todos los nodos)
al nodo controlador."""

lista = [1,4,5,6,7]
rdd=sparkContext.parallelize(lista)
rddCollect = rdd.collect()
#print("Number of Partitions: "+str(rdd.getNumPartitions()))
#print("Action: First element: "+str(rdd.first()))
#print(rddCollect)



"""Formar un conjunto de RDD que contenga una lista con el elemento junto con el valor 
“0” si el elemento es divisible entre 2 y “1” en caso contrario
Aplicaremos una función lambda que crearemos a parte"""

def func1(x):
    if x % 2 == 0:
        return [x,0]
    else:
        return [x,1]

rdd2 = rdd.map(lambda x: func1(x))
rddCollect2 = rdd2.collect()
#print(rddCollect2)




"""Descartar aquellos que sean pares
Lo haremos con un filter"""

rdd3 = rdd2.filter(lambda x: x[1]==1)
rddCollect3 = rdd3.collect() 
#print(rddCollect3)


"""Suma de los elementos originales"""
rdd4 = rdd3.map(lambda x: x[0])
rddCollect4 = rdd4.collect()
#print(rddCollect4)
sum = rdd4.reduce(lambda x,y: x+y)
#print(sum)



"""Primero dar la vuelta, para tener (clave,valor)"""
rdd6 = rdd2.map(lambda x: (x[1],x[0]))
rddCollect6 = rdd6.collect()
#print(rddCollect6)
"""Hacemos reduceByKey"""
rdd7 = rdd6.reduceByKey(lambda x,y: x+y)
rddCollect7 = rdd7.collect()
#print(rddCollect7)







 
 



