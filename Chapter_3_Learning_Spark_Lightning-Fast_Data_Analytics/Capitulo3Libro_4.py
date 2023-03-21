# -*- coding: utf-8 -*-
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F


fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                     StructField('UnitID', StringType(), True),
                     StructField('IncidentNumber', IntegerType(), True),
                     StructField('CallType', StringType(), True), 
                     StructField('CallDate', StringType(), True), 
                     StructField('WatchDate', StringType(), True),
                     StructField('CallFinalDisposition', StringType(), True),
                     StructField('AvailableDtTm', StringType(), True),
                     StructField('Address', StringType(), True), 
                     StructField('City', StringType(), True), 
                     StructField('Zipcode', IntegerType(), True), 
                     StructField('Battalion', StringType(), True), 
                     StructField('StationArea', StringType(), True), 
                     StructField('Box', StringType(), True), 
                     StructField('OriginalPriority', StringType(), True), 
                     StructField('Priority', StringType(), True), 
                     StructField('FinalPriority', IntegerType(), True), 
                     StructField('ALSUnit', BooleanType(), True), 
                     StructField('CallTypeGroup', StringType(), True),
                     StructField('NumAlarms', IntegerType(), True),
                     StructField('UnitType', StringType(), True),
                     StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                     StructField('FirePreventionDistrict', StringType(), True),
                     StructField('SupervisorDistrict', StringType(), True),
                     StructField('Neighborhood', StringType(), True),
                     StructField('Location', StringType(), True),
                     StructField('RowID', StringType(), True),
                     StructField('Delay', FloatType(), True)])



spark = (SparkSession
         .builder
         .appName("Example-3_6")
         .getOrCreate())
     
sf_fire_file = "C:/Practica_Bosonit/Capitulo_3/sf-fire-calls.csv"

fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)
     
few_fire_df = (fire_df
               .select("IncidentNumber", "AvailableDtTm", "CallType")
               .where(col("CallType") != "Medical Incident"))

#few_fire_df.show(5, truncate=False)
     
#####################
###Numero de  distintos tipos de CallType


ejercicio2 = (fire_df
                 .select("CallType")
                 .where(col("CallType").isNotNull())
                 .agg(countDistinct("CallType").alias('DistinctCallTypes')))
#ejercicio2.show()

##################
##Muestra los diferentes tipos de CallType
ejercicio3 = (fire_df
                 .select("CallType")
                 .where(col("CallType").isNotNull())
                 .distinct())
#ejercicio3.show()

#################
### Cambiamos nombre de la columna DELAY

new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")

(new_fire_df
 .select("ResponseDelayedinMins")
 .where(col("ResponseDelayedinMins") > 5))


#########################
###

fire_ts_df = (new_fire_df
        .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
        .drop("CallDate")
        .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
        .drop("WatchDate")
        .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),
                                                  "MM/dd/yyyy hh:mm:ss a"))
        .drop("AvailableDtTm"))


# Select the converted columns
"""
(fire_ts_df
     .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
     .show(5, False))
"""

##########################
###
"""
(fire_ts_df
     .select(year('IncidentDate'))
     .distinct()
     .orderBy(year('IncidentDate'))
     .show())

"""
####################
####
"""
(fire_ts_df
      .select("Calltype")
      .where(col("CallType").isNotNull())
      .groupBy("CallType")
      .count()
      .orderBy("count", ascending= False)
      .show(n=10, truncate=False)
 )

"""

##################
####

"""
(fire_ts_df
     .select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"), F.min("ResponseDelayedinMins"),F.max("ResponseDelayedinMins"))
     .show())
"""





######Ejercicios Pagina 91

########### 1.

####ESTE ME DEVUELVE UNA TABLA CON 
#LA PRIMERA COLUMNA ES DIFRENTES CALLTYPE Y SEGUNDA COLUMNA EL TOTAL DE ESE AÑO 2018
"""
(fire_ts_df
     .select("CallType")
     .where( year('IncidentDate') == 2018)
     .groupBy("CallType")
     .count()
     .show(n=10, truncate=False))



ejercicio2 = (fire_df
                 .select("CallType")
                 .where(col("CallType").isNotNull())
                 .agg(countDistinct("CallType").alias('DistinctCallTypes')))
"""

#SOLUCION
"""
(fire_ts_df
      .select("CallType")
      .where(year("IncidentDate") == 2018)
      .agg(countDistinct("CallType").alias('DistinctCallTypes'))
      .show(n=10, truncate=False)
      )

"""


################### 2.
"""
(fire_ts_df
     .select("IncidentDate")
     .where(year("IncidentDate") == 2018)
     .groupBy(month("IncidentDate"))
     .count()
     .orderBy("count", ascending=False)
     .show(n=10, truncate=False))
"""

################## 3.

"""
(fire_ts_df
     .select("Neighborhood")
     .where(col("City") == "San Francisco")
     .where(year("IncidentDate") == 2018)
     .groupBy("Neighborhood")
     .count()
     .orderBy("count", ascending=False)
     .show(n=10, truncate=False))
"""

############### 4.
"""
(fire_ts_df
     .select("ResponseDelayedinMins","Neighborhood")
     .where(year("IncidentDate") == 2018)
     .orderBy("ResponseDelayedinMins", ascending= False)
     .show(n=10, truncate=False))
"""



################ 5.

"""
(fire_ts_df
     .select("IncidentDate")
     .where(year("IncidentDate") == 2018)
     .groupBy(week("IncidentDate"))
     .count()
     .orderBy("count", ascending= False)
     .show(n=10, truncate=False))

"""


############# 6.


(fire_ts_df
     .select("Neighborhood")
     .count())


(fire_ts_df
     .select("Zipcode")
     .count())



############### 












