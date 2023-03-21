from pyspark.sql import SparkSession 
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F



csv_file = "C:/Practica_Bosonit/Capitulo_4/departuredelays.csv"


schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"



spark = (SparkSession
         .builder
         .appName("SparkSQLExampleApp")
         .getOrCreate())




df = (spark.read.format("csv")
 .option("inferSchema", "true")
 .option("header", "true")
 .load(csv_file))

df.createOrReplaceTempView("us_delay_flights_tbl")


"""
####Ejercicio 1

(df.select("distance", "origin", "destination")
     .where(col("distance") > 1000)
     .orderBy(desc("distance"))).show(10)


####Ejercicio 2

(df.select("date", "delay", "origin", "destination")
     .where(col("delay") > 120)
     .where(col("origin") == "SFO")
     .where(col("destination") == "ORD")
     .orderBy(desc("delay"))).show(10)



#####Ejercicio 3

(df.select("delay", "origin", "destination")
     .withColumn("Flight_Delays", when(col("delay") >360, "Very Long Delays")
                                 .when(col("delay") > 120 & col("delay") < 360, "Long Delays")
                                 .when(col("delay") > 60 & col("delay") < 120, "Short Delays")
                                 .when(col("delay") > 0 & col("delay") < 60, "Tolerable Delays")
                                 .when(col("delay") ==0, "No Delays")
                                 .otherwise("Early"))
     .orderBy(desc("origin"))).show(10)


"""

####Crear base de datos

# In Python
# Path to our US flight delays CSV file 
# Schema as defined in the preceding example
flights_df = spark.read.csv(csv_file, schema=schema)
flights_df.write.saveAsTable("us_delay_flights_tbl")


us_flights_df = spark.sql("SELECT * FROM us_delay_flights_tbl")
us_flights_df2 = spark.table("us_delay_flights_tbl")









