# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession 
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F

from pyspark.sql.types import LongType

import pandas as pd

tripdelaysFilePath ="C:/Practica_Bosonit/Capitulo_5/departuredelays.csv"

airportsnaFilePath ="C:/Practica_Bosonit/Capitulo_5/airport-codes-na.txt"


if __name__ == "__main__":
 # Create a SparkSession
    spark = (SparkSession
              .builder
              .appName("Example-3_6")
              .getOrCreate())
    
    airportsna = (spark.read
                  .format("csv")
                  .options(header="true", inferSchema="true", sep="\t")
                  .load(airportsnaFilePath))
    
    airportsna.createOrReplaceTempView("airports_na")
    
    
    departureDelays = (spark.read
                       .format("csv")
                       .options(header="true")
                       .load(tripdelaysFilePath))
    
    departureDelays = (departureDelays
                       .withColumn("delay", expr("CAST(delay as INT) as delay"))
                       .withColumn("distance", expr("CAST(distance as INT) as distance")))
    
    departureDelays.createOrReplaceTempView("departureDelays")
    
    
    # Create temporary small table
    foo = (departureDelays
           .filter(expr("""origin == 'SEA' and destination == 'SFO' and 
                        date like '01010%' and delay > 0""")))
                        
    foo.createOrReplaceTempView("foo")
    
    
    #spark.sql("SELECT * FROM airports_na LIMIT 10").show()
    
    #spark.sql("SELECT * FROM departureDelays LIMIT 10").show()
    
    
    #spark.sql("SELECT * FROM foo").show()
    
    
    bar = departureDelays.union(foo)
    bar.createOrReplaceTempView("bar")
    # Show the union (filtering for SEA and SFO in a specific time range)
    #bar.filter(expr("""origin == 'SEA' AND destination == 'SFO'
                    #AND date LIKE '01010%' AND delay > 0""")).show()

    #Join departure delays data (foo) with airport info
    
    foo.join(
        airportsna,
        airportsna.IATA == foo.origin
    ).select("City", "State", "date", "delay", "distance", "destination").show()
    
    
    foo.show()
    
    foo2 = (foo.withColumn(
            "status",
            expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END")
            ))
    
    foo2.show()
    
    foo3 = foo2.drop("delay")
    foo3.show()
    
    foo4 = foo3.withColumnRenamed("status", "flight_status")
    foo4.show()




