# Databricks notebook source
# https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

# COMMAND ----------

dbutils.widgets.text("Storage Location", "")
dbutils.widgets.text("Database Name", "NYTaxi")

# COMMAND ----------

StorageLocation = dbutils.widgets.get("Storage Location")
DBName = dbutils.widgets.get("Database Name")

# COMMAND ----------

# DBTITLE 1,Import libraries and set configurations
# from datetime import datetime
# from dateutil import parser
# from dateutil.relativedelta import relativedelta

from pyspark.sql.functions import *
from pyspark.sql.types import *


basePath = StorageLocation
weatherStationsTableLocation = basePath + "/datastore/weatherStations" 
dailyWeatherTableLocation = basePath + "/datastore/dailyWeather" 
lgaWeatherTableLocation = basePath + "/datastore/lgaDailyWeather"
dailyFeaturesTableLocation = basePath + "/datastore/dailyFeatures"
PaymentTypeTableLocation = basePath + "/datastore/PaymentType"
RateCodeTableLocation = basePath + "/datastore/RateCode"
ZonesTableLocation = basePath + "/datastore/Zones"
ZonesCSVTableLocation = basePath + "/datastore/ZonesCSV"
DateTableLocation = basePath + "/datastore/date"
VendorTableLocation = basePath + "/datastore/vendor"

rawGreenTripsTableLocation = basePath + "/datastore/rawGreenTrips"
dailyGreenTripsTableLocation = basePath + "/datastore/dailyGreenTrips"
rawYellowTripsTableLocation = basePath + "/datastore/rawYellowTrips"
dailyYellowTripsTableLocation = basePath + "/datastore/dailyYellowTrips"
rawtripsTableLocation = basePath + "/datastore/rawTrips"
dailytripsTableLocation = basePath + "/datastore/dailyTrips"

spark.conf.set("c.weatherStationsDataPath", "dbfs:" + weatherStationsTableLocation)
spark.conf.set("c.lgaWeatherDataPath", "dbfs:" + lgaWeatherTableLocation)
spark.conf.set("c.PaymentTypeDataPath", "dbfs:" + PaymentTypeTableLocation)
spark.conf.set("c.RateCodeDataPath", "dbfs:" + RateCodeTableLocation)
spark.conf.set("c.ZonesDataPath", "dbfs:" + ZonesTableLocation)
spark.conf.set("c.DateDataPath", "dbfs:" + DateTableLocation)
spark.conf.set("c.VendorDataPath", "dbfs:" + VendorTableLocation)

spark.conf.set("c.dailyGreenTripsDataPath", "dbfs:" + dailyGreenTripsTableLocation)
spark.conf.set("c.rawGreenTripsDataPath", "dbfs:" + rawGreenTripsTableLocation)
spark.conf.set("c.dailyYellowTripsDataPath", "dbfs:" + dailyYellowTripsTableLocation)
spark.conf.set("c.rawYellowTripsDataPath", "dbfs:" + rawYellowTripsTableLocation)
spark.conf.set("c.rawtripsDataPath", "dbfs:" + rawtripsTableLocation)
spark.conf.set("c.dailytripsDataPath", "dbfs:" + dailytripsTableLocation)


# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS {0}".format(DBName))

# COMMAND ----------

spark.sql("USE {0}".format(DBName))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Taxi Data

# COMMAND ----------

# DBTITLE 1,Load taxi data from the Databricks datasets
# MAGIC %fs ls dbfs:/databricks-datasets/nyctaxi/

# COMMAND ----------

# MAGIC %md
# MAGIC Taxi Reference Data

# COMMAND ----------

df_PT = spark.read.option("header", True)\
  .option("inferSchema",True)\
  .option("ignoreTrailingWhitespace", True)\
  .csv("dbfs:/databricks-datasets/nyctaxi/taxizone/taxi_payment_type.csv")\
  .write.format("delta").mode("overwrite").save(PaymentTypeTableLocation)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS PaymentType;
# MAGIC 
# MAGIC CREATE TABLE PaymentType
# MAGIC USING DELTA 
# MAGIC LOCATION '${c.PaymentTypeDataPath}';
# MAGIC 
# MAGIC SHOW TABLES;

# COMMAND ----------

df_RC = spark.read.option("header", True)\
  .option("inferSchema",True)\
  .option("ignoreTrailingWhitespace", True)\
  .csv("dbfs:/databricks-datasets/nyctaxi/taxizone/taxi_rate_code.csv")\
  .write.format("delta").mode("overwrite").save(RateCodeTableLocation)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS RateCode;
# MAGIC 
# MAGIC CREATE TABLE RateCode
# MAGIC USING DELTA 
# MAGIC LOCATION '${c.RateCodeDataPath}';
# MAGIC 
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sh
# MAGIC # Pull CSV file from url
# MAGIC wget -nc https://raw.githubusercontent.com/furlong46/Images/main/NYTaxiZoneswithLatLong.csv

# COMMAND ----------

# MAGIC %sh
# MAGIC ls

# COMMAND ----------

dbutils.fs.cp("file:/databricks/driver/NYTaxiZoneswithLatLong.csv", ZonesCSVTableLocation)

# COMMAND ----------

df_Z = spark.read.option("header", True)\
  .option("inferSchema",True)\
  .option("ignoreTrailingWhitespace", True)\
  .csv(ZonesCSVTableLocation)\
  .write.format("delta").mode("overwrite").save(ZonesTableLocation)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS taxi_zones;
# MAGIC 
# MAGIC CREATE TABLE taxi_zones
# MAGIC USING DELTA 
# MAGIC LOCATION '${c.ZonesDataPath}';
# MAGIC 
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE VIEW date_vw 
# MAGIC AS 
# MAGIC WITH calendarDate AS (
# MAGIC   select
# MAGIC     explode(
# MAGIC       sequence(
# MAGIC         to_date('2000-01-01'),
# MAGIC         to_date('2019-12-31'),
# MAGIC         interval 1 day
# MAGIC       )
# MAGIC     ) AS calendarDate
# MAGIC )
# MAGIC --SELECT * FROM calendarDate
# MAGIC select
# MAGIC   year(calendarDate) * 10000 + month(calendarDate) * 100 + day(calendarDate) as dateInt,
# MAGIC   CalendarDate,
# MAGIC   year(calendarDate) AS CalendarYear,
# MAGIC   date_format(calendarDate, 'MMMM') as CalendarMonth,
# MAGIC   month(calendarDate) as MonthOfYear,
# MAGIC   date_format(calendarDate, 'EEEE') as CalendarDay,
# MAGIC   dayofweek(calendarDate) as DayOfWeek,
# MAGIC   weekday(calendarDate) + 1 as DayOfWeekStartMonday,
# MAGIC   case
# MAGIC     when weekday(calendarDate) < 5 then 'Y'
# MAGIC     else 'N'
# MAGIC   end as IsWeekDay,
# MAGIC   dayofmonth(calendarDate) as DayOfMonth,
# MAGIC   case
# MAGIC     when calendarDate = last_day(calendarDate) then 'Y'
# MAGIC     else 'N'
# MAGIC   end as IsLastDayOfMonth,
# MAGIC   dayofyear(calendarDate) as DayOfYear,
# MAGIC   weekofyear(calendarDate) as WeekOfYearIso,
# MAGIC   quarter(calendarDate) as QuarterOfYear
# MAGIC --   ,
# MAGIC   /* Use fiscal periods needed by organization fiscal calendar */
# MAGIC --   case
# MAGIC --     when month(calendarDate) >= 10 then year(calendarDate) + 1
# MAGIC --     else year(calendarDate)
# MAGIC --   end as FiscalYearOctToSep,
# MAGIC --   (month(calendarDate) + 2) % 12 + 1 as FiscalMonthOctToSep,
# MAGIC --   case
# MAGIC --     when month(calendarDate) >= 7 then year(calendarDate) + 1
# MAGIC --     else year(calendarDate)
# MAGIC --   end as FiscalYearJulToJun,
# MAGIC --   (month(calendarDate) + 5) % 12 + 1 as FiscalMonthJulToJun
# MAGIC from
# MAGIC   calendarDate
# MAGIC order by
# MAGIC   calendarDate

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE dimdate
# MAGIC USING DELTA
# MAGIC LOCATION '${c.DateDataPath}'
# MAGIC AS
# MAGIC SELECT * FROM date_vw

# COMMAND ----------

# MAGIC %md
# MAGIC ## Taxi trips

# COMMAND ----------

# DBTITLE 1,All green taxi historical data
path = ["dbfs:/databricks-datasets/nyctaxi/tripdata/green/green_tripdata_2016-0[^123456].csv.gz",
        "dbfs:/databricks-datasets/nyctaxi/tripdata/green/green_tripdata_2016-[12]*.csv.gz",
        "dbfs:/databricks-datasets/nyctaxi/tripdata/green/green_tripdata_201[789]-*.csv.gz"]

dfTripsGreen = spark.read.option("header", True)\
  .option("inferSchema",True)\
  .option("ignoreTrailingWhitespace", True)\
  .csv(path)

dfTripsGreen.count()

# COMMAND ----------

# DBTITLE 1,Aggregate taxi trip data by day
taxi_zones = spark.table("taxi_zones")
dfTrips = dfTripsGreen.withColumn("pickupDate", to_date(dfTripsGreen.lpep_pickup_datetime)) \
  .join(taxi_zones, dfTripsGreen.DOLocationID == taxi_zones.LocationID, "left_outer" )
  
dfTrips = dfTrips.where(dfTrips.pickupDate.between("2016-07-01", "2019-12-31"))

dfDailyTrips = dfTrips.groupBy(dfTrips.pickupDate, dfTrips.Borough)\
  .agg(sum(dfTrips.trip_distance).alias("sumDistance"), sum(dfTrips.total_amount).alias("sumTotalFare"), count(dfTrips.total_amount).alias("tripCount"))\
  .orderBy(dfTrips.pickupDate)

dfDailyTrips.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(dailyGreenTripsTableLocation)

display(dfDailyTrips)

# COMMAND ----------

# DBTITLE 1,Add daily trips table to view the data in Databricks SQL
# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS dailyGreenTrips;
# MAGIC 
# MAGIC CREATE TABLE dailyGreenTrips
# MAGIC USING DELTA 
# MAGIC LOCATION '${c.dailyGreenTripsDataPath}';
# MAGIC 
# MAGIC SHOW TABLES;

# COMMAND ----------

dfTrips = dfTripsGreen.withColumn("pickupDate", to_date(dfTripsGreen.lpep_pickup_datetime))
  
dfTrips = dfTrips.where(dfTrips.pickupDate.between("2016-07-01", "2019-12-31"))

dfTrips.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(rawGreenTripsTableLocation)

display(dfTrips)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS rawGreenTrips;
# MAGIC 
# MAGIC CREATE TABLE rawGreenTrips
# MAGIC USING DELTA 
# MAGIC LOCATION '${c.rawGreenTripsDataPath}';
# MAGIC 
# MAGIC SHOW TABLES;

# COMMAND ----------

path = ["dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2016-0[^123456].csv.gz",
        "dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2016-[12]*.csv.gz",
        "dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_201[789]-*.csv.gz"]

dfTripsYellow = spark.read.option("header", True)\
  .option("inferSchema",True)\
  .option("ignoreTrailingWhitespace", True)\
  .csv(path)

dfTripsYellow.count()

# COMMAND ----------

taxi_zones = spark.table("taxi_zones")
dfTripsY = dfTripsYellow.withColumn("pickupDate", to_date(dfTripsYellow.tpep_pickup_datetime)) \
  .join(taxi_zones, dfTripsYellow.DOLocationID == taxi_zones.LocationID, "left_outer" )
  
dfTripsY = dfTripsY.where(dfTripsY.pickupDate.between("2016-07-01", "2019-12-31"))

dfDailyTripsYellow = dfTripsY.groupBy(dfTripsY.pickupDate, dfTripsY.Borough)\
  .agg(sum(dfTripsY.trip_distance).alias("sumDistance"), sum(dfTripsY.total_amount).alias("sumTotalFare"), count(dfTripsY.total_amount).alias("tripCount"))\
  .orderBy(dfTripsY.pickupDate)

dfDailyTripsYellow.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(dailyYellowTripsTableLocation)

display(dfDailyTripsYellow)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS dailyYellowTrips;
# MAGIC 
# MAGIC CREATE TABLE dailyYellowTrips
# MAGIC USING DELTA 
# MAGIC LOCATION '${c.dailyYellowTripsDataPath}';
# MAGIC 
# MAGIC SHOW TABLES;

# COMMAND ----------

dfTripsY = dfTripsYellow.withColumn("pickupDate", to_date(dfTripsYellow.tpep_pickup_datetime)) \
  
dfTripsY = dfTripsY.where(dfTripsY.pickupDate.between("2016-07-01", "2019-12-31"))

dfTripsY.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(rawYellowTripsTableLocation)

display(dfTripsY)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS rawYellowTrips;
# MAGIC 
# MAGIC CREATE TABLE rawYellowTrips
# MAGIC USING DELTA 
# MAGIC LOCATION '${c.rawYellowTripsDataPath}';
# MAGIC 
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE rawtrips
# MAGIC USING DELTA 
# MAGIC LOCATION '${c.rawtripsDataPath}'
# MAGIC AS
# MAGIC 
# MAGIC SELECT 
# MAGIC VendorID,
# MAGIC 'green' AS taxitype,
# MAGIC pickupDate,
# MAGIC hour(to_timestamp(lpep_pickup_datetime, "yyyy-MM-dd HH:mm:ss")) AS pickupHour,
# MAGIC -- lpep_pickup_datetime,
# MAGIC to_timestamp(lpep_pickup_datetime, "yyyy-MM-dd HH:mm:ss") AS pickup,
# MAGIC -- lpep_dropoff_datetime,
# MAGIC to_timestamp(lpep_dropoff_datetime, "yyyy-MM-dd HH:mm:ss") AS dropoff,
# MAGIC (cast(to_timestamp(lpep_dropoff_datetime, "yyyy-MM-dd HH:mm:ss") as long) 
# MAGIC - cast(to_timestamp(lpep_pickup_datetime, "yyyy-MM-dd HH:mm:ss") as long) ) / 60.0 AS trip_duration_minutes,
# MAGIC store_and_fwd_flag,
# MAGIC RatecodeID,
# MAGIC PULocationID,
# MAGIC DOLocationID,
# MAGIC trip_type,
# MAGIC payment_type,
# MAGIC CAST(passenger_count AS float) AS passenger_count,
# MAGIC CAST(trip_distance AS float) AS trip_distance,
# MAGIC CAST(fare_amount AS float) AS fare_amount,
# MAGIC CAST(extra AS float) AS extra,
# MAGIC CAST(mta_tax AS float) AS mta_tax,
# MAGIC CAST(tip_amount AS float) AS tip_amount,
# MAGIC CAST(tolls_amount AS float) AS tolls_amount,
# MAGIC CAST(ehail_fee AS float) AS ehail_fee,
# MAGIC CAST(total_amount AS float) AS total_amount
# MAGIC FROM rawgreentrips
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC SELECT 
# MAGIC VendorID,
# MAGIC 'yellow' AS taxitype,
# MAGIC pickupDate,
# MAGIC hour(to_timestamp(tpep_pickup_datetime, "yyyy-MM-dd HH:mm:ss")) AS pickupHour,
# MAGIC -- lpep_pickup_datetime,
# MAGIC to_timestamp(tpep_pickup_datetime, "yyyy-MM-dd HH:mm:ss") AS pickup,
# MAGIC -- lpep_dropoff_datetime,
# MAGIC to_timestamp(tpep_dropoff_datetime, "yyyy-MM-dd HH:mm:ss") AS dropoff,
# MAGIC (cast(to_timestamp(tpep_dropoff_datetime, "yyyy-MM-dd HH:mm:ss") as long) 
# MAGIC - cast(to_timestamp(tpep_pickup_datetime, "yyyy-MM-dd HH:mm:ss") as long) ) / 60.0 AS trip_duration_minutes,
# MAGIC store_and_fwd_flag,
# MAGIC RatecodeID,
# MAGIC PULocationID,
# MAGIC DOLocationID,
# MAGIC '' AS trip_type,
# MAGIC payment_type,
# MAGIC CAST(passenger_count AS float) AS passenger_count,
# MAGIC CAST(trip_distance AS float) AS trip_distance,
# MAGIC CAST(fare_amount AS float) AS fare_amount,
# MAGIC CAST(extra AS float) AS extra,
# MAGIC CAST(mta_tax AS float) AS mta_tax,
# MAGIC CAST(tip_amount AS float) AS tip_amount,
# MAGIC CAST(tolls_amount AS float) AS tolls_amount,
# MAGIC NULL AS ehail_fee,
# MAGIC CAST(total_amount AS float) AS total_amount
# MAGIC FROM rawyellowtrips

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS vendor;
# MAGIC 
# MAGIC CREATE TABLE vendor
# MAGIC USING DELTA 
# MAGIC LOCATION '${c.VendorDataPath}'
# MAGIC AS
# MAGIC 
# MAGIC SELECT DISTINCT 
# MAGIC VendorID
# MAGIC FROM rawtrips

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE dailytrips
# MAGIC USING DELTA 
# MAGIC LOCATION '${c.dailytripsDataPath}'
# MAGIC AS
# MAGIC 
# MAGIC SELECT
# MAGIC pickupDate,
# MAGIC 'green' AS taxitype,
# MAGIC Borough,
# MAGIC sumDistance,
# MAGIC sumTotalFare,
# MAGIC tripCount
# MAGIC FROM dailygreentrips
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC SELECT
# MAGIC pickupDate,
# MAGIC 'yellow' AS taxitype,
# MAGIC Borough,
# MAGIC sumDistance,
# MAGIC sumTotalFare,
# MAGIC tripCount
# MAGIC FROM dailyyellowtrips

# COMMAND ----------

# MAGIC %md
# MAGIC Optimize Tables for Performance

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE dailytrips;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE rawtrips;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE dimdate;
# MAGIC OPTIMIZE taxi_zones;
# MAGIC OPTIMIZE vendor;
# MAGIC OPTIMIZE paymenttype;
# MAGIC OPTIMIZE ratecode;

# COMMAND ----------

# MAGIC %sql
# MAGIC ANALYZE TABLE dailytrips COMPUTE STATISTICS;
# MAGIC ANALYZE TABLE rawtrips COMPUTE STATISTICS;
# MAGIC ANALYZE TABLE dimdate COMPUTE STATISTICS;
# MAGIC ANALYZE TABLE taxi_zones COMPUTE STATISTICS;
# MAGIC ANALYZE TABLE vendor COMPUTE STATISTICS;
# MAGIC ANALYZE TABLE paymenttype COMPUTE STATISTICS;
# MAGIC ANALYZE TABLE ratecode COMPUTE STATISTICS;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean Up Database and Storage

# COMMAND ----------

# spark.sql("DROP DATABASE {0} CASCADE".format(DBName))

# COMMAND ----------

# dbutils.fs.rm(StorageLocation, recurse=True)
