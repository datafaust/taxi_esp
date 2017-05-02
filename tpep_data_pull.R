

#pull data by date, hour, zone, and count of trips------
library(RODBC)
library(data.table)
TPEP2 = odbcConnect('TPEP2')
yellow_raw = sqlQuery(TPEP2, paste("SELECT
                      convert(char(10), tpep_pickup_datetime, 121) as date,
                      DATEPART(HOUR, tpep_pickup_datetime) as hour,
                      PULocationID as puloc,
                      count(tpep_pickup_datetime) as trips_med
                      FROM
                      TPEP2_Triprecord
                      WHERE
                      tpep_pickup_datetime >= '2015-01-01' and tpep_pickup_datetime < '2017-01-01'
                      GROUP BY
                      convert(char(10), tpep_pickup_datetime, 121),
                      DATEPART(HOUR, tpep_pickup_datetime),
                      PULocationID 
                      "), as.is = T)



shl_raw =  sqlQuery(TPEP2, paste(
                                "
                                 SELECT
                                 convert(char(10), lpep_pickup_datetime, 121) as date,
                                 DATEPART(HOUR, lpep_pickup_datetime) as hour,
                                 PULocationID as puloc,
                                 count(lpep_pickup_datetime) as trips_shl
                                 FROM
                                 LPEP2_Triprecord
                                 WHERE
                                 lpep_pickup_datetime >= '2015-01-01' and lpep_pickup_datetime < '2017-01-01'
                                 GROUP BY
                                 convert(char(10), lpep_pickup_datetime, 121),
                                 DATEPART(HOUR, lpep_pickup_datetime),
                                 PULocationID
                                 "), as.is = T)

#pull zone data and merge----
tz_key = setDT(sqlFetch(TPEP2,"TPEP2_LOCATION_Lookup"))[,zone_from:=as.character(Zone)]
yellow_raw = setDT(merge(yellow_raw, tz_key, by.x = "puloc", by.y = "LocationID", all.x = T))[,.(date, hour, puloc, trips_med, zone_from)]
shl_raw = setDT(merge(shl_raw, tz_key, by.x = "puloc", by.y = "LocationID", all.x = T))[,.(date, hour, puloc, trips_shl, zone_from)]


#write out-------
setwd("C:\\Users\\lopezf\\Desktop\\regression_test\\regression_test")
write.csv(yellow_raw, "hourly_med_trips_loc.csv")
write.csv(shl_raw, "hourly_shl_trips_loc.csv")