#core script for creating dataset----------------------
library(latticeExtra)
library(data.table)
library(fasttime)
library(ggmap)
#library(scizor)
library(RODBC)
library(zoo)
library(stringr)
library(etl)
library(citibike)
#library(weatherData)
library(pbapply)
library(DMwR)
#library(rwunderground)
library(lubridate)
library(tidyr)
TPEP2 = odbcConnect('TPEP2')
options(scipen=999)

cabinets = list(foursquare = "I:/COF/COF/_M3trics/external_useful_data/foursquare_data_2014-2016",
                home = "C:/Users/lopezf/Desktop/regression_test/regression_test",
                gas_scrape= "C:/Users/lopezf/Desktop/regression_test/regression_test/gas_data",
                citibike = "C:/Users/lopezf/Desktop/regression_test/regression_test/citibike/citibike_geocoded",
                mta = "C:/Users/lopezf/Desktop/regression_test/regression_test/mta_turnstile_data_geocoded_tz",
                git_home = "C:/Users/lopezf/Documents/R/R-3.3.1/library/taxi_esp"
) 



setwd(cabinets$gas_scrape)

x = "C:\\Users\\lopezf\\Desktop\\regression_test\\regression_test\\gas_data"
files = list.files(path = x,pattern = ".csv")
temp = lapply(files, fread, sep=",")
gas_data = rbindlist( temp, fill = T)
gas_data = as.data.frame(gas_data)

#extract date
gas_data$date = as.Date(gas_data$time_called)

#transform variables
gas_data$price = as.numeric(gas_data$price)

#aggregate to daily averages of gas
gas_date = aggregate(price ~ date, gas_data, FUN = mean)

#read gas from FRED
gas_date_harbour = fread("http://www.quandl.com/api/v1/datasets/FRED/DGASNYH.csv")
gas_date_harbour$DATE = as.Date(gas_date_harbour$DATE)


#Quality of Life--------------------------------------------------------------------------------------------------

setwd(cabinets$home)

#sp500 data by day
sp500 = read.csv("SP500.csv", header=T)

sp500$DATE = as.Date(sp500$DATE)
sp500$VALUE[sp500$VALUE == "."] = NA
sp500$VALUE = as.numeric(as.character(sp500$VALUE))

#cpi food and beverages
cpifbev = read.csv("cpi_food_bev.csv", header = T)
cpifbev$DATE = as.Date(cpifbev$DATE)



#weather data by day------------------------------------------------------------------------------------------------------
weather = fread("https://www.wunderground.com/history/airport/KNYC/2015/1/1/CustomHistory.html?dayend=20&monthend=11&yearend=2016&req_city=&req_state=&req_statename=&reqdb.zip=&reqdb.magic=&reqdb.wmo=&format=1")
weather2 = fread("https://www.wunderground.com/history/station/87938/2016/2/2/CustomHistory.html?dayend=16&monthend=12&yearend=2016&req_city=&req_state=&req_statename=&reqdb.zip=&reqdb.magic=&reqdb.wmo=&format=1")

names(weather2) = names(weather)

weather = rbind(weather, weather2)
weather = weather[nchar(weather$EST) > 5,]


#split dates, edit and reprep
weather = setDT(data.frame(weather, do.call(rbind, str_split(weather$EST, '-'))))
weather[,X2:= as.numeric(as.character(X2))]
weather[,X3:= as.numeric(as.character(X3))]
weather[,X2:= ifelse(X2 < 10, paste0("0",X2), X2)]
weather[,X3:= ifelse(X3 < 10, paste0("0",X3), X3)]
weather[,EST:= as.Date(paste0(X1, "-" ,X2,"-" ,X3), "%Y-%m-%d")]


#variables we want
weather = weather[, c("EST", "Mean.TemperatureF", "Mean.Wind.SpeedMPH", "PrecipitationIn")][
  ,Mean.TemperatureF:=as.numeric(Mean.TemperatureF)][
    ,Mean.Wind.SpeedMPH:=as.numeric(Mean.Wind.SpeedMPH)][
      ,PrecipitationIn:=as.numeric(PrecipitationIn)]
weather = weather[!duplicated(weather$EST),]


#active drivers list medallions--------------------------------------------------------------------------------------------

#vehicle_list = fread("https://data.cityofnewyork.us/api/views/rhe8-mgbb/rows.csv?accessType=DOWNLOAD")

#vehicle_list = RJSONIO::fromJSON("https://data.cityofnewyork.us/resource/7drc-shp9.json?$query=SELECT last_updated_date")
featurez = c("Name" ,"Last Date Updated","Last Time Updated")
vehicle_list = fread("med_vehicles.csv", select = featurez)


vehicle_list$`Last Date Updated` = format(as.Date(vehicle_list$`Last Date Updated`, "%m/%d/%Y"), "%Y-%m-%d")
vehicle_list$`Last Time Updated` = paste0(vehicle_list$`Last Time Updated`, ":00")
vehicle_list$time_stamp = as.POSIXct(paste(vehicle_list$`Last Date Updated`, vehicle_list$`Last Time Updated`), format="%Y-%m-%d %H:%M:%S")
daily_vehicles  = vehicle_list[,.N, by=list(vehicle_list$`Last Date Updated`, vehicle_list$time_stamp)]
daily_vehicles = daily_vehicles[,sum(N), by = "vehicle_list"]

#daily_vehicles = daily_vehicles[!duplicated(daily_vehicles$time_stamp),]

titlez = c("date", "authorized_vehicles")
names(daily_vehicles) = titlez
daily_vehicles$date = as.Date(daily_vehicles$date)
daily_vehicles$authorized_vehicles[daily_vehicles$authorized_vehicles > 15000] = NA




#citibike data by hour by taxi zone-------------------------------------------------------------------------------------------------
#pull is depracated

# bikes = etl("citibike", dir = "C:/Users/lopezf/Desktop/regression_test/regression_test/citibike")
# class(bikes)
# is.etl(bikes)
# bikes

#pull data and create database
#bikes %>%
# etl_extract(bikes, years = 2015, months = 1:12) %>%
#etl_transform(bikes, years = 2015, months = 1:12)  %>%
#etl_load(bikes, years = 2015, months = 1:12)

#sum trips hourly by region 
setwd(cabinets$citibike)

bike_trips = rbindlist(
  pblapply(list.files(), function(x) {
    featurez = c("starttime", "OBJECTID")
    bike_trips = fread(x, select = featurez)
    bike_trips = setDT(data.frame(bike_trips, do.call(rbind, str_split(bike_trips$starttime, ' '))))
    bike_trips = setDT(data.frame(bike_trips, do.call(rbind, str_split(bike_trips$X2, ':'))))
    daily_trips = aggregate(starttime ~ X1 + X1.1 + OBJECTID, bike_trips, FUN =length)
    rm(bike_trips)
    gc()
    return(assign(x, daily_trips))
  })
)

names(bike_trips) = c("date", "hour","taxi_zone","bike_trips")
bike_trips[,date:= format(as.Date(date, "%m/%d/%Y"), "%Y-%m-%d")]

#paste0 into bike data if less than 10
bike_trips[,hour:= as.numeric(as.character(hour))][,hour:= ifelse(hour < 10, paste0("0",hour),hour)][
    ,hour:= paste0(hour, ":00")]
bike_trips[,date_zone_id:= paste0(date,"_",hour,"_",taxi_zone)]
bike_trips[,date_zone_id:= gsub("[^0-9]", "",date_zone_id)]


#mta subway data by hour and region------------------------------------------------------------------------------------------------------

#pull longlat data 
#longlat_mta = fread("http://web.mta.info/developers/data/nyct/subway/StationEntrances.csv")


#build list to push to links for download
#daters = data.frame(seq.Date(as.Date("2015-01-03"), as.Date("2016-12-17"), 7))

# food = rbind(
#   lapply(daters$seq.Date.as.Date..2015.01.03....as.Date..2016.12.17....7., function(x) {
#     q = gsub("[^0-9]","",x)
#     q = substring(q, 3, nchar(q))
#     return(q)
#   })
# )

#download files # done downloading
# for (i in food) {
#   print(i)
#   setwd("C:/Users/lopezf/Desktop/regression_test/regression_test/mta_turnstile_data")
#   q = paste0("http://web.mta.info/developers/data/nyct/turnstile/turnstile_", i, ".txt")
#   print(q)
#   download.file(q, paste0("mta_turnstile",i, ".csv"))
#   gc()
# }

setwd(cabinets$mta)

#loop through files and aggregate
subway_trips = rbindlist(
  pblapply(list.files(), function(x) {
    options(scipen=999)
    featurez = c("ENTRIES", "DATE", "TIME", "OBJECTID")
    test = fread(x, select = featurez)
    #print(test)
    subway_trips = test[,.(sum_entries=sum(ENTRIES)), by = .(DATE,TIME,OBJECTID)] 
    subway_trips$DATE = format(as.Date(subway_trips$DATE, "%m/%d/%Y"), "%Y-%m-%d")
    subway_trips$hour = substr(subway_trips$TIME, 1,2)
    subway_trips = subway_trips[,.(sum_entries=sum(sum_entries)), by = .(DATE,hour,OBJECTID)]
    subway_trips$hour = paste0(subway_trips$hour, ":00")
    return(assign(x,subway_trips))
    rm(subway_trips)
    gc()
  })
)

#sort
subway_trips = subway_trips[order(OBJECTID, DATE, hour),]
subway_trips[,date_zone_id:= paste0(DATE, "_",
                                   hour, "_",
                                   OBJECTID)][
                                     ,date_zone_id:= gsub("[^0-9]", "", date_zone_id)][
                                       ,timestamp:= paste(DATE, hour)]

#source zone values------------------------------------------------------------------------
setwd(cabinets$git_home)
source("tz_centroid_builder.R")

#foursquare commercial strenght---------------------------------------------------------------------------------------------------------------------
setwd(cabinets$foursquare)

featurez = c("OBJECTID", "big_cat", "year", "locationid", "locationname")
fs = fread("geocoded_taxizones_master_fs.csv",  
           select = featurez)
fs_data = fs[ ,.N , by = .(year,OBJECTID, big_cat)][,year_id:= paste0(year,OBJECTID)]

#pivot data to merge on year
fs_data = spread(fs_data, big_cat, N)
names(fs_data) = c("year", "OBJECTID", "year_id", "arts_entertainment",
                   "college_university", "event", "food", "nightlife",
                   "outdoors_rec", "professional", "residence", "shop_service", "travel")
rm(fs)
gc()




#bring trips by day, hour, taxi zone-----------------------------------------------------------------------------------------------------
setwd(cabinets$home)

#create base file that has every taxi location and every day and every hour
base_hours = seq(
  from=as.POSIXct("2015-1-1 0:00", tz="America/New_York"),
  to=as.POSIXct("2016-11-11 23:00", tz="America/New_York"),
  by="hour"
) 

taxi_zones = sqlFetch(TPEP2, "TPEP2_LOCATION_lookup")
loc = as.character(taxi_zones$LocationID)
master_hour = setDT(merge(loc, base_hours))
colnames(master_hour) = c("zone","timestamp")
master_hour[,date_zone_id:= paste0(substr(timestamp,1,16))][
  ,date_zone_id:= paste0(gsub(" ", "_", date_zone_id),"_", zone)][
    ,date_zone_id:= gsub("[^0-9]", "", date_zone_id)]
master_hour = master_hour[order(timestamp),]
gc()

#merge zone long lat onto master hour (inner join gets rid of numbers that aren't zones)
master_hour = merge(master_hour, centroids[,c("plong", "plat", "zone","boro")],
                    by = "zone")
master_hour[,.N, by = .(is.na(plat))] #tests NA values should be only FALSE


featurez = c("puloc", "trips_med","date" ,"hour", "zone_from")
hourly_med_trips_loc = fread("hourly_med_trips_loc.csv", select = featurez)
hourly_shl_trips_loc = fread("hourly_shl_trips_loc.csv", select = featurez)
hourly_med_trips_loc[,date:= as.Date(date)]
hourly_shl_trips_loc[,date:= as.Date(date)]
hourly_med_trips_loc[,hour:= paste0(hour, ":00")]
hourly_shl_trips_loc[,hour:= paste0(hour, ":00")]

hourly_med_trips_loc[,date_zone_id:= paste0(date, "_",
                                           hour,"_",
                                           puloc)]

hourly_med_trips_loc[,date_zone_id:= gsub("[^0-9]", "", date_zone_id)]

colnames(hourly_shl_trips_loc)[colnames(hourly_shl_trips_loc) == "trips_med"] = "trips_shl"

hourly_shl_trips_loc[,date_zone_id:= paste0(date, "_",
                                           hour,"_",
                                           puloc)]
hourly_shl_trips_loc[,date_zone_id:= gsub("[^0-9]", "", date_zone_id)]

#merge all dat-----------------------------------------------------------------------

#merge daily variables
master_day = setDT(merge(weather,gas_date_harbour, by.x = "EST", by.y = "DATE",all.x = T))[
  ,VALUE:= na.locf(DGASNYH,fromLast = T)][,c(1,2,3,4,6)]
names(master_day) = c("date", "mean_temp", "mean_wind_speed", "precipitation", "gas_prices")

#merge hourly variables on master tpep trip set
master_hour = merge(master_hour, hourly_med_trips_loc, by = "date_zone_id", all.x = T)

master_hour = merge(master_hour, hourly_shl_trips_loc[, c("date_zone_id","trips_shl")], 
                    by = 'date_zone_id', all.x = T)
master_hour = merge(master_hour, bike_trips[,c("date_zone_id","bike_trips")],by = "date_zone_id", all.x = T)
master_hour = master_hour[order(date
                                ,zone_from
                                ,hour),]
#master_hour = merge(master_hour, subway_trips[,c("date_zone_id","sum_entries")], by = "date_zone_id", all.x =T)

#merge fs data
master_hour$year_id = paste0(year(master_hour$timestamp),master_hour$zone)
master_hour = merge(master_hour, fs_data, by = "year_id", all.x =T)
master_hour$date = as.Date(master_hour$timestamp)

#merge weather and gas to the master set
master_hour = merge(master_hour, master_day, by = "date", all.x = T)

gc()
#master_hour$timestamp = paste(master_hour$date, master_hour$hour)

master_hour = master_hour[order(master_hour$zone, timestamp),]
summary(master_hour)

#treat missing values in data--------------------------------------------------------------
#we assume taxi data as pure so NA's for trips = 0
master_hour[,trips_shl:= ifelse(is.na(trips_shl), 0, trips_shl)]
master_hour[,trips_med:= ifelse(is.na(trips_med), 0, trips_med)]
master_hour[,bike_trips:= ifelse(is.na(bike_trips),0,bike_trips)]

#spatial and fs recode to 0 and objectid
master_hour[,OBJECTID:= ifelse(is.na(OBJECTID), zone, OBJECTID)]
master_hour[,arts_entertainment:= ifelse(is.na(arts_entertainment), 0, arts_entertainment)]
master_hour[,college_university := ifelse(is.na(college_university), 0 , college_university)]
master_hour[,event:= ifelse(is.na(event), 0, event)]
master_hour[,food := ifelse(is.na(food), 0, food)]
master_hour[,nightlife:= ifelse(is.na(nightlife), 0, nightlife)]
master_hour[,outdoors_rec := ifelse(is.na(outdoors_rec), 0, outdoors_rec)]
master_hour[,professional := ifelse(is.na(professional), 0, professional)]
master_hour[,residence := ifelse(is.na(residence), 0, residence)]
master_hour[,shop_service := ifelse(is.na(shop_service),0, shop_service)]
master_hour[,travel := ifelse(is.na(travel), 0 , travel)]

#total_trips, weekday and zone as factor
master_hour[,total_trips := trips_shl + trips_med]
master_hour[,weekday.f := as.factor(weekdays(timestamp))]
master_hour[,zone.f := as.factor(zone)]
master_hour[,hour.f :=as.factor(hour(timestamp))]


#extract final data set-------------------------------------------------------------
master_write = master_hour[, c("date_zone_id",
                               "zone",
                               "plong",
                               "plat",
                               "boro",
                               "zone.f",
                               "hour.f",
                               "timestamp",
                               "weekday.f",
                               "gas_prices",
                               "mean_temp",
                               "mean_wind_speed",
                               "precipitation",
                               "bike_trips",
                               "trips_shl",
                               "trips_med",
                            #   "sum_entries",
                            #   "OBJECTID",
                            "arts_entertainment", 
                            "college_university",
                             "event",
                            "food",
                            "nightlife",
                            "outdoors_rec",
                            "professional",      
                            "residence",
                            "shop_service",
                            "travel",
                            "total_trips")]


rm(master_hour)
gc()

setwd(cabinets$home)
write.csv(master_write, "master_raw.csv")

#pull out zones that don't have a lot of data
zone_distribution = master_write[,sum(total_trips), by = .(zone, year(timestamp))]





#extract only manhattan---------------------------------------------------------------
manhattan = taxi_zones[taxi_zones$Borough == "Manhattan",]
master_man = master_write[master_write$zone %in% manhattan$LocationID,]
master_man = na.omit(master_man)


#test basic linear model for manhattan-----------------------------------------------

fit = lm(total_trips ~  + 
           mean_temp +
           zone.f +
           hour.f +
           weekday.f +
           #mean_wind_speed + 
           precipitation +
           gas_prices 
         #+ authorized_vehicles
         + arts_entertainment
         , master_man)

summary(fit)

#test residuals are normally distributed start with attribute join
master_man_agg = master_man[,.(total_trips = sum(total_trips)), by = .(zone = as.numeric(zone))]
tracts@data = merge(tracts@data, master_man_agg, by.x = "OBJECTID", by.y = "zone")


master_man$residuals = residuals(fit)
grps = 10
brks = quantile(master_man$residuals, 0:(grps-1)/(grps-1), na.rm=TRUE)



tracts@data



p = spplot(h, "houseValue", at=brks, col.regions=rev(brewer.pal(grps, "RdBu")), col="transparent" )
p + layer(sp.polygons(hh))

ggplot() +
  geom_polygon(data=tracts, aes(x=long, y=lat, group=group)
               , fill="black", colour="grey90", alpha=1) +
  



#impute missing data-------------------------------------------


# master$mean_wind_speed = ifelse(is.na(master$mean_wind_speed), mean(master$mean_wind_speed, na.rm = T), master$mean_wind_speed) # recode to mean
# master$precip_inches = ifelse(is.na(master$precip_inches), 0, master$precip_inches) # recode to 0
# master$wholesale_gas_price = na.locf(master$wholesale_gas_price, fromLast = T) # recode to mean
# master$authorized_vehicles = ifelse(is.na(master$authorized_vehicles), mean(master$authorized_vehicles, na.rm = T),
#                                     master$authorized_vehicles) #recode to mean
# master$bike_trips = ifelse(is.na(master$bike_trips), mean(master$bike_trips, na.rm = T),master$bike_trips)



#impute missing data using KNN
knnOutput = knnImputation(master[, !names(master) %in% c("trips_med", "trips_shl", "date")])  # perform knn imputation.
anyNA(knnOutput)

master = cbind(master$date, knnOutput, master[, c("trips_med", "trips_shl")])
colnames(master)[colnames(master) == "master$date"] = "date"



#add day and dummy code
master$weekday = weekdays(master$date)
master$weekday.f = as.factor(master$weekday)

master$total_trips = master$trips_med+master$trips_shl



#exploratory analysis--------------------------
master = master_man
gc()
#histograms
par(mfrow=c(2,2))
hist(master$mean_temp, main="temperature")
hist(master$mean_wind_speed, main="wind speed")
hist(master$precipitation, main="precipitation")
hist(master$gas_prices, main = "wholesale gas prices")
#hist(master, main = "daily fhv trips")
#hist(master$authorized_vehicles, main = "daily authorized tlc vehicles")
hist(master$bike_trips, main = "daily citibike trips")
hist(master$total_trips, main = "daily total trips")
#hist(master$subway_trips_entries, main = "daily subway entries")


#hist(master$subway_trips_exits, main = "daily subway exits") not necessary 

options(scipen=999)
#plot(master$trips_shl,master$trips_total)
plot(master$mean_temp, master$trips_total)
plot(master$mean_wind_speed, master$trips_total)
plot(master$authorized_vehicles, master$trips_total)
plot(master$precip_inches, master$total_trips)
plot(master$bike_trips, master$trips_total)
plot(master$subway_trips_entries, master$trips_total)
#plot(master$subway_trips_exits, master$trips_med) #not necessarry


# preliminary linear regression-------------------------
fit = lm(total_trips ~  + 
           mean_temp +
           zone.f +
           hour.f +
           weekday.f +
           #mean_wind_speed + 
           precipitation +
           gas_prices 
           #+ authorized_vehicles
          arts_entertainment +
            
         , master_man)

summary(fit)
plot(fit)


#adding cooks distance to account for outlier influece
cooksd = cooks.distance(mod)



#outlier removal---------------------------------------
boxplot.stats(master$trips_fhv)$out
boxplot.stats(master$authorized_vehicles)$out
boxplot.stats(master$mean_temp)$out
boxplot.stats(master$mean_wind_speed)$out #wind has outliers
boxplot.stats(master$precip_inches)$out #rain has outliers
boxplot.stats(master$wholesale_gas_price)$out #outliers in gas
boxplot.stats(master$authorized_vehicles)$out
boxplot.stats(master$bike_trips)$out
boxplot.stats(master$sp500)$out #sp500 outliers
boxplot.stats(master$subway_trips_entries)$out #outliers in subway entry 



#removing sp00 outliers, gas, rain and subway entry outliers
rain_out = boxplot.stats(master$precip_inches)$out 
gas_out = boxplot.stats(master$wholesale_gas_price)$out 
sp500_out = boxplot.stats(master$sp500)$out 
subway_out = boxplot.stats(master$subway_trips_entries)$out 


#create copy of master for outliers removed
master_or = master

master_or$precip_inches[master_or$precip_inches %in% rain_out] = NA
master_or$wholesale_gas_price[master_or$wholesale_gas_price %in% gas_out] = NA
master_or$sp500[master_or$sp500 %in% sp500_out] = NA
master_or$subway_trips_entries[master_or$subway_trips_entries %in% subway_out] = NA


#impute missing variables with KNN classifier----------------------------------------
summary(master_or)
master_or_idv = master_or[,1:15]
mice::md.pattern(master_or)

#extract non numerical values
master_or_idv = master_or[, !names(master_or) %in% c("weekday", "weekday.f", "date")]

#run knn
knnOutput = knnImputation(master_or_idv[, !names(master_or_idv) %in% "total_trips"])  # perform knn imputation.
anyNA(knnOutput)

#check for nas
summary(knnOutput)

#rebind to dv
master_final = cbind(master_or$date,knnOutput,master_or$weekday.f,master_or$total_trips)

titlez = c("date","mean_temp", "mean_wind_speed", 
           "precip_inches","wholesale_gas_price", 
           "trips_fhv", "authorized_vehicles", "bike_trips", 
           "sp500", "subway_trips_entries", 
           "subway_trips_exits","trips_med", "trips_shl", "weekday.f",
           "total_trips")
names(master_final) = titlez



#run again without outliers and KNN inmputations--------------------------
fit = lm(total_trips ~ 
           trips_fhv + 
           #mean_temp +
           #mean_wind_speed + 
           #precip_inches +
           wholesale_gas_price 
         + weekday.f
         #+ authorized_vehicles
         + bike_trips
         + sp500
         + subway_trips_entries
         #+ cpi_food_bev
         , master_final)

summary(fit)
plot(fit)














#the summary creates the formula below
#Weight = 520.79 + 202*price

#original dv
gas_date$trips_per_day

#predicted values
fitted(fit)
p_o = as.data.frame(cbind(fitted(fit), gas_date$trips_per_day))
p_o$difference = p_o[,1] - p_o[,2]
mean(p_o$difference^2)






test_mta_works = fread("http://web.mta.info/developers/data/nyct/turnstile/turnstile_161224.txt", sep = ',')

test_mta_wont_work = fread("http://web.mta.info/developers/data/nyct/turnstile/turnstile_140419.txt", sep = ',', fill = T)




library(sqldf)
a1r <- head(warpbreaks)
a1s <- sqldf("select * from warpbreaks limit 6")
