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
                citibike = "I:/COF/COF/Analytics_and_Automation_Engineering/regression build/citibike/citibike_geocoded",
                mta = "I:/COF/COF/Analytics_and_Automation_Engineering/regression build/mta_turnstile_data_geocoded_tz",
                git_home = "C:/Users/lopezf/Documents/R/R-3.3.1/library/taxi_esp",
                gis = "I:\\COF\\COF\\GIS"
) 

#read gas from FRED
gas_date_harbour = fread("http://www.quandl.com/api/v1/datasets/FRED/DGASNYH.csv")
gas_date_harbour[,timestampz:= fastPOSIXct(DATE, tz = "GMT")][
  ,gas_price:= as.numeric(DGASNYH)]


#Quality of Life--------------------------------------------------------------------------------------------------

setwd(cabinets$home)

#sp500 data by day
sp500 = fread("SP500.csv")
sp500[,timestampz:= fastPOSIXct(DATE, tz = "GMT")]
sp500[,VALUE:= ifelse(VALUE == ".", NA, VALUE)]
sp500[,sp500:= as.numeric(as.character(VALUE))]

#cpi food and beverages
cpifbev = fread("cpi_food_bev.csv")
cpifbev[,timestampz:= fastPOSIXct(DATE, tz = "GMT")]
cpifbev[,cpi:= as.numeric(as.character(CPIFABSL))]


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
      ,PrecipitationIn:=as.numeric(PrecipitationIn)][
        ,EST:=fastPOSIXct(EST, tz = "GMT")]
weather = weather[!duplicated(weather$EST),]
names(weather) = c("timestampz", "mean_temp","mean_wind_speed","rain_inches")

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
#bike_trips = fread(list.files()[1])

#sum trips hourly by region 
setwd(cabinets$citibike)
bike_trips = rbindlist(
  pblapply(list.files()
           #[1:2]
           , function(x) {
    featurez = c("starttime", "OBJECTID")
    bike_trips = fread(x, select = featurez)
    #split time at the slash
    bike_trips = setDT(data.frame(bike_trips, do.call(rbind, str_split(bike_trips$starttime, '/'))))
    bike_trips = setDT(data.frame(bike_trips, do.call(rbind, str_split(bike_trips$X3, ' '))))
    bike_trips = setDT(data.frame(bike_trips, do.call(rbind, str_split(bike_trips$X2.1, ':'))))
    
    #split time, and hour then add zeros below 10 and paste back together to form a viable timestamp
    bike_trips[,X1:=as.numeric(X1)][,X2:=as.numeric(X2)][,X1.2:=as.numeric(X1.2)]
    bike_trips[,X1:=ifelse(X1 < 10, paste0("0", X1), X1)][
      ,X2:=ifelse(X2 < 10, paste0("0", X2), X2)][
        ,X1.2:=ifelse(X1.2 < 10, paste0("0", X1.2), X1.2)][
          ,timestampz:=fastPOSIXct(paste0(X1.1,"-",X1,"-",X2," ",X1.2,":","00", ":00"),tz = "GMT"),]
    
    #sum trips by hour and taxi zone
    daily_trips = bike_trips[,.(bike_trips = .N), by = .(taxi_zone = as.numeric(OBJECTID), timestampz)]
    rm(bike_trips)
    gc()
    return(assign(x, daily_trips))
  })
)



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
#mta = fread(list.files()[1])
#loop through files and aggregate
subway_trips = rbindlist(
  pblapply(list.files(), function(x) {
    featurez = c("HOURLY_ENTRY", "HOURLY_EXITS" ,"DATE", "TIME", "OBJECTID", "TIMESTAMPZ")
    mta = fread(x, select = featurez)
    subway_trips = mta[,.(subway_entries=sum(HOURLY_ENTRY), subway_exits = sum(HOURLY_EXITS))
                       , by = .(taxi_zone = OBJECTID, timestampz = TIMESTAMPZ)]
    return(assign(x,subway_trips))
    rm(subway_trips)
    gc()
  })
)

subway_trips = subway_trips[,taxi_zone:=as.numeric(taxi_zone)][
  ,timestampz:=fastPOSIXct(timestampz, tz = "GMT")][order(taxi_zone, timestampz),]
subway_trips[,subway_entries:=as.numeric(ifelse(subway_entries < 1, "NA", subway_entries))]
subway_trips[,subway_exits:=as.numeric(ifelse(subway_exits < 1, "NA", subway_exits))]
subway_trips = na.omit(subway_trips)
summary(subway_trips)


#source zone values------------------------------------------------------------------------
setwd(cabinets$git_home)
source("tz_centroid_builder.R")

#foursquare commercial strenght---------------------------------------------------------------------------------------------------------------------
setwd(cabinets$foursquare)

featurez = c("OBJECTID", "big_cat", "year", "locationid", "locationname")
fs = fread("geocoded_taxizones_master_fs.csv",  
           select = featurez)
fs_data = fs[ ,.N , by = .(year,OBJECTID, big_cat)][,year_id:= paste0(year,as.numeric(OBJECTID))]

#pivot data to merge on year
fs_data = spread(fs_data, big_cat, N)
names(fs_data) = c("year", "taxi_zone", "year_id", "arts_entertainment",
                   "college_university", "event", "food", "nightlife",
                   "outdoors_rec", "professional", "residence", "shop_service", "travel")
rm(fs)
gc()




#bring trips by day, hour, taxi zone-----------------------------------------------------------------------------------------------------
setwd(cabinets$home)

#create base file that has every taxi location and every day and every hour
base_hours = seq(
  from=fastPOSIXct("2015-1-1 0:00", tz="GMT"),
  to=fastPOSIXct("2016-11-11 23:00", tz="GMT"),
  by="hour"
) 

taxi_zones = sqlFetch(TPEP2, "TPEP2_LOCATION_lookup")
loc = as.character(taxi_zones$LocationID)
master_hour = setDT(merge(loc, base_hours))
colnames(master_hour) = c("taxi_zone","timestampz")
gc()

#merge zone long lat onto master hour (inner join gets rid of numbers that aren't zones)
master_hour = merge(master_hour, centroids[,c("plong", "plat", "taxi_zone","boro")],by = "taxi_zone")
master_hour[,.N, by = .(is.na(plat))] #tests NA values should be only FALSE
master_hour[,taxi_zone:=as.numeric(taxi_zone)]

#taxi data
featurez = c("puloc", "trips_med","date" ,"hour", "zone_from")
hourly_med_trips_loc = fread("hourly_med_trips_loc.csv", select = featurez)
hourly_shl_trips_loc = fread("hourly_shl_trips_loc.csv", select = featurez)
hourly_med_trips_loc[,timestampz:= fastPOSIXct(paste0(date, " ", hour, ":00:00"),tz = "GMT")]
hourly_shl_trips_loc[,timestampz:= fastPOSIXct(paste0(date, " ", hour, ":00:00"),tz = "GMT")]
colnames(hourly_shl_trips_loc)[colnames(hourly_shl_trips_loc) == "trips_med"] = "trips_shl"
colnames(hourly_med_trips_loc)[colnames(hourly_med_trips_loc) == "puloc"] = "taxi_zone"
colnames(hourly_shl_trips_loc)[colnames(hourly_shl_trips_loc) == "puloc"] = "taxi_zone"
hourly_med_trips_loc[,taxi_zone:=as.numeric(taxi_zone)]
hourly_shl_trips_loc[,taxi_zone:=as.numeric(taxi_zone)]


#test class equality (should show true)
class(hourly_med_trips_loc$taxi_zone) == class(master_hour$taxi_zone)
class(master_hour$taxi_zone) == class(hourly_shl_trips_loc$taxi_zone)
class(master_hour$taxi_zone) == class(bike_trips$taxi_zone)
class(bike_trips$taxi_zone) == class(subway_trips$taxi_zone)
class(hourly_med_trips_loc$timestampz) == class(master_hour$timestampz)
class(master_hour$timestampz) == class(bike_trips$timestampz)
class(bike_trips$timestampz) == class(subway_trips$timestampz)
class(subway_trips$timestampz) == class(gas_date_harbour$timestampz)
class(gas_date_harbour$timestampz) == class(sp500$timestampz)
class(sp500$timestampz) == class(cpifbev$timestampz)
class(cpifbev$timestampz) == class(weather$timestampz)

#merge all dat----------------------------------------------------------------------------------------------------

#merge daily variables
master_day = setDT(merge(weather,gas_date_harbour, by = "timestampz", all.x = T))
master_day = merge(master_day, sp500, by = c("timestampz"), all.x = T)
master_day = merge(master_day, cpifbev, by = c("timestampz"), all.x = T)[,c(1,2,3,4,7,10,13)][
  ,gas_price:= na.locf(gas_price,fromLast = F)][
    ,sp500:=na.locf(sp500,fromLast = F)][
      ,cpi:=na.locf(cpi,fromLast = F),][
        ,rain_inches:=ifelse(is.na(rain_inches), mean(rain_inches, na.rm = T), rain_inches)]

#merge hourly variables on master tpep trip set
master_hour = merge(master_hour, hourly_med_trips_loc[,c("taxi_zone", "trips_med","timestampz")]
                    ,by = c("taxi_zone","timestampz"), all.x = T)

#merge shl trip set
master_hour = merge(master_hour, hourly_shl_trips_loc[, c("taxi_zone","trips_shl", "timestampz")] 
                    ,by = c("taxi_zone","timestampz"), all.x = T)

#merge bike trips
master_hour = merge(master_hour, bike_trips[,c("taxi_zone","timestampz","bike_trips")]
                    ,by = c("taxi_zone","timestampz"), all.x = T)

#merge subway entries & exits
master_hour = merge(master_hour, subway_trips[,c("taxi_zone","timestampz","subway_entries", "subway_exits")]
                    ,by = c("taxi_zone","timestampz"), all.x = T)

#merge weather and gas 
master_hour = merge(master_hour, master_day, by = "timestampz", all.x = T)

#merge fs data
master_hour[,year_id:= paste0(year(timestampz),taxi_zone)]
master_hour = merge(master_hour, fs_data, by = c("year_id","taxi_zone"), all.x =T)

master_hour[,total_trips:= trips_med + trips_shl]
setorder(master_hour, taxi_zone, timestampz)

gc()
summary(master_hour)

#treat missing values in data--------------------------------------------------------------
#we assume taxi data as pure so NA's for trips = 0
master_hour[,trips_shl:= ifelse(is.na(trips_shl), 0, trips_shl)]
master_hour[,trips_med:= ifelse(is.na(trips_med), 0, trips_med)]
master_hour[,bike_trips:= ifelse(is.na(bike_trips),0,bike_trips)]

#code fs variables as 0 if not present 
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
master_hour[,weekday.f := as.factor(weekdays(timestampz))][
,zone.f := as.factor(taxi_zone)][
,hour.f :=as.factor(hour(timestampz))]


#extract final data set-------------------------------------------------------------
master_write = master_hour[, c("taxi_zone",
                               "timestampz",
                               "plong",
                               "plat",
                               "boro",
                               "zone.f",
                               "hour.f",
                               "weekday.f",
                               "gas_price",
                               "mean_temp",
                               "mean_wind_speed",
                               "rain_inches",
                               "bike_trips",
                               "trips_shl",
                               "trips_med",
                                 "subway_entries",
                               "subway_exits",
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

master_man = master_write[boro == "Manhattan",]

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
         + college_university
         + event
         + food
         + nightlife
         + outdoors_rec
         + professional      
         + residence
         + shop_service
         + travel
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
