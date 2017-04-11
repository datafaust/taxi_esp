#build out centroids from tz to use as spatial markers for zones 

setwd(cabinets$gis)

#load libraries-----------------------------
library(rgeos)
library(rgdal)
library(sp)
library(GISTools)
library(ggplot2)

#load tz file and transform------------------------
tracts = readOGR("tz_cd.shp", layer="tz_cd")

#transform coordinate system
lat_long = CRS("+init=epsg:4326") 
tracts_lat_long = spTransform(tracts, lat_long)

#test transformation
proj4string(tracts_lat_long)

#extract centroids--------------------------------------
centroids = cbind(as.data.frame(getSpPPolygonsLabptSlots(tracts_lat_long)),
                  tracts@data)[,c(1,2,4,5,6,9)]

#rename headings etc------------------------------------
names(centroids) = c("plong", "plat", "zone", "taxi_zone", "boro", "puma")

