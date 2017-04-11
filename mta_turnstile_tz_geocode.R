#geocode turnstile data
library(rgdal)
library(ggmap)
library(maptools)
library(spatialEco)

#basic map --------------------------------------------------------
setwd(cabinets$gis)
cd = readOGR("tz_cd.shp", layer = "tz_cd")
cd_sp = readShapeSpatial("tz_cd.shp", proj4string=CRS("+proj=longlat +datum=NAD83"))


#pull lookup data--------------------------------------
setwd(cabinets$git_home)
lookup = read.csv("mta_longlat_lookup_table_final.csv", header =T)


#merge and spatial join -----------------------------------
setwd("C:/Users/lopezf/Desktop/regression_test/regression_test/mta_turnstile_data")


pblapply(list.files(), function(x) {
  
  setwd("C:/Users/lopezf/Desktop/regression_test/regression_test/mta_turnstile_data")
  fs_data = fread(x)
  gc()
  
  #print(fs_data)
  
  #merge turnstile data with lookup--------
  fs_data$STATION = tolower(fs_data$STATION) #lower letters
  fs_data$STATION =  gsub(" ", "", fs_data$STATION, fixed = TRUE) #kill spaces
  fs_data$id = gsub("[^0-9]", "", fs_data$STATION) #extract numbers only
  fs_data$id = ifelse(fs_data$id == "", fs_data$STATION, fs_data$id) #fill no numericals
  fs_data$superid = paste0(fs_data$id, fs_data$LINENAME) #create superid
  fs_data$superid = tolower(fs_data$superid) #lower one last time
  
  
  
  #merge
  fs_data = merge(fs_data, lookup, by = "superid", all.x = T)
  
  #print(table(is.na(fs_data$Station_Longitude)))
  
  colnames(fs_data)[colnames(fs_data) == "Station_Longitude"] = 'x'
  colnames(fs_data)[colnames(fs_data) == "Station_Latitude"] = 'y'
  
  #print(table(is.na(fs_data$x)))
  
  #eliminate NA's in latitude (mismatches already looked through)------
  fs_data = na.omit(fs_data, cols = "x")
  
  #print(fs_data)
  
  #spatial join 
  fs_points = fs_data
  
  #print(table(is.na(fs_data$Station_Latitude)))
  
  #print(fs_points)
  fs_points$x = as.numeric(as.character(fs_points$x))
  fs_points$y = as.numeric(as.character(fs_points$y))
  
  #print(table(is.na(fs_points$x)))
  
  
  fs_points = fs_points[, c("x","y")]
  
  
  #print(table(is.na(fs_points$x)))
  
  
  pointsSP = SpatialPoints(fs_points, 
                           proj4string=CRS("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"))
  
  pointsSP = spTransform(pointsSP, proj4string(cd))
  
  # Use 'over' to get _indices_ of the Polygons object  containing each point 
  indices = over(pointsSP, cd)
  
  fs_data = cbind(fs_data, indices)
  
  rm(fs_points)
  
  setwd("C:/Users/lopezf/Desktop/regression_test/regression_test/mta_turnstile_data_geocoded_tz")
  
  #print(fs_data)
  
  write.csv(fs_data, paste0("geocoded_taxizones_",substr(x,1, nchar(x)-4),".csv"))
  
  gc()
})

