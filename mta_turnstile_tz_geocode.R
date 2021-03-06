#geocode turnstile data
library(rgdal)
library(ggmap)
library(maptools)
library(spatialEco)
library(data.table)
library(readxl)
library(pbapply)

#basic map --------------------------------------------------------
setwd(cabinets$gis)
cd = readOGR("tz_cd.shp", layer = "tz_cd")
cd_sp = readShapeSpatial("tz_cd.shp", proj4string=CRS("+proj=longlat +datum=NAD83"))

#pull lookup data--------------------------------------

#pulls custom geographic 
setwd(cabinets$git_home)
lookup = read.csv("mta_longlat_lookup_table_final.csv", header =T)


#pull in remote key on booths
temp = tempfile(fileext = ".xls")
dataURL = "http://web.mta.info/developers/resources/nyct/turnstile/Remote-Booth-Station.xls"
download.file(dataURL, destfile=temp, mode='wb')
remote_key = read_excel(temp, sheet =1)
names(remote_key) = c("UNIT", "BOOTH", "STATION", "LINENAME", "DIVISION")


#merge and spatial join -----------------------------------
setwd("I:\\COF\\COF\\Analytics_and_Automation_Engineering\\regression build\\mta_turnstile_data")


pblapply(list.files(), function(x) {
  
  setwd("I:\\COF\\COF\\Analytics_and_Automation_Engineering\\regression build\\mta_turnstile_data")
  fs_data = fread(x)
  gc()
  #fs_data = fread(list.files()[1])
  #print(fs_data)
  
  #merge turnstile data with lookup--------
  fs_data[,STATION:= tolower(STATION)] #lower letters
  fs_data[,STATION:=  gsub(" ", "", STATION, fixed = TRUE)] #kill spaces
  fs_data[,id:= gsub("[^0-9]", "", STATION)] #extract numbers only
  fs_data[,id:= ifelse(id == "", STATION, id)] #fill no numericals
  fs_data[,superid:= tolower(paste0(id, LINENAME))] #create superid
  
  
  #read data
  fs_data[,DATE:= format(as.Date(DATE, "%m/%d/%Y"), "%Y-%m-%d")][
    ,HOUR:= substr(TIME, 1,2)][
      ,TIMESTAMPZ:=as.POSIXct(format(paste(DATE, TIME), origin = "%m/%d/%Y %H:%M:%S"), tz = "America/New_York")][
        ,DIFF_HOURS:=round(
          as.numeric(
            difftime(shift(TIMESTAMPZ, type = "lead"), TIMESTAMPZ, units = "hours")
          )
        ), by = .(STATION, SCP, `C/A`, UNIT)]
  
  fs_data[,HOURLY_ENTRY:= c(NA, diff(ENTRIES)), by = .(STATION, SCP, `C/A`, UNIT)][
    ,HOURLY_EXITS:= c(NA, diff(EXITS)), by = .(STATION, SCP, `C/A`, UNIT)]

  #merge
  fs_data = merge(fs_data, lookup, by = "superid"#, all.x = T
  )
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
  fs_data = setDT(cbind(fs_data, indices))#[,.(superid, C/A, UNIT, SCP, STATION, LINENAME,
                                           #   ENTIRIES, EXITS, TIMESTAMPZ, DIFF_HOURS, HOURLY_ENTRIES,
                                          #    HOURLY_EXITS)]
  rm(fs_points)
  
  setwd("I:/COF/COF/Analytics_and_Automation_Engineering/regression build/mta_turnstile_data_geocoded_tz")
  
  #print(fs_data)
  write.csv(fs_data, paste0("geocoded_taxizones_",substr(x,1, nchar(x)-4),".csv"))
  
  gc()
})

