#spatially code taxi zones into citibike data 

library(rgdal)
library(ggmap)
library(maptools)
library(spatialEco)
library(data.table)

#basic map --------------------------------------------------------
setwd("C:\\Users\\lopezf\\Desktop\\regression_test\\regression_test")
cd = readOGR("taxi_zones_sp.shp", layer = "taxi_zones_sp")
cd_sp = readShapeSpatial("taxi_zones_sp.shp", proj4string=CRS("+proj=longlat +datum=NAD83"))

#spatial join procedure-------------------------------------------
setwd("C:\\Users\\lopezf\\Desktop\\regression_test\\regression_test\\citibike\\load")
pblapply(list.files()[4:22], function(x) {
  
  setwd("C:\\Users\\lopezf\\Desktop\\regression_test\\regression_test\\citibike\\load")
  fs_data = fread(x)
  gc()
  
  # Convert pointsDF to a SpatialPoints object 
  fs_points = fs_data
  
  names(fs_points)[names(fs_points) == 'start station longitude'] = 'x'
  names(fs_points)[names(fs_points) == 'start station latitude'] = 'y'
  fs_points = fs_points[, c("x","y")]
  
  fs_points$x = as.numeric(as.character(fs_points$x))
  fs_points$y = as.numeric(as.character(fs_points$y))
  
  pointsSP = SpatialPoints(fs_points, 
                           proj4string=CRS("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"))
  
  pointsSP = spTransform(pointsSP, proj4string(cd))
  
  # Use 'over' to get _indices_ of the Polygons object  containing each point 
  indices = over(pointsSP, cd)
  
  fs_data = cbind(fs_data, indices)
  
  rm(fs_points)
  
  setwd("C:\\Users\\lopezf\\Desktop\\regression_test\\regression_test\\citibike\\citibike_geocoded")
  
  write.csv(fs_data, paste0("geocoded_taxizones_",substr(x,1, nchar(x)-4),".csv"))
  
  gc()
})






