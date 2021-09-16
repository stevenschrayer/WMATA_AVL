library(tidyverse)
library(sf)
library(mapview)

path_sp <- "C:/OD/Foursquare ITP/Projects - WMATA Datamart"

path_data <-
  file.path(
    path_sp,
    "Task 3 - Bus Priority",
    "Data",
    "01-Interim"
  )

rawnav_matched <-
  read_csv(
    file.path(path_data,"rawnav_mapmatch_matched.csv")
  ) %>%
  st_as_sf(
    .,
    coords = c("lon", "lat"),
    crs = 4326L, #WGS84
    agr = "constant"
  )
      

rawnav_in <-
  read_csv(
    file.path(path_data,"rawnav_mapmatch_in.csv")
  ) %>%
  st_as_sf(
    .,
    coords = c("lon", "lat"),
    crs = 4326L, #WGS84
    agr = "constant"
  )
      
# compare
mapview(rawnav_in, col.regions = "blue") + mapview(rawnav_matched, col.regions = "red")

# look at size of miss

mapview(rawnav_in, col.regions = "grey80", alpha = 0.5) + 
  mapview(rawnav_matched, zcol = "distance_from_trace_point")


# distance along edge value
# The distance along the associated edge for this matched point. For example, if the matched point is halfway along the edge then the value would be 0.5. This value will not exist if this point was unmatched.
mapview(rawnav_matched, zcol = "distance_along_edge")


# edge index
# 
mapview(rawnav_matched, zcol = "edge_index")

