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

rawnav <-
  read_csv(
    file.path(path_data,"rawnav_matched.csv"),
    n_max = 100000
  ) 

rawnav_matched <-
  rawnav %>%
  st_as_sf(
    .,
    coords = c("longmatch", "latmatch"),
    crs = 4326L, #WGS84
    agr = "constant"
  )

rawnav_og <-
  rawnav %>%
  select(
    filename,
    index_run_start,
    index_loc,
    long,
    lat
  ) %>%
  st_as_sf(
    .,
    coords = c("long", "lat"),
    crs = 4326L, #WGS84
    agr = "constant"
  )

thetripfile <- "rawnav02205171017.txt"
thetripidx <- 1827

rawnav_og_ti <-
  rawnav_og %>%
  filter(
    filename == thetripfile,
    index_run_start == thetripidx
  )
      
rawnav_matched_ti <-
  rawnav_matched %>%
  filter(
    filename == thetripfile,
    index_run_start == thetripidx
  )

# compare
mapview(rawnav_og_ti, col.regions = "grey80", alpha = 0.5, layer.name = "Original") +
  mapview(rawnav_matched_ti, col.regions = "red", layer.name = "Matched")

# look at size of miss
mapview(rawnav_og_ti, col.regions = "grey80", alpha = 0.5, layer.name = "Original") + 
  mapview(rawnav_matched_ti, zcol = "distance_from_trace_point", layer.name = "Dist. from Orig.")


# edge index
mapview(rawnav_matched_ti, zcol = "street_names")


mapview(rawnav_matched_ti, zcol = "way_id")


# distance along edge value
# The distance along the associated edge for this matched point. For example, if the matched point is halfway along the edge then the value would be 0.5. This value will not exist if this point was unmatched.
mapview(rawnav_matched_ti, zcol = "distance_along_edge")

# The distance along the associated edge for this matched point. For example, if the matched point is halfway along the edge then the value would be 0.5. This value will not exist if this point was unmatched.
mapview(rawnav_matched_ti, zcol = "end_heading", layer.name = "street heading")


