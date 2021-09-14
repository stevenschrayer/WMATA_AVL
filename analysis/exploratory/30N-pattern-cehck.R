# Why are we getting multiple 30N patterns in a direction in 
# the rawnav data, but our other records see only one?

library(gtfsec)
library(tidytransit)
library(tidyverse)
library(mapview)
library(sf)

wmata_gtfs_mar <-
  read_gtfs(
    path = "C:/OD/Foursquare ITP/Projects - WMATA Datamart/Task 3 - Bus Priority/Data/00-Raw/wmatabus-20210308-gtfs.zip"
  )

wmata_prep_mar <-
  prepare_gtfs(wmata_gtfs_mar)


# hmm, only one shape
patts_30n_mar <-
  wmata_prep_mar$trip_prepared %>%
  filter(route_short_name == "30N", direction_id == 1) %>%
  distinct(shape_id, .keep_all = TRUE)

theshapes_mar <-
  wmata_prep_mar %>%
  make_gtfs_line(route_short_name, direction_id, shape_id)

theshapes_mar_30N <-
  theshapes_mar %>% 
  filter(route_short_name == "30N", direction_id == 1)

# February 2021-02-08 -------------------------------------------------------------


wmata_gtfs_feb <-
  read_gtfs(
    path = "C:/OD/Foursquare ITP/Projects - WMATA Datamart/Task 3 - Bus Priority/Data/00-Raw/wmatabus-20210308-gtfs.zip"
  )

wmata_prep_feb <-
  prepare_gtfs(wmata_gtfs_feb)


# hmm, only one shape
patts_30n_feb <-
  wmata_prep_feb$trip_prepared %>%
  filter(route_short_name == "30N", direction_id == 1) %>%
  distinct(shape_id, .keep_all = TRUE)

theshapes_feb <-
  wmata_prep_feb %>%
  make_gtfs_line(route_short_name, direction_id, shape_id)

theshapes_feb_30N <-
  theshapes_feb %>% 
  filter(route_short_name == "30N", direction_id == 1)

# test <- 
#   theshapes_feb_30N %>%
#   st_cast('LINESTRING')

mapview(theshapes_feb_30N) + mapview(theshapes_mar_30N)
