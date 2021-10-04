
library(tidyverse)
library(sf)
library(mapview)
library(httr)
library(gtfsec)
library(tidytransit)
library(smoothr)
options(scipen = 999)
path_sp <- "C:/OD/Foursquare ITP/Projects - WMATA Datamart"

gtfs_obj <-
  tidytransit::read_gtfs(
    file.path(
      path_sp,
      "Task 3 - Bus Priority",
      "Data",
      "00-Raw",
      "wmatabus-20210308-gtfs.zip"
    )
  )

# drop to a few routes
routes_hi <-
  c('30N','30S','32','33','36','37','39','42','43','G8')

gtfs_obj$routes <- 
  gtfs_obj$routes %>%
  filter(route_short_name %in% routes_hi)

gtfs_prep <-
  prepare_gtfs(
    gtfs_obj
  )
