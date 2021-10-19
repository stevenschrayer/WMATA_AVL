
library(tidyverse)
library(sf)
library(mapview)

path_sp <- "C:/OD.old/Foursquare ITP/Projects - WMATA Datamart"

path_data <-
  file.path(
    path_sp,
    "Task 3 - Bus Priority",
    "Data",
    "01-Interim"
  )

rawnav <-
  read_csv(
    file.path(path_data,"route43_decomp_match.csv"),
    n_max = 100
  )




preview_match <-
  function(
    atripfile,
    atripidx,
    zcol = "index_loc"
  ){
    
    rawnav_matched_ti <-
      rawnav %>%
      filter(
        filename == atripfile,
        index_run_start == atripidx
      ) %>%
      st_as_sf(
        .,
        coords = c("longmatch", "latmatch"),
        crs = 4326L, #WGS84
        agr = "constant",
        remove = FALSE
      )
    
    rawnav_og_ti <-
      rawnav %>%
      filter(
        filename == atripfile,
        index_run_start == atripidx
      ) %>%
      st_as_sf(
        .,
        coords = c("long", "lat"),
        crs = 4326L, #WGS84
        agr = "constant"
      )
    
    mapview(
      rawnav_og_ti,
      col.regions = "grey30",
      alpha = 0,
      layer.name = "Original"
    ) +
      mapview(
        rawnav_matched_ti,
        layer.name = "Matched",
        alpha = 0,
        zcol = zcol
      )
    
    
  }

preview_match(
  atripfile = "rawnav06501191021.txt",
  atripidx = 17318
)

preview_match(
  atripfile = "rawnav02547171017.txt",
  atripidx = 5735
)

preview_match(
  atripfile = "rawnav02516171011.txt",
  atripidx = 4260,
  zcol = "stop_id_group"
)


preview_match(
  atripfile = "rawnav02527171015.txt",
  atripidx = 5378,
  zcol = "stop_id_group"
)

rawnav %>%
  filter(
    filename == "rawnav02527171015.txt",
    index_run_start == 5378,
  ) %>%
  View()

# this one in the same file
preview_match(
  atripfile = "rawnav02527171015.txt",
  atripidx = 6475,
  zcol = "door_state"
)
# even after redoing the match, still looks bad
testrematch <-
  read_csv("testrematch.csv")

testrematch <-
  testrematch %>%
  st_as_sf(
    .,
    coords = c("longmatch", "latmatch"),
    crs = 4326L, #WGS84
    agr = "constant"
  )
    

preview_match(
  atripfile = "rawnav02531171013.txt",
  atripidx = 12892,
  zcol = "trip_seg"
)

rawnav02533171018.txt
22465

preview_match(
  atripfile = "rawnav02531171013.txt",
  atripidx = 12892,
  zcol = "trip_seg"
)

anothermatch <-
  read_csv("another43.csv") %>%
  st_as_sf(
    .,
    coords = c("longmatch", "latmatch"),
    crs = 4326L, #WGS84
    agr = "constant"
  )

mapview::mapview(anothermatch, zcol = "index_loc")
