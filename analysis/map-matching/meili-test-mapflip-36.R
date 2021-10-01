
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
    file.path(path_data,"rawnav_match_decomp_36.csv")
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
  atripfile = "rawnav02516171009.txt",
  atripidx = 5595
)

preview_match(
  atripfile = "rawnav02516171009.txt",
  atripidx = 5595,
  zcol = "stop_id_group"
)

preview_match(
  atripfile = "rawnav02516171009.txt",
  atripidx = 5595,
  zcol = "door_case"
)

preview_match(
  atripfile = "rawnav02516171009.txt",
  atripidx = 5595,
  zcol = "heading"
)


preview_match(
  atripfile = "rawnav02197171004.txt",
  atripidx = 18400
)

# what's with this trip
atrip <-
rawnav %>%
  filter(
    filename == "rawnav02197171004.txt",
    index_run_start == 18400
  )


preview_match(
  atripfile = "rawnav02197171030.txt",
  atripidx = 17699,
  zcol = "heading"
)


preview_match(
  atripfile = "rawnav02102171021.txt",
  atripidx = 3639,
  zcol = "heading"
)

preview_match(
  atripfile = "rawnav02102171021.txt",
  atripidx = 3639,
  zcol = "type"
)

rematch <-
  read_csv("rematch.csv") %>%
  st_as_sf(
    .,
    coords = c("longmatch", "latmatch"),
    crs = 4326L, #WGS84
    agr = "constant"
  )

mapview::mapview(rematch, zcol = "index_loc")


preview_match(
  atripfile = "rawnav02553171031.txt",
  atripidx = 9602
)

# checking on edge ids
# id = 988043827912
# way_id = 695842775
# latmatch 38.901333
# longmatch -77.04216
# filename rawnav02553171031.txt
# index_run_start 9602
# 11446


