library(readr)
library(dplyr)
library(sf)
r43unique <-
  readr::read_csv(
    "C:/OD/Foursquare ITP/Projects - WMATA Datamart/Task 3 - Bus Priority/Data/01-Interim/route43_decomp_match.csv",
    col_types = 
      cols_only(
        'unique_id' = col_character(),
        'latmatch' = col_number(),
        'longmatch' = col_number()
      )
  )

r43unique_out <-
  r43unique %>%
  st_as_sf(
    .,
    coords = c("longmatch", "latmatch"),
    crs = 4326L, #WGS84
    agr = "constant"
  ) %>%
  group_by(unique_id) %>%
  summarize(do_union = FALSE) %>%
  st_cast("LINESTRING")

write_sf(
  r43unique_out, 
  dsn = "C:/OD/Foursquare ITP/Projects - WMATA Datamart/Task 3 - Bus Priority/Data/01-Interim/route43_unique.geojson"
)


stop_patt_all <-
  read_csv(
    "C:/OD/Foursquare ITP/Projects - WMATA Datamart/Task 3 - Bus Priority/Data/02-Processed/schedule_data_allroutes_oct17_oct19.csv"
  )

stop_patt_hi <-
  read_csv(
    "C:/OD/Foursquare ITP/Projects - WMATA Datamart/Task 3 - Bus Priority/Data/02-Processed/schedule_data_allroutes_oct17_oct19.csv"
  )

