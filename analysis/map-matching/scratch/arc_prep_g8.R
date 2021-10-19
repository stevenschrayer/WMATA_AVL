library(readr)
library(dplyr)
library(sf)
rg8_unique <-
  readr::read_csv(
    "C:/OD/Foursquare ITP/Projects - WMATA Datamart/Task 3 - Bus Priority/Data/01-Interim/decomp_match_g8_1m.csv",
    col_types = 
      cols_only(
        'filename' = col_character(),
        'index_run_start' = col_number(),
        'pattern' = col_number(),
        'latmatch' = col_number(),
        'longmatch' = col_number()
      )
  ) %>%
  mutate(
    unique_id = paste0(filename,"_",index_run_start)
  )

rg8_unique_out <-
  rg8_unique %>%
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
  rg8_unique_out, 
  dsn = "C:/OD/Foursquare ITP/Projects - WMATA Datamart/Task 3 - Bus Priority/Data/01-Interim/routeg8_unique.geojson"
)
