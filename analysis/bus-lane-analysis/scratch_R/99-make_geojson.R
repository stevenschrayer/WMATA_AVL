library(tidyverse)
library(sf)
library(geojsonsf)

harddrive <- "C:\\Users\\e043868\\Documents\\RawNav\\segment_shps"

shps <- list.files(harddrive, pattern = ".shp", full.names = TRUE)

shps_df <- map_dfr(shps, read_sf) %>%
  #make nickname for shorter seg_name_id
  mutate(nickname = case_when(
    name_str == "M Street Southeast" ~ "MSE",
    name_str == "Martin Luther King Junior Avenue Southeast" ~ "MLK",
    name_str == "7th Street Northwest" ~ "7th",
    TRUE ~ str_sub(name_str, 1, 4)
  ))


shps_df_sorted <- shps_df %>%
  #workaround to arrange north-south so IDs are in order
  mutate(coords_start = lwgeom::st_startpoint(.),
         coords_end = lwgeom::st_endpoint(.),
         coords_start_y = as.numeric(st_coordinates(coords_start)[,2]),
         coords_end_y = as.numeric(st_coordinates(coords_end)[,2]),
         northmost = if_else(coords_start_y > coords_end_y, coords_start_y, coords_end_y)) %>%
  group_by(name_str) %>%
  arrange(desc(northmost)) %>%
  #make segment name
  rowid_to_column() %>%
  mutate(seg_name_id = glue::glue("{nickname}_{rowid}")) %>%
  ungroup() %>%
  select(seg_name_id, name_str, geometry) %>%
  st_as_sf()

#write to geojson
sf_geojson(shps_df_sorted) %>%
  writeLines(., file.path(harddrive, glue::glue("{Sys.Date()}-segments.geojson")))

#just 16th for testing
shps_df_sorted %>%
  filter(str_detect(seg_name_id, "16th")) %>%
  sf_geojson() %>%
  writeLines(., file.path(harddrive, glue::glue("{Sys.Date()}-16th-segments.geojson")))
