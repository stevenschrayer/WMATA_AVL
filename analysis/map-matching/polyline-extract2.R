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


encoded <-
  read_csv(
    file.path(path_data,"encodedlines36.csv")
  )

# this is the valhalla recommended function for R
# https://valhalla.readthedocs.io/en/latest/decoding/
# slightly_renamed
decode_val <- function(encoded) {
  chars <- stringr::str_split(encoded, "")[[1]]
  lats <- vector(mode = "integer", length = 1)
  lons <- vector(mode = "integer", length = 1)
  i <- 0
  
  while (i < length(chars)){
    shift <- 0
    result <- 0
    byte <- 0x20L
    
    while (byte >= 0x20) {  
      i <- i + 1
      byte <- chars[[i]] %>% utf8ToInt() - 63
      result <- bitwOr(result, bitwAnd(byte, 0x1f) %>% bitwShiftL(shift))
      shift <- shift + 5
      if (byte < 0x20) break
    }
    
    if (bitwAnd(result, 1)) {
      result <- result %>% bitwShiftR(1) %>% bitwNot()
    } else {
      result <- result %>% bitwShiftR(1)
    }
    
    lats <- c(lats, (lats[[length(lats)]] + result))
    
    shift <- 0
    result <- 0
    byte <- 10000L
    
    while (byte >= 0x20) {  
      i <- i + 1
      byte <- chars[[i]] %>% utf8ToInt() - 63
      result <- bitwOr(result, bitwAnd(byte, 0x1f) %>% bitwShiftL(shift))
      shift <- shift + 5
      if (byte < 0x20) break
    }
    
    if (bitwAnd(result, 1)) {
      result <- result %>% bitwShiftR(1) %>% bitwNot()
    } else {
      result <- result %>% bitwShiftR(1)
    }
    
    lons <- c(lons, (lons[[length(lons)]] + result))
  }
  
  decoded <- 
    tibble::tibble(
      lat = lats[2:length(lats)]/1000000,
      lng = lons[2:length(lons)]/1000000)
  
  return (decoded)
}

safe_decode_val <- safely(decode_val)

decoded <-
  encoded %>%
  mutate(
    shape = pmap(list(edge_shape),safe_decode_val)
  ) 

# Check for errors
decode_error <-
  decoded %>%
  pull(
    shape
  ) %>%
  set_names(nm = decoded$edge_id) %>%
  purrr::transpose() %>%
  pluck("error") %>%
  compact()

encoded %>% filter(edge_id == "2049706056392") %>% View()

# Create shape
decoded_proc <-
  decoded %>%
  hoist(.col = shape, "result", .remove = FALSE, .simplify = FALSE)  %>%
  select(-shape) %>%
  unnest(result) %>%
  sf::st_as_sf(
    .,
    coords = c("lng", "lat"),
    crs = 4326L, #WGS84
    agr = "constant",
    remove = FALSE
  ) %>%
  group_by(
    edge_shape, edge_id, edge_forward, edge_end_node, street_names
  ) %>%
  summarize(
    do_union = FALSE,
    .groups = "drop"
  ) %>%
  st_cast("LINESTRING")
      
decoded_proc %>%
  mutate(
    edge_id = as.character(edge_id),
    edge_id = fct_shuffle(edge_id)
  ) %>%
  mapview::mapview(zcol = "edge_id",legend = FALSE)

# Add in Rawnav -----------------------------------------------------------

rawnav36 <-
  read_csv(
    file.path(path_data,"rawnav_match_decomp_36.csv")
  )

# just to get a sense of total trips
count_trips <-
  rawnav36 %>%
  mutate(
    unique_trip = paste0(filename,"-",index_run_start)
  ) %>%
  summarize(
    n_distinct(unique_trip)
  )

edge_counts <-
  rawnav36 %>%
  mutate(
    unique_trip = paste0(filename,"-",index_run_start)
  ) %>%
  group_by(
    id
  ) %>%
  summarize(
    n = length(unique(unique_trip))
  )

# join 
used_edges <-
  decoded_proc %>%
  left_join(
    edge_counts,
    by = c("edge_id" = "id")
  ) %>%
  filter(
    n >= 1
  ) %>%
  arrange(
    n
  )

mapview::mapview(used_edges, zcol = "n", lwd = 3)

used_edges %>%
  filter(
    n > 35
  ) %>%
  mapview::mapview(zcol = "n", lwd = 3)


used_edges %>%
  filter(
    n > 300
  ) %>%
  mapview::mapview(zcol = "n", lwd = 3)

used_edges %>%
  filter(
    n > 300
  ) %>%
  write_sf("used_edges.geojson")
