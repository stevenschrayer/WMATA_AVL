library(tidyverse)
library(googlePolylines)
library(sf)
library(mapview)

path_poly <-
  "C:/OD/Foursquare ITP/Projects - WMATA Datamart/Task 3 - Bus Priority/Data/01-Interim/planet_extract.polyline"

planet_lines <-
  readLines(path_poly)

planet_sf <-
  googlePolylines::decode(planet_lines)
# this seems wrong, not quite the right latlong


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
  
  decoded <- tibble::tibble(lat = lats[2:length(lats)]/1000000,
                            lng = lons[2:length(lons)]/1000000)
  
  return (decoded)
}

# wow that function stinks, but at least the lat/long is correct?
planet_val_sf <-
  readLines(path_poly) %>%
  head() %>%
  map(decode_val)
  st_as_sf(
    .,
    coords = c("lng", "lat"),
    crs = 4326L, #WGS84
    agr = "constant",
    remove = FALSE
  )
      

# how does geojson work, if at all?

path_poly <-
  "C:/OD/Foursquare ITP/Projects - WMATA Datamart/Task 3 - Bus Priority/Data/01-Interim/planet_extract.polyline"
# oh, this is literally just a polyline with a different exptension

#valhalla_export_edges --column , --row \n --config /custom_files/valhalla.json > /custom_files/wmata_reexport2.polyline
path_v2 <-
  "C:/Users/WylieTimmerman/Documents/projects_local/wmata-valhalla-docker/custom_files_wmata309/wmata_reexport2.polyline"

planet_sf2 <-
  readLines(path_v2) %>%
  googlePolylines::decode()
# this just breaks 
# Error in rcpp_decode_polyline(polylines, "coords") : 
# basic_string::at: __n (which is 101) >= this->size() (which is 101)

# i think this is a slightly older version of the original code
# https://git.inter-media.net/valhalla/valhalla/commit/e42862ecf3eeb56f88d1f5bce34315832e19a3e9


# maybe you need to modify code to get IDs?
# https://github.com/valhalla/valhalla/issues/1584
