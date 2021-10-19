# Since we have some good R tools for gtfs, we're going to use this to 
# extract the 'main' path for each route pattern in oct 2017 and 2019 on the 
# H&I streets. 

library(tidyverse)
library(sf)
library(mapview)
library(httr)
library(gtfsec)
library(tidytransit)
library(smoothr)
options(scipen = 999)
path_sp <- "C:/OD/Foursquare ITP/Projects - WMATA Datamart"

# Key params
crs_wmata <- 6488L #maryladn

routes_hi <-
  c('30N','30S','32','33','36','37','39','42','43','G8')

years_hi <- 
  c(
    # 2017#, #2017 turned out to have crap shapes
    2019
  )

# the function
mapmatch <-
  function(df){
    df_proce <-
      df %>%
      st_transform(4326L) %>%
      dplyr::bind_cols(
        .,
        as.data.frame(sf::st_coordinates(.,geometry))
      ) %>%
      dplyr::rename(
        'lon' = X,
        'lat' = Y
      ) 
    
    use_df <-
      df_proce %>%
      sf::st_drop_geometry() %>%
      select(
        lat,
        lon
      )
    
    use_data <-
      list(
        shape = use_df,
        costing = "bus",
        shape_match = "map_snap",
        directions_options =
          list(
            units = "miles",
            directions_type = "none"
          ),
        trace_options.search_radius = 100,
        trace_options.interpolation_distance = 100,
        trace_options.turn_penalty_factor = 500
      )
    
    url_base = "http://localhost:8002/trace_attributes"
    res <-
      httr::POST(
        url = url_base,
        httr::add_headers(
          "Content-Type" = "application/json"
        ),
        body = jsonlite::toJSON(use_data, auto_unbox = TRUE)
      )
    
    cont <- content(res)
    
    httr::stop_for_status(res)
    
    edges <-
      cont$edges %>%
      enframe() %>%
      unnest_wider(value)  %>%
      rowwise() %>%
      mutate(
        street_names = paste(unlist(names),collapse = ", "),
        .before = everything()
      ) %>%
      ungroup() %>%
      mutate(
        edge_index = row_number() - 1,
        .before = everything()
      ) %>%
      select(-end_node, -names) 
    
    
    matched <-
      cont$matched_points %>%
      enframe() %>%
      unnest_wider(value) %>%
      rename(
        lonmatch = lon,
        latmatch = lat
      )
    
    df_out <-
      df_proce %>%
      bind_cols(
        matched
      ) %>%
      left_join(
        edges,
        by = c("edge_index")
      ) %>%
      st_as_sf(
        .,
        coords = c("lonmatch", "lat"),
        crs = 4326L, #WGS84
        agr = "constant",
        remove = FALSE
      )
    
    return(df_out)
  }


# Iterate -----------------------------------------------------------------

shapes <- map(years_hi,function(ayear){
  gtfs_obj <-
    tidytransit::read_gtfs(
      file.path(
        path_sp,
        "Task 3 - Bus Priority",
        "Data",
        "00-Raw",
        paste0("gtfs-wmata-oct",ayear,".zip")
      )
    )
  
  gtfs_obj$routes <- 
    gtfs_obj$routes %>%
    filter(route_short_name %in% routes_hi)
  
  gtfs_prep <-
    prepare_gtfs(
      gtfs_obj
    )
  
  shapes <-
    make_gtfs_line(
      gtfs_prep,
      route_short_name,
      shape_id
    )
  
  # this package doesn't seem to do anything...
  shapes_den <-
    shapes %>%
    smoothr::densify(max_distance = .25)

  shapes_den_exp <-
    shapes_den %>%
    st_cast("POINT")
  
  shapes_match_out <-
    shapes_den_exp %>%
    group_by(shape_id) %>%
    group_map(~mapmatch(.x),.keep = TRUE) %>%
    reduce(bind_rows) %>%
    mutate(theyear = ayear)
  
}) %>%
  reduce(rbind)

shapes_out <- 
  shapes %>%
  select(
    where(~ !is.list(.x))
  )

shapes_dissolve <-
  shapes_out %>%
  distinct(
    route = route_short_name,
    id
  ) %>%
  mutate(
    isvalid = TRUE
  )
  
# write_sf(
#   shapes_out,
#   dsn = 
#     file.path(
#       path_sp,
#       "Task 3 - Bus Priority",
#       "Data",
#       "01-interim",
#       "matched_pattern_shapes_points_hi.geojson"
#     )
# )


write_sf(
  shapes_dissolve,
  dsn =
    file.path(
      path_sp,
      "Task 3 - Bus Priority",
      "Data",
      "01-interim",
      "valid_pattern_ids_hi.geojson"
    )
)

