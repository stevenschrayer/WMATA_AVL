preview_match <-
  function(
    atripfile,
    atripidx
  ){
    rawnav_og_ti <-
      rawnav_og %>%
      filter(
        filename == atripfile,
        index_run_start == atripidx
      )
    
    rawnav_matched_ti <-
      rawnav_matched %>%
      filter(
        filename == atripfile,
        index_run_start == atripidx
      )
    
    mapview(
      rawnav_og_ti,
      col.regions = "grey30",
      alpha = 0,
      layer.name = "Original"
    ) +
      mapview(
        rawnav_matched_ti,
        col.regions = "red",
        layer.name = "Matched",
        alpha = 0
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

#starts at 6469 in original file
rawnav %>%
  filter(
    filename == "rawnav02547171017.txt",
    index_run_start == 5735
  ) %>%
  View()

rawnav %>%
  filter(
    filename == "rawnav02547171017.txt",
    index_run_start == 5735
  ) %>%
  st_as_sf(
    .,
    coords = c("long", "lat"),
    crs = 4326L, #WGS84
    agr = "constant"
  ) %>%
  mapview::mapview(zcol = "odom_ft_og")
