match_segs_out_line <-
  match_segs_out %>%
  st_drop_geometry() %>%
  select(
    shape_id,
    seg_start,
    seg_end,
    stop_id_start,
    stop_id_end,
    id,
    way_id,
    latmatch,
    lonmatch
  ) %>%
  group_by(
    seg_start,
    seg_end,
    stop_id_start,
    stop_id_end,
    id,
    way_id,
  ) %>%
  mutate(
    n = if_else(row_number() == n(),2,1) 
  ) %>%
  uncount(weights = n, .remove = FALSE) %>%
  ungroup() %>%
  group_by(
    shape_id,
  ) %>%
  mutate(
    across(
      c(
        seg_start,
        seg_end,
        stop_id_start,
        stop_id_end,
        id,
        way_id
      ),
      ~ duper(.x)
    )
  ) %>%
  ungroup() %>%
  st_as_sf(
    .,
    coords = c("lonmatch", "latmatch"),
    crs = 4326L, #WGS84
    agr = "constant",
    remove = FALSE
  ) %>%
  group_by(
    shape_id,
    seg_start,
    seg_end,
    stop_id_start,
    stop_id_end,
    id,
    way_id,
  ) %>% 
  summarize(do_union = FALSE, .groups = "drop") %>%
  st_cast("LINESTRING")

write_sf(
  match_segs_out_line,
  dsn = file.path(
    path_sp,
    "Task 3 - Bus Priority",
    "Data",
    "01-Interim",
    "match_segs_out_line.geojson"
  )
)

stops <-
  make_gtfs_point(
    gtfs_prep,
    shape_id,
    stop_id
  )


write_sf(
  stops,
  dsn = file.path(
    path_sp,
    "Task 3 - Bus Priority",
    "Data",
    "01-Interim",
    "stop_patterns.geojson"
  )
)

