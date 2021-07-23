library(tidyverse)
library(sf)
library(mapview)
library(plotly)

rawnav1 <-
  read_csv(
    "C:/Users/WylieTimmerman/Documents/projects_local/WMATA_AVL_datamart/test.csv",
    col_types = cols(
      X1 = col_double(),
      index_loc = col_double(),
      lat = col_double(),
      long = col_double(),
      heading = col_double(),
      door_state = col_character(),
      veh_state = col_character(),
      odom_ft = col_double(),
      sec_past_st = col_double(),
      sat_cnt = col_double(),
      stop_window = col_character(),
      blank = col_double(),
      lat_raw = col_double(),
      long_raw = col_double(),
      row_before_apc = col_double(),
      route_pattern = col_character(),
      pattern = col_double(),
      index_run_start = col_double(),
      index_run_end = col_double(),
      filename = col_character(),
      start_date_time = col_datetime(format = ""),
      route = col_character(),
      wday = col_character(),
      odom_ft_next = col_double(),
      sec_past_st_next = col_double(),
      odom_ft_next3 = col_double(),
      sec_past_st_next3 = col_double(),
      secs_marg = col_double(),
      odom_ft_marg = col_double(),
      fps_next = col_double(),
      fps_next3 = col_double()
    )
  ) %>%
  st_as_sf(
    ., 
    coords = c("long", "lat"),
    crs = 4326L, #WGS84
    agr = "constant"
  ) %>%
  mutate(
    mph_next = fps_next / 1.467,
    mph_next3 = fps_next3 / 1.467,
    fps_next_lead = lead(fps_next),
    fps_next3_lead = lead(fps_next3),
    accel_next = (fps_next_lead - fps_next) / (sec_past_st_next - sec_past_st),
    accel_next3 = (fps_next3_lead - fps_next3) / (sec_past_st_next3 - sec_past_st),
    accel_next = round(accel_next,3),
    accel_next3 = round(accel_next3,3),
    accel_mph_next = accel_next / 1.467,
    accel_mph_next3 = accel_next3 / 1.467
  )


g <- 
  ggplot(
    data = rawnav1
  ) +
  geom_point(
    aes(color = mph_next,
        y = odom_ft,
        x = sec_past_st)
  )

ggplotly(g)

# look at accel
g <- 
  ggplot(
    data = rawnav1
  ) +
  geom_point(
    aes(color = accel_mph_next3,
        y = odom_ft,
        x = sec_past_st)
  ) +
  scale_color_gradient2(
  )

ggplotly(g)


# check on the cases where weird shit happened
rawnav1 %>%
  select(
    index_loc,
    odom_ft,
    sec_past_st,
    fps_next,
    fps_next3,
    accel_next,
    accel_next3,
    door_state
  ) %>%
  View()


rawnav %>%
  arrange(veh_state) %>%
  mapview(.,zcol = 'veh_state')


# Version 1 reimport ------------------------------------------------------

rawnav1_reex <-
  read_csv(
    "C:/Users/WylieTimmerman/Documents/projects_local/WMATA_AVL_datamart/test_reexport.csv",
    col_types = cols(
      X1 = col_double(),
      index_loc = col_double(),
      lat = col_double(),
      long = col_double(),
      heading = col_double(),
      door_state = col_character(),
      veh_state = col_character(),
      odom_ft = col_double(),
      sec_past_st = col_double(),
      sat_cnt = col_double(),
      stop_window = col_character(),
      blank = col_double(),
      lat_raw = col_double(),
      long_raw = col_double(),
      row_before_apc = col_double(),
      route_pattern = col_character(),
      pattern = col_double(),
      index_run_start = col_double(),
      index_run_end = col_double(),
      filename = col_character(),
      start_date_time = col_datetime(format = ""),
      route = col_character(),
      wday = col_character(),
      odom_ft_next = col_double(),
      sec_past_st_next = col_double(),
      odom_ft_next3 = col_double(),
      sec_past_st_next3 = col_double(),
      secs_marg = col_double(),
      odom_ft_marg = col_double(),
      fps_next = col_double(),
      fps_next3 = col_double()
    )
  ) %>%
  st_as_sf(
    ., 
    coords = c("long", "lat"),
    crs = 4326L, #WGS84
    agr = "constant"
  ) %>%
  mutate(
    mph_next = fps_next / 1.467,
    mph_next3 = fps_next3 / 1.467,
    fps_next_lead = lead(fps_next),
    fps_next3_lead = lead(fps_next3),
    accel_next = (fps_next_lead - fps_next) / (sec_past_st_next - sec_past_st),
    accel_next3 = (fps_next3_lead - fps_next3) / (sec_past_st_next3 - sec_past_st),
    accel_next = round(accel_next,3),
    accel_next3 = round(accel_next3,3),
    accel_mph_next = accel_next / 1.467,
    accel_mph_next3 = accel_next3 / 1.467
  )
      

g <- 
  ggplot(
    data = rawnav1_reex
  ) +
  geom_point(
    aes(color = mph_next,
        y = odom_ft,
        x = sec_past_st)
  )

ggplotly(g)

# look at accel
g <- 
  ggplot(
    data = rawnav1_reex
  ) +
  geom_point(
    aes(color = accel_mph_next3,
        y = odom_ft,
        x = sec_past_st)
  ) +
  scale_color_gradient2(
  )

ggplotly(g)


# other stuff -------------------------------------------------------------


# check on speeds vs. veh_state

rawnav_veh_state <-
  rawnav %>%
  select(
    index_loc,
    odom_ft,
    sec_past_st,
    fps_next,
    fps_next3,
    row_before_apc,
    stop_window,
    veh_state,
    door_state
  ) 
View(rawnav_veh_state)

rm(rawnav,rawnav_veh_state)

# Let's look at another one

rawnav <-
  read_csv(
    "C:/Users/WylieTimmerman/Documents/projects_local/WMATA_AVL_datamart/test_3.csv",
    col_types = cols(
      X1 = col_double(),
      index_loc = col_double(),
      lat = col_double(),
      long = col_double(),
      heading = col_double(),
      door_state = col_character(),
      veh_state = col_character(),
      odom_ft = col_double(),
      sec_past_st = col_double(),
      sat_cnt = col_double(),
      stop_window = col_character(),
      blank = col_double(),
      lat_raw = col_double(),
      long_raw = col_double(),
      row_before_apc = col_double(),
      route_pattern = col_character(),
      pattern = col_double(),
      index_run_start = col_double(),
      index_run_end = col_double(),
      filename = col_character(),
      start_date_time = col_datetime(format = ""),
      route = col_character(),
      wday = col_character(),
      odom_ft_next = col_double(),
      sec_past_st_next = col_double(),
      odom_ft_next3 = col_double(),
      sec_past_st_next3 = col_double(),
      secs_marg = col_double(),
      odom_ft_marg = col_double(),
      fps_next = col_double(),
      fps_next3 = col_double()
    )
  )%>%
  st_as_sf(
    ., 
    coords = c("long", "lat"),
    crs = 4326L, #WGS84
    agr = "constant"
  )

mapview(rawnav,zcol = "fps_next3", stroke = FALSE)

