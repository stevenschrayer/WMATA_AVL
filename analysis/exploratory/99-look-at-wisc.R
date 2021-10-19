library(tidyverse)
library(sf)
library(mapview)
library(plotly)


# Functions ---------------------------------------------------------------

load_rawnav <- 
  function(thepath){
    suppressWarnings({
      read_csv(
        thepath,
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
      )
    })
    
  }


cleanup_things <- function(rawnav){
  rawnav %>%
    st_as_sf(
      ., 
      coords = c("long", "lat"),
      crs = 4326L, #WGS84
      agr = "constant"
    ) %>%
    mutate(
      mins_past_st = sec_past_st / 60,
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
}

plot_rawnav <-
  function(rawnav, var, thetitle = NULL){
    if (is.null(thetitle)){
      thetitle = rlang::as_label(enquo(var))
    }
    
    g <- 
      ggplot(
        data = rawnav
      ) +
      geom_line(
        aes(
          y = odom_ft,
          x = mins_past_st
        ),
        color = "grey80"
      ) +
      geom_point(
        aes(
          color = {{var}},
          y = odom_ft,
          x = mins_past_st
        )
      ) +
      xlab("Time Past Trip Start (Minutes)") +
      ylab("Trip Odometer Distance (Feet)") +
      scale_color_gradient2(
        low = "#d7191c",
        mid = "#ffffbf",
        high = "#1a9641"
        # low = ("red"),
        # mid = ("grey90"),
        # high = ("green")
        # low = scales::muted("red"),
        # mid = scales::muted("yellow"),
        # high = scales::muted("green")
      ) +
      theme_minimal() +
      guides(color=guide_legend(title=thetitle))
      
    
    # g
    ggplotly(g)
  }

plot_dots <-
  function(rawnav){
    
    g <- 
      ggplot(
        data = rawnav
      ) +
      geom_line(
        aes(
          y = odom_ft,
          x = sec_past_st
        ),
        color = "grey80"
      ) +
      geom_point(
        aes(
          y = odom_ft,
          x = sec_past_st
        )
      ) +
      xlab("Time Past Trip Start (Minutes)") +
      ylab("Trip Odometer Distance (Feet)") +
      theme_minimal() 
    # g
    ggplotly(g)
  }



# Look at individual ones -------------------------------------------------

# Original
rawnav1 <-
  load_rawnav(
    "C:/Users/WylieTimmerman/Documents/projects_local/WMATA_AVL_datamart/test.csv"
  ) %>%
  cleanup_things()

plot_rawnav(rawnav1,accel_mph_next3)

# With some cleaning
rawnav1_reex <-
  load_rawnav(
    "C:/Users/WylieTimmerman/Documents/projects_local/WMATA_AVL_datamart/test_reexport.csv"
  ) %>%
  cleanup_things()

plot_rawnav(rawnav1_reex,mph_next)    

plot_rawnav(rawnav1_reex,accel_mph_next3)    

# Another one

rawnav3 <-
  load_rawnav(
    "C:/Users/WylieTimmerman/Documents/projects_local/WMATA_AVL_datamart/test_3.csv"
  ) %>%
  cleanup_things()

plot_rawnav(rawnav3,mph_next)    

plot_rawnav(rawnav3,accel_mph_next3)    

# testcheckold
rawnav_tcold <-
  load_rawnav(
    "C:/Users/WylieTimmerman/Documents/projects_local/WMATA_AVL_datamart/testcheckold.csv"
  ) %>%
  mutate(
    mins_past_st = sec_past_st / 60,
    sec_past_st_next = lead(sec_past_st),
    odom_ft_next = lead(odom_ft),
    fps_next = (odom_ft_next - odom_ft) / (sec_past_st_next - sec_past_st),
    fps_next_lag = lag(fps_next),
    sec_past_st_lag = lag(sec_past_st),
    accel = (fps_next - fps_next_lag) / (sec_past_st_next - sec_past_st_lag)
  )

plot_dots(rawnav_tcold)    
plot_rawnav(rawnav_tcold, accel, "Acceleration (fps^2)")
   
# fixed
rawnav_tfixed <-
  load_rawnav(
    "C:/Users/WylieTimmerman/Documents/projects_local/WMATA_AVL_datamart/testcheckfixed.csv"
  ) %>%
  mutate(
    mins_past_st = sec_past_st/60
  )

plot_rawnav(rawnav_tfixed,accel_next)    

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

# other -------------------------------------------------------------------

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

mapview(rawnav,zcol = "fps_next3", stroke = FALSE)

# -------------------------------------------------------------------------
# now that we've fixed accel/decel, diving further into the next steps

rawnav_tfixed4 <-
  load_rawnav(
    "C:/Users/WylieTimmerman/Documents/projects_local/WMATA_AVL_datamart/test_checkfixed4.csv"
  ) %>%
  mutate(
    mins_past_st = sec_past_st / 60,
    accel_next_mph = accel_next / 1.467
  )

plot_rawnav(rawnav_tfixed4,accel_next_mph)    

plot_rawnav(rawnav_tfixed4,fps_next)    

# What is the speed we shou

