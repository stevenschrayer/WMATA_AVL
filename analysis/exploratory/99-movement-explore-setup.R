#*******************************************************************************
#PROJECT:       WMATA DATAMART
#DATE CREATED:  Wed Aug 04 05:47:43 2021
#TITLE:         Defines functions and things to be used in noteboosk
#AUTHOR:        Wylie Timmerman (wtimmerman@foursquareitp.com
#NOTES:       
#*******************************************************************************


library(tidyverse)
library(sf)
library(mapview)
library(plotly)

path_sp <- "C:/OD/Foursquare ITP/Projects - WMATA Datamart"

path_data <-
  file.path(
    path_sp,
    "Task 3 - Bus Priority",
    "Data",
    "01-Interim"
  )

options(scipen = 999)

knitr::opts_chunk$set(echo = FALSE)

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
  df <- 
  rawnav %>%
    group_by(filename, index_run_start) %>%
    mutate(
      mins_past_st = sec_past_st / 60
    )
  
  if (!all(c('fps_next','accel_next') %in% colnames(df))){
    df <-
      df %>%
      mutate(
        sec_past_st_next = lead(sec_past_st),
        odom_ft_next = lead(odom_ft),
        fps_next = (odom_ft_next - odom_ft) / (sec_past_st_next - sec_past_st),
        fps_last = lag(fps_next),
        accel_next = (fps_next - fps_last) / (sec_past_st_next - sec_past_st),
        thelabel = "",
        mph_next = fps_next / 1.467
      )
  }
  
  df <-
    df %>% 
    ungroup()
  
  if ("steady_fps" %in% colnames(df)){
    df <-
      df %>%
      mutate(
        thelabel = 
          paste0(
            "steady fps: ", round(steady_fps,1),
            "<br>fps: ", round(fps_next,1),
            "<br>accel: ",round(accel_next,1)
          )
      )
  } else {
    df <-
      df %>%
      mutate(
        thelabel = 
          paste0(
            "fps: ", round(fps_next,1),
            "<br>accel: ",round(accel_next,1)
          )
      )
  }
  
  return(df)
  
}

filter_range <- function(df,themin,themax){
  df <- 
    df %>%
    filter(
      (odom_ft >= themin) & (odom_ft <= themax)
    )
  
  if (nrow(df) < 2){
    stop('range of filter values too narrow')
  }
  
  return(df)
}

plot_rawnav <-
  function(rawnav, var, thetitle = NULL,odom_min = NULL,odom_max = NULL){
    # setup
    if (is.null(thetitle)){
      thetitle = rlang::as_label(enquo(var))
    }
    
    if (is.environment(rawnav)){
      thedata <- rawnav$data()
    } else {
      thedata <- rawnav
    }
    
    varclass <- thedata %>% pull({{var}}) %>% class()
    
    # Define basic time-space diagram
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
          color = {{var}},
          y = odom_ft,
          x = sec_past_st
        )
      ) +
      xlab("Time Past Trip Start (Seconds)") +
      ylab("Trip Odometer Distance (Feet)") +
      theme_minimal()
    
    # set colors
    if (all(varclass == "numeric")){
      g <-
        g +
        scale_color_gradient2(
          low = "#d7191c",
          mid = "#ffffbf",
          high = "#1a9641"
        ) +
        guides(color=guide_legend(title=thetitle))
      
    } else if (rlang::as_label(enquo(var)) == "basic_decomp"){
      g <-
        g +
        scale_color_manual(
          values = c(
            "steady" = "#80b1d3",
            "decel" = "#ffffb3",
            "stopped" = "#fb8072",
            "accel" = "#8dd3c7",
            "other_delay" = "#bebada"
          )
        ) +
        guides(color=guide_legend(title=thetitle))
      
    } else if (any("factor" %in% varclass)) {
      g <- 
        g + 
        scale_color_brewer(
          type = "div",
          palette = "RdYlGn",
          drop = FALSE
        ) +
        guides(color=guide_legend(title=thetitle))
    }
    
    # convert to ggplotly
    gplot <- 
      ggplotly(
        g, 
        dynamicTicks = TRUE, 
        tooltip = c("x","y","colour","text")
      ) 
    
    # set bounds
    if (!is.null(odom_min) & !is.null(odom_max)){
      sec_range <-
        thedata %>% # because we expect weird thing
        filter_range(odom_min,odom_max) %>%
        pull(sec_past_st) %>%
        range()
      
      # add a little room
      inter_sec_range <- sec_range[2] - sec_range[1]
      sec_range[1] <- sec_range[1] - inter_sec_range * .25
      sec_range[2] <- sec_range[2] + inter_sec_range * .25
      
      # odom _range
      inter_odom_range <- odom_max - odom_min
      odom_min <- odom_min - inter_odom_range * .25
      odom_max <- odom_max + inter_odom_range * .25
      
      gplot <-
        gplot %>%
        layout(
          yaxis = 
            list(
              range = c(odom_min, odom_max),
              autorange = FALSE
            ),
          xaxis = 
            list(
              range = sec_range,
              autorange = FALSE
            )
        )
    }
    
    return(gplot)
    
  }
