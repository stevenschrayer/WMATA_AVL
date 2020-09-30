
library(tidyverse)
library(lubridate)
library(glue)
library(dbplyr)
library(RJDBC)
library(janitor)
library(testthat)
library(janitor)

source(here::here("analysis", "bus-lane-analysis", "planapi_functions.R"))


# Parameters --------------------------------------------------------------

param_month <- "2019-10-01"
param_route <- "S4"

con <- connect_planapi(jar_path = stin_jar())

# Get list of values from Bus State ---------------------------------------

## In order to filter out TSP logs that are unneeded, we use the raw Bus State 
## data to filter to just the buses that ran the specified route. We then
## create a filename that matches the format of the TSP logs.

## Query Bus State Raw Data

query_start <- floor_date(ymd(param_month), unit = "month") %>%
  to_oracle_date()

## Bus State data gets downloaded when the bus reconnects at the garage. This
## doesn't always happen the same day, and there are some flukes where
## data might sit on the bus for for a long time. This bleed of 14 days after
## makes sure we get all the data for the month.

query_end <- (ceiling_date(ymd(param_month), unit = "month") + ddays(14)) %>%
  to_oracle_date()

bus_state_query <- glue("select *
from BUS_STATE_RAW_V
where filedate between '{query_start}' and '{query_end}' and route_id like '{route}%'",
                        route = param_route,
                        query_start = query_start,
                        query_end = query_end)

bus_state_filtered <- dbGetQuery(con, bus_state_query) %>%
  as_tibble()

## Create Filename

bus_state_ID_date <- bus_state_filtered %>%
  clean_names() %>%
  mutate(event_time_parse = ymd_hms(event_dtm)) %>%
  #this is the format of the TSP Logs (.txt.zip)
  mutate(date_log_format = glue("rawnav{bus_id}{year}{month}{day}.txt.zip",
                                bus_id = str_pad(bus_id, 5, side = "left", pad = "0"),
                                year = str_sub(filedate, 3, 4),
                                month = str_sub(filedate, 6, 7),
                                day = str_sub(filedate, 9, 10))) %>%
  distinct(date_log_format)

write_csv(bus_state_ID_date, here::here("analysis", "bus-lane-analysis", glue::glue("{param_month}-{param_route}-busstate_file_IDs.csv")))
