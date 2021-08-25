# sketch out possible stop area decompositions
library(tidyverse)


vec_passengers <-
  c("pax","nopax")

vec_movestate <-
  c("move","stop")

vec_doorstate <-
  c("open","close")

vec_relative <-
  c("pre","at","post","a")

stop_area_decomps_raw <-
  purrr::cross_df(
    list(
      anypax = vec_passengers,
      movestate = vec_movestate,
      doorstate = vec_doorstate,
      relative = vec_relative
    )
  )

stop_area_decomps_marked <-
  stop_area_decomps_raw %>%
  # mark not possible combos
  mutate(
    possible = 
      case_when(
        anypax == "nopax" & (
          (relative %in% c("pre","post","at")) |
            (doorstate == "open")
        ) ~ FALSE,
        anypax == "pax" & doorstate == "open" & (
          relative %in% c("pre","a")
        ) ~ FALSE,
        anypax == "pax" & (relative %in% c("a")) ~ FALSE,
        doorstate == "open" & movestate == "move" ~ FALSE,
        (relative %in% ("at")) & (doorstate == "close") ~ FALSE,
        TRUE ~ TRUE
      )
  )
  
  
stop_area_decomps <-
  stop_area_decomps_marked %>%
  filter(
    possible
  )
