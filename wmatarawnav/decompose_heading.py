# -*- coding: utf-8 -*-
"""
Created on Wed Sep 29 13:01:57 2021

@author: JackMcDowell
"""

# Libraries
import pandas as pd
import numpy as np

# Reset heading so it doesn't wrap around at 0/360 degrees
def reset_heading(rawnav):
    # The idea here is to make the difference in between heading values always
    # fall between -180 and +180 degrees. For example, we want the change from 355 
    # to 5 degrees to be +10 degrees, not -350 degrees.
    
    # The resulting heading values from this process can easily be transformed 
    # back into their original values by using modulo 360.
    
    # Lag the heading values
    rawnav[['heading_lag']] = (
        rawnav
        .groupby(['filename','index_run_start'], sort = False)[['heading']]
        .transform(lambda x: x.shift(1))
    )
    
    # Calculate the difference in heading between records
    rawnav_reset_heading = (
        rawnav
        .assign(
            heading_marg = lambda x:
                np.where(
                        #If the difference is less than 180 degrees, do a normal difference
                        abs(x.heading - x.heading_lag) <= 180,
                        x.heading - x.heading_lag,
                        #If the difference is greater than 180, do one of two things:
                        np.where(
                            x.heading > x.heading_lag,
                            # If the heading is a larger value than the previous one, the change is actually negative. 
                            # Add 360 to the previous value so we get a difference that falls between -180 and 0.
                            x.heading - (x.heading_lag + 360),
                            # If the heading is a smaller value than the previous one, the change is actually positive.
                            # Just use modulo. Alternatively you could add 360 to the difference for the same result.
                            (x.heading - x.heading_lag)%360
                        )
                )
        )            
    )
    
    # Set the first heading_marg of each trip to just the heading
    rawnav_reset_heading.loc[rawnav_reset_heading.groupby(['filename','index_run_start']).head(1).index, 'heading_marg'] = rawnav_reset_heading.loc[rawnav_reset_heading.groupby(['filename','index_run_start']).head(1).index, 'heading']
    
    # Cumulatively sum the heading_marg column for a new heading column that doesn't wrap around at 0 and 360
    rawnav_reset_heading['heading_new'] = (
        rawnav_reset_heading
        .groupby(['filename','index_run_start'], sort = False)[['heading_marg']]
        .cumsum()        
    )
    
    return(rawnav_reset_heading)
    
    
    
# Decomposition
def decompose_heading(rawnav_speed, speed_col, heading_col):
    # Lag speed
    rawnav_speed[[speed_col+'_lag']] = (
        rawnav_speed
        .groupby(['filename','index_run_start'], sort = False)[[speed_col]]
        .transform(lambda x: x.shift(1))
    )
    
    
    # Identify where speed crosses zero
    heading_class = (
        rawnav_speed
        .assign(
            heading_group_id = lambda x:
                (x[speed_col].le(0) & x[speed_col+'_lag'].ge(0)) |
                (x[speed_col].ge(0) & x[speed_col+'_lag'].le(0))
        )
    )
            
    # Create column to ID groups in between the points where speed crosses zero
    heading_class['heading_group_id'] = (
        	heading_class
        	.groupby(['filename','index_run_start'], sort = False)[['heading_group_id']]
        	.transform(lambda x: x.cumsum())
        )
    
    
    # Measure the total heading change of each turn
    heading_class_groups = (
        heading_class
        .assign(ang_speed = lambda x: abs(x[speed_col]))
        .groupby(['filename','index_run_start','route_pattern','pattern','route',
                  'heading_group_id'])
        .agg(heading_start = (heading_col,'first'),
             heading_end = (heading_col,'last'),
             odom_start = ('odom_ft','first'),
             odom_end = ('odom_ft','last'),
             sec_start = ('sec_past_st','first'),
             sec_end = ('sec_past_st','last'),
             ang_speed_max = ('ang_speed','max'))
        .reset_index()
        .assign(heading_chg_deg = lambda x: x.heading_end - x.heading_start,
                turn_dist_ft = lambda x: x.odom_end - x.odom_start,
                turn_dur_sec = lambda x: x.sec_end - x.sec_start,
                ang_speed_avg = lambda x: abs(x.heading_chg_deg) / x.turn_dur_sec,
                turn_sharpness = lambda x: abs(x.heading_chg_deg) / x.turn_dist_ft)
        .drop(['heading_start','heading_end','odom_start','odom_end','sec_start','sec_end'],
              axis = "columns")
    )
    
    
    # Lag heading groups
    # Lag heading change
    heading_class_groups[['heading_chg_deg_lag']] = (
        heading_class_groups
        .groupby(['filename','index_run_start'], sort = False)[['heading_chg_deg']]
        .transform(lambda x: x.shift(1))
    )
    
    # Lag max angular speed
    heading_class_groups[['ang_speed_max_lag']] = (
        heading_class_groups
        .groupby(['filename','index_run_start'], sort = False)[['ang_speed_max']]
        .transform(lambda x: x.shift(1))
    )

    # Label turns, lane changes, and smaller curves
    heading_class_groups2 = (
        heading_class_groups
        .assign(actual_turn = lambda x: (abs(x.heading_chg_deg) >= 22.5) & 
                                        (x.ang_speed_max >= 1) & 
                                        (x.turn_sharpness >= 0.05),                
                lane_change = lambda x: (x.ang_speed_max >= 0.5) & 
                                        (x.ang_speed_max_lag >= 0.5) &
                                        (abs(x.heading_chg_deg + x.heading_chg_deg_lag) <= 2),
                lane_change_dir = lambda x: np.select(
                        # Use the lagged heading change value since the first turn
                        # of the lane change has the direction of the lane change
                        [x.lane_change & (x.heading_chg_deg_lag > 0),
                         x.lane_change & (x.heading_chg_deg_lag < 0)],
                        ["lanechg_right",
                         "lanechg_left"],
                         default = "no_lanechg"),
                slight_angle = lambda x: (x.ang_speed_max_lag >= 0.75) &
                                         (x.actual_turn != True) &
                                         (x.lane_change != True))
    )
        
    # Lead lane_change values
    heading_class_groups2[['lane_change_dir_lead']] = (
        heading_class_groups2
        .groupby(['filename','index_run_start'], sort = False)[['lane_change_dir']]
        .transform(lambda x: x.shift(-1))
    )
    
    # Collapse lane_change values
    heading_class_groups2 = (
        heading_class_groups2
        .assign(lane_change_new = lambda x: np.select(
                    [(x.lane_change_dir != "no_lanechg") & 
                         (x.lane_change_dir_lead == "no_lanechg"),
                     (x.lane_change_dir == "no_lanechg") & 
                         (x.lane_change_dir_lead != "no_lanechg"),
                     (x.lane_change_dir != "no_lanechg") & 
                         (x.lane_change_dir_lead != "no_lanechg"),
                     # The last value in a trip instance will have NA for the lead 
                     # value, we need to make sure this is marked correctly.
                     # Unfortunately this does not work... no idea why
                     pd.isna(x.lane_change_dir_lead)],
                    [x.lane_change_dir,
                     x.lane_change_dir_lead,
                     # If both values have a lane change, use the first one
                     x.lane_change_dir,
                     # NA case
                     x.lane_change_dir],
                     default = "no_lanechg"))
        .drop(['lane_change_dir_lead'],
              axis = "columns")
        )
    
    # Fill NA that appears in the final row for each trip instance
    heading_class_groups2['lane_change_new'] = (
        heading_class_groups2['lane_change_new']
        .fillna(value = heading_class_groups2.to_dict()['lane_change_dir'])
    )
    
    # Combine turning labels into one column
    heading_class_groups2 = (
        heading_class_groups2
        .assign(heading_decomp = lambda x: np.select(
                    [x.actual_turn & (x.heading_chg_deg > 0),
                     x.actual_turn & (x.heading_chg_deg < 0),
                     x.lane_change_new != "no_lanechg",
                     x.slight_angle & (x.heading_chg_deg > 0),
                     x.slight_angle & (x.heading_chg_deg < 0)],
                    ["turn_right",
                     "turn_left",
                     x.lane_change_new,
                     "angle_right",
                     "angle_left"],
                     default = "straight"))
    )
    
    
    # Join back to data
    heading_class_out = (
        heading_class_groups2
        .merge(heading_class,
               how = 'left',
               on = ['filename','index_run_start','route_pattern','pattern','route',
                  'heading_group_id'])
        .drop(['heading_chg_deg_lag','ang_speed_max_lag','actual_turn',
               'lane_change','lane_change_dir','slight_angle','lane_change_new'],
              axis = "columns")
    )
        
    return(heading_class_out)
    
    
    
    