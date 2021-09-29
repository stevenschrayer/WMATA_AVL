# -*- coding: utf-8 -*-
"""
Create by: jmcdowell
Purpose: Identify a set of classes that differentiate bus heading cases
Created on Wed Aug 04 2021
"""

# %% Load libraries

# Libraries
import os, sys, pandas as pd, pyarrow.parquet as pq, numpy as np
from dotenv import dotenv_values
import plotly.express as px
import plotly.io as pio
pio.renderers.default='browser'


# %% Set paths

# Paths
if os.getlogin() == "WylieTimmerman":
    # Working Paths
    path_working = r"C:\Users\WylieTimmerman\Documents\projects_local\WMATA_AVL_datamart"
    os.chdir(os.path.join(path_working))
    sys.path.append(r"C:\Users\WylieTimmerman\Documents\projects_local\WMATA_AVL_datamart")
    path_sp = r"C:\OD\Foursquare ITP\Projects - WMATA Datamart\Task 3 - Bus Priority"
    path_source_data = os.path.join(path_sp,"Data","00-Raw")
    path_processed_data = os.path.join(path_sp, "Data", "02-Processed") 
elif os.getlogin() == "JackMcDowell":
    os.environ["GDAL_DATA"] = os.environ["CONDA_PREFIX"] + "\Library\share\gdal"
    path_working = r"C:\OD\OneDrive - Foursquare ITP\Documents\0002 R\WMATA Datamart\WMATA_AVL"
    os.chdir(os.path.join(path_working))
    sys.path.append(r"C:\OD\OneDrive - Foursquare ITP\Documents\0002 R\WMATA Datamart\WMATA_AVL")
    path_sp = r"C:\OD\Foursquare ITP\Projects - WMATA Datamart\Task 3 - Bus Priority"
    path_source_data = os.path.join(path_sp,"data","00-Raw")
    path_processed_data = os.path.join(path_sp, "data","02-Processed")
else:
    raise FileNotFoundError("Define the path_working, path_source_data, gtfs_dir, \
                            ZippedFilesloc, and path_processed_data in a new elif block")


# Server credentials
config = dotenv_values(os.path.join(path_working, '.env'))

# Globals
# q_jump_route_list = ['30N','30S','33','31']
analysis_routes = ['30N']
analysis_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
wmata_crs = 2248

# Load wmatarawnav library
import wmatarawnav as wr


# %% Read in rawnav data for Wisconsin Ave corridor

rawnav_raw = pd.DataFrame()

for yr in [
    '202102',
    # '202103',
    # '202104',
    # '202105'
]:
    rawnav_raw_temp = (
        wr.read_cleaned_rawnav(
            analysis_routes_= analysis_routes,
            analysis_days_ = analysis_days,
            path = os.path.join(
                path_processed_data,
                ("rawnav_data_" + yr + ".parquet")
            )
        )
        .drop(
            [
                # "heading",
                "sat_cnt",
                "blank",
                "lat_raw",
                "long_raw",
                "index_run_end",
                "wday",
                # "veh_state",
                # "pattern", #keep pattern
                "start_date_time"
            ],
            axis = "columns"
        )
    )

    rawnav_raw = rawnav_raw.append(rawnav_raw_temp, ignore_index = True)

del rawnav_raw_temp 


# %% stop index data
stop_index = (
    pq.read_table(source=os.path.join(path_processed_data,"stop_index.parquet"),
                    columns = [ 'route',
                                'pattern',
                                'direction', #same as pattern_name_wmata_schedule
                                'stop_id',
                                'filename',
                                'index_run_start',
                                'index_loc',
                                'odom_ft',
                                'sec_past_st',
                                'geo_description'],
                    use_pandas_metadata = True
    )
    .to_pandas()
    # As a bit of proofing, we confirm this is int32 and not string, may remove later
    .assign(pattern = lambda x: x.pattern.astype('int32')) 
    .rename(columns = {'odom_ft' : 'odom_ft_stop'})
    .reset_index()
)

stop_summary = (
    pq.read_table(
        source = os.path.join(path_processed_data,"stop_summary.parquet"),
        columns = [
            'filename',
            'index_run_start',
            'direction_wmata_schedule',
            'pattern_destination_wmata_schedule',
            'route',
            'pattern',
            'start_date_time',
            'end_date_time'
        ],
        use_pandas_metadata = True
    )
    .to_pandas()
    # As a bit of proofing, we confirm this is int32 and not string, may remove later
    .assign(pattern = lambda x: x.pattern.astype('int32')) 
    .reset_index()
)
    
    
# %% Join Datasets
# At this point, if the odom reading of that nearest point is found in multiple places 
# (sometimes odom reading will repeat), some stops will show up twice. I think this is okay 
# for the calculations that follow.

rawnav_fil = pd.DataFrame()

for rt in analysis_routes:
    print(rt)
    rawnav_rt = rawnav_raw.query('route == @rt')
    
    rawnav_fil_rt = (
        rawnav_rt
        .merge(
            stop_index
            # TODO: join on index_loc as well
            .filter(items = ['filename','index_run_start','odom_ft_stop','stop_id']),
            left_on = ['filename','index_run_start','odom_ft'],
            right_on = ['filename','index_run_start','odom_ft_stop'],
            how = "left"
        )
    )
    
    rawnav_fil = pd.concat([rawnav_fil,rawnav_fil_rt])
    
del rawnav_raw
del rawnav_rt
del rawnav_fil_rt


# %% Identify the stop window

rawnav_window = (
    wr.assign_stop_area(
        rawnav_fil,
        stop_field = "stop_id",
        upstream_ft = 150,
        downstream_ft = 150
    )
)

del rawnav_fil


# %% Reset headings so they never loop back around zero

# The idea here is to make the difference in between heading values always
# fall between -180 and +180 degrees. For example, we want the change from 355 
# to 5 degrees to be +10 degrees, not -350 degrees.

# The resulting heading values from this process can easily be transformed 
# back into their original values by using modulo 360.

# Lag the heading values
rawnav_window[['heading_lag']] = (
    rawnav_window
    .groupby(['filename','index_run_start'], sort = False)[['heading']]
    .transform(lambda x: x.shift(1))
)

# Calculate the difference in heading between records
rawnav_reset_heading = (
    rawnav_window
    .assign(
        heading_chg = lambda x:
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

# Set the first heading_chg of each trip to just the heading
rawnav_reset_heading.loc[rawnav_reset_heading.groupby(['filename','index_run_start']).head(1).index, 'heading_chg'] = rawnav_reset_heading.loc[rawnav_reset_heading.groupby(['filename','index_run_start']).head(1).index, 'heading']

# Cumulatively sum the heading_chg column for a new heading column that doesn't wrap around at 0 and 360
rawnav_reset_heading['heading_new'] = (
    rawnav_reset_heading
    .groupby(['filename','index_run_start'], sort = False)[['heading_chg']]
    .cumsum()        
)

rawnav_reset_heading = rawnav_reset_heading[rawnav_reset_heading.index_run_start == 14528]

# %% Prepare data for speed calculation

# Replace the heading column with the new heading values
rawnav_heading = rawnav_reset_heading.assign(heading = rawnav_reset_heading[['heading_new']])

# aggregate so we only have one observation for each second - this uses the 'last' heading observation
rawnav_heading2 = agg_sec(rawnav_heading)

# quick check of how many pings we will have repeated seconds values
(rawnav_heading2.shape[0] - rawnav_heading.shape[0]) / rawnav_heading.shape[0]

# This is a separate step again; there are some places where we'll want to interpolate that
# aren't just the ones where we aggregated seconds. In general, we'll probably want to 
# revisit the aggregation/interpolation process, so I'm going to try not to touch this too much
# more for now.
# this works:
rawnav_heading3 = interp_column_over_sec(rawnav_heading2, 'heading')


# %% Plot sample data
    
heading_sample = (
    rawnav_heading3
    .query('filename == "rawnav07225210305.txt"')
    .assign(stop_zone = lambda x: 
                np.where(
                    x.stop_window_area.isna(),
                    "no",
                    "stop_area"
                ),
            stop_zone_stop = lambda x:
                np.where(
                    x.stop_id.isna(),
                    x.stop_zone,
                    "stop"
                )
    )
)

fig = px.scatter(x = heading_sample.odom_ft, 
                 y = heading_sample.heading,
                 color = heading_sample.stop_zone_stop)
fig.show()



# %% Smooth heading values

rawnav_heading_sm = smooth_rawnav_column(rawnav_heading3,
                                         smooth_col = 'heading',
                                         window_size = 11)

# %% Plot sample data
    
heading_sample_sm = (
    rawnav_heading_sm
    .assign(stop_zone = lambda x: 
                np.where(
                    x.stop_window_area.isna(),
                    "no",
                    "stop_area"
                ),
            stop_zone_stop = lambda x:
                np.where(
                    x.stop_id.isna(),
                    x.stop_zone,
                    "stop"
                )
    )
)

fig = px.scatter(x = heading_sample_sm.odom_ft, 
                 y = heading_sample_sm.heading_sm,
                 color = heading_sample_sm.stop_zone_stop)
fig.show()


# %% Calculate angular speed

# these are not the rolling vals, though i think we will want to include those before long.
rawnav_heading4 = calc_rawnav_speed(rawnav_heading_sm, 'heading_sm')


# %% Plot sample data

speed_sample = (
    rawnav_heading4
    .assign(stop_zone = lambda x: 
                np.where(
                    x.stop_window_area.isna(),
                    "no",
                    "stop_area"
                ),
            stop_zone_stop = lambda x:
                np.where(
                    x.stop_id.isna(),
                    x.stop_zone,
                    "stop"
                )
    )
)

fig = px.scatter(x = speed_sample.odom_ft, 
                 y = speed_sample.heading_sm_speed_next,
                 color = speed_sample.stop_zone_stop)
fig.show()


# %% Smooth speed

rawnav_speed_smooth = smooth_rawnav_column(rawnav_heading4,
                                           smooth_col = 'heading_sm_speed_next',
                                           window_size = 11)


# %% Visualize sample again

speed_sample_smooth = (
    rawnav_speed_smooth
    .assign(stop_zone = lambda x: 
                np.where(
                    x.stop_window_area.isna(),
                    "no",
                    "stop_area"
                ),
            stop_zone_stop = lambda x:
                np.where(
                    x.stop_id.isna(),
                    x.stop_zone,
                    "stop"
                )
    )
)

fig = px.scatter(x = speed_sample_smooth.odom_ft, 
                 y = speed_sample_smooth.heading_sm_speed_next_sm,
                 color = speed_sample_smooth.stop_zone_stop)
fig.show()


# %% Calculate acceleration

rawnav_accel = calc_rawnav_accel(rawnav_speed_smooth, 'heading_sm_speed_next_sm')



# %% Visualize acceleration

accel_sample = (
    rawnav_accel
    .assign(stop_zone = lambda x: 
                np.where(
                    x.stop_window_area.isna(),
                    "no",
                    "stop_area"
                ),
            stop_zone_stop = lambda x:
                np.where(
                    x.stop_id.isna(),
                    x.stop_zone,
                    "stop"
                )
    )
)

fig = px.scatter(x = accel_sample.odom_ft, 
                 y = accel_sample.deg_accel_next,
                 color = accel_sample.stop_zone_stop)
fig.show()


# Conclusion: Acceleration is not helpful.


# %% Determine categories
    
# Lag smoothed speed
rawnav_speed_smooth[['heading_sm_speed_next_sm_lag']] = (
    rawnav_speed_smooth
    .groupby(['filename','index_run_start'], sort = False)[['heading_sm_speed_next_sm']]
    .transform(lambda x: x.shift(1))
)


# Identify where speed crosses zero
heading_class = (
    rawnav_speed_smooth
#    .query('filename == "rawnav07225210305.txt"')
    .assign(
        heading_chg_dir = lambda x:
            (x.heading_sm_speed_next_sm.le(0) & x.heading_sm_speed_next_sm_lag.ge(0)) |
            (x.heading_sm_speed_next_sm.ge(0) & x.heading_sm_speed_next_sm_lag.le(0))
    )
)
        
# Create column to ID groups in between the points where speed crosses zero
heading_class['heading_chg_dir'] = (
    	heading_class
    	.groupby(['filename','index_run_start'], sort = False)[['heading_chg_dir']]
    	.transform(lambda x: x.cumsum())
    )


# Classify speeds into positive, negative, and near-zero
# Positive is turning to the right
# Negative is turning to the left
heading_class = (
    heading_class
    .assign(
        heading_decomp = lambda x: 
            np.select(
                [x.heading_sm_speed_next_sm >= 0.5,
                 x.heading_sm_speed_next_sm <= -0.5],
                ["right_turn",
                 "left_turn"],
                 default = pd.NA
            ),
        stop_zone = lambda x: 
                np.where(
                    x.stop_window_area.isna(),
                    "no",
                    "stop_area"
                ),
            stop_zone_stop = lambda x:
                np.where(
                    x.stop_id.isna(),
                    x.stop_zone,
                    "stop"
                )
    )
)

# Fill out NAs with right/left/straight values
heading_class['heading_decomp'] = (
        heading_class
        .groupby(['filename','index_run_start','heading_chg_dir'])[['heading_decomp']]
        .transform(lambda x: x.ffill().bfill())
        .fillna(value = "straight")
    )

# %% Visualize

# Color speed diagram
fig = px.scatter(x = heading_class.sec_past_st, 
                 y = heading_class.heading_sm_speed_next_sm,
                 color = heading_class.heading_decomp)
fig.show()

# Color heading diagram
fig = px.scatter(x = heading_class.sec_past_st, 
                 y = heading_class.heading_sm,
                 color = heading_class.heading_decomp)
fig.show()

# Color lat/long map
fig = px.scatter(x = heading_class.long, 
                 y = heading_class.lat,
                 color = heading_class.heading_decomp)
fig.show()
    
# Pull into stop
# - Positive change in heading (right turn)
# - Touches a stop zone
# - Occurs before the stop itself (door open)

# Pull out of stop
# - Negative change in heading (left turn)
# - Touches a stop zone
# - Occurs after the stop itself (door open)

# Change lanes
# - positive speed followed by negative speed, or vice versa
# - net zero heading change after the two movements

# Turn at intersection
# - Positive speed is always right, negative speed is always left
# - sharpness (degrees/distance) is higher
# - Speed >= 1
# - Sharpness >= 0.05
# - Total Angle >= 15


# Drive straight
# - select a threshold that is considered near-zero speed
# - however, curves in the road should be considered driving "straight"
# - sharpness (degrees/distance) is lower


# At stop (door open?)


# %% Next round of heading decomp

# Measure the total heading change of each turn
heading_class_groups = (
    heading_class
    .assign(ang_speed = lambda x: abs(x.heading_sm_speed_next_sm))
    .groupby(['filename','index_run_start','route_pattern','pattern','route',
              'heading_chg_dir','heading_decomp'])
    .agg(heading_start = ('heading_sm','first'),
         heading_end = ('heading_sm','last'),
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

# %% Join back to data and visualize
    
heading_class2 = (
    heading_class_groups
    .merge(heading_class,
           how = 'left',
           on = ['filename','index_run_start','route_pattern','pattern','route',
              'heading_chg_dir','heading_decomp'])
)

# Color lat/long map
fig = px.scatter(x = heading_class2.long, 
                 y = heading_class2.lat,
                 color = heading_class2.heading_chg_deg)
fig.show()


# %% Start assigning labels

# Lag heading groups
heading_class_groups[['heading_chg_deg_lag']] = (
    heading_class_groups
    .groupby(['filename','index_run_start'], sort = False)[['heading_chg_deg']]
    .transform(lambda x: x.shift(1))
)

heading_class_groups[['ang_speed_max_lag']] = (
    heading_class_groups
    .groupby(['filename','index_run_start'], sort = False)[['ang_speed_max']]
    .transform(lambda x: x.shift(1))
)

# %% Assign labels
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
    .assign(combined = lambda x: np.select(
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


# %% Join back to data and visualize

heading_class3 = (
    heading_class_groups2
    .merge(heading_class,
           how = 'left',
           on = ['filename','index_run_start','route_pattern','pattern','route',
              'heading_chg_dir','heading_decomp'])
)

# Color lat/long map
fig = px.scatter(x = heading_class3.long, 
                 y = heading_class3.lat,
                 color = heading_class3.combined)
fig.show()

## Color lat/long map
#fig = px.scatter(x = heading_class3.long, 
#                 y = heading_class3.lat,
#                 color = heading_class3.actual_turn)
#fig.show()






