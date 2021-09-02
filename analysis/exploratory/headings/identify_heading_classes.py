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
os.environ["GDAL_DATA"] = os.environ["CONDA_PREFIX"] + "\Library\share\gdal"
path_working = r"C:\OD\OneDrive - Foursquare ITP\Documents\0002 R\WMATA Datamart\WMATA_AVL"
os.chdir(os.path.join(path_working))
sys.path.append(r"C:\OD\OneDrive - Foursquare ITP\Documents\0002 R\WMATA Datamart\WMATA_AVL")
path_sp = r"C:\OD\Foursquare ITP\Projects - WMATA Datamart\Task 3 - Bus Priority"
path_source_data = os.path.join(path_sp,"data","00-Raw")
path_processed_data = os.path.join(path_sp, "data","02-Processed")
# Server credentials
config = dotenv_values(os.path.join(path_working, '.env'))

# Globals
q_jump_route_list = ['30N','30S','33','31']
analysis_routes = ['30N']
analysis_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
wmata_crs = 2248

# Load wmatarawnav library
import wmatarawnav as wr


# %% Read in rawnav data for Wisconsin Ave corridor

rawnav_raw = pd.DataFrame()

for yr in [
    '202102',
    '202103',
    '202104',
    '202105'
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
rawnav_heading3 = interp_column_over_sec(rawnav_heading2[rawnav_heading2.index_run_start == 14528], 'heading')

# %% Plot sample data
    
heading_sample = (
#    rawnav_reset_heading[rawnav_reset_heading.index_run_start == 9306]
    rawnav_heading3[rawnav_heading3.index_run_start == 14528]
#    .query('filename == "rawnav07225210305.txt"')
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



# %% Smooth heading values?

rawnav_heading_sm = smooth_rawnav_column(rawnav_heading3[rawnav_heading3.index_run_start == 14528],
                                         smooth_col = 'heading',
                                         window_size = 11)

# %% Plot sample data
    
heading_sample_sm = (
#    rawnav_reset_heading[rawnav_reset_heading.index_run_start == 9306]
    rawnav_heading_sm[rawnav_heading_sm.index_run_start == 14528]
#    .query('filename == "rawnav07225210305.txt"')
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
rawnav_heading4 = calc_rawnav_speed(rawnav_heading_sm[rawnav_heading_sm.index_run_start == 14528], 'heading_sm')



# %% Plot sample data

speed_sample = (
    rawnav_heading4[rawnav_heading4.index_run_start == 14528]
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

rawnav_heading_smooth = smooth_rawnav_column(rawnav_heading4,
                                             smooth_col = 'heading_sm_speed_next',
                                             window_size = 11)


# %% Visualize sample again

speed_sample_smooth = (
    rawnav_heading_smooth[rawnav_heading_smooth.index_run_start == 14528]
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



# %% Determine categories
    
    
# Pull into stop
# Pull out of stop
# Change lanes
# Turn at intersection
# Drive straight
# At stop (door open?)
















