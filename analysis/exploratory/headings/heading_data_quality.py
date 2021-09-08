# -*- coding: utf-8 -*-
"""
Created by: jmcdowell
Purpose: Analyze data quality of heading column
Created on Tue Sep 07 2021
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
analysis_routes = ['31']
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


# %% Count NAs in heading column

heading_na = pd.to_numeric(rawnav_fil['heading']).isna().sum()

# No missing values at all!!

heading_min = rawnav_fil.heading.min()
heading_max = rawnav_fil.heading.max()

# Values are all between 0 and 360

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

del rawnav_window

# %% Prepare data for speed calculation

# Replace the heading column with the new heading values
rawnav_heading = rawnav_reset_heading.assign(heading = rawnav_reset_heading[['heading_new']])

del rawnav_reset_heading

# aggregate so we only have one observation for each second - this uses the 'last' heading observation
rawnav_heading2 = agg_sec(rawnav_heading)

# quick check of how many pings we will have repeated seconds values
(rawnav_heading2.shape[0] - rawnav_heading.shape[0]) / rawnav_heading.shape[0]

del rawnav_heading

# This is a separate step again; there are some places where we'll want to interpolate that
# aren't just the ones where we aggregated seconds. In general, we'll probably want to 
# revisit the aggregation/interpolation process, so I'm going to try not to touch this too much
# more for now.
rawnav_heading3 = interp_column_over_sec(rawnav_heading2, 'heading')

del rawnav_heading2

# %% Smooth heading values

# Drop broken trips
rawnav_heading3['trip_rows'] = (
    rawnav_heading3
    .groupby(['filename','index_run_start'], sort = False)[['odom_ft']]
    .transform(len)
)

rawnav_heading4 = rawnav_heading3[rawnav_heading3.trip_rows > 10]

#rawnav_heading3_1 = rawnav_heading3[~rawnav_heading3.index_run_start.isin([3607,14212,25546,8820,3406,8117,3246])]

#trip_list = rawnav_heading4.index_run_start.unique()
#
#for idx in trip_list:
#    print(idx)
rawnav_heading_sm = smooth_rawnav_column(rawnav_heading4,
                                         smooth_col = 'heading',
                                         window_size = 11)

del rawnav_heading3

# %% Calculate angular speed

# these are not the rolling vals, though i think we will want to include those before long.
rawnav_heading5 = calc_rawnav_speed(rawnav_heading_sm, 'heading_sm')

del rawnav_heading_sm

# %% Check how many trips have speeds of more than 45 deg/sec

# Number of pings with unrealistic speeds
speed_check_pings = rawnav_heading5[rawnav_heading5.heading_sm_speed_next.ge(45)]

speed_check_pings.shape[0] / rawnav_heading5.shape[0]

# Number of trips with unrealistic speeds
speed_check_trips = speed_check_pings[['index_run_start','filename']].drop_duplicates()

speed_check_trips.shape[0] / rawnav_heading5[['index_run_start','filename']].drop_duplicates().shape[0]



# %% Save values for each route

pings_total_31 = rawnav_heading5.shape[0]
pings_bad_31 = speed_check_pings.shape[0]

trips_total_31 = rawnav_heading5[['index_run_start','filename']].drop_duplicates().shape[0]
trips_bad_31 = speed_check_trips.shape[0]

(pings_bad_30N + pings_bad_30S + pings_bad_33 + pings_bad_31) / (pings_total_30N + pings_total_30S + pings_total_33 + pings_total_31)
(trips_bad_30N + trips_bad_30S + trips_bad_33 + trips_bad_31) / (trips_total_30N + trips_total_30S + trips_total_33 + trips_total_31)


