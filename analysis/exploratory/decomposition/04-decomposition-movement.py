# -*- coding: utf-8 -*-
"""
Created on Wed July 22 02:45:31 2021

@author: WylieTimmerman

We'll use this to test out some new options for decomposing the bus's movement
"""

# % Environment Setup
import os, sys, pandas as pd, numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import dotenv_values

if os.getlogin() == "WylieTimmerman":
    # Working Paths
    # the following needed to run in vscode jupyter interpreter
    os.environ["GDAL_DATA"] = os.environ["CONDA_PREFIX"] + "\Library\share\gdal"
    path_working = r"C:\Users\WylieTimmerman\Documents\projects_local\WMATA_AVL_datamart"
    os.chdir(os.path.join(path_working))
    sys.path.append(r"C:\Users\WylieTimmerman\Documents\projects_local\WMATA_AVL_datamart")
    path_sp = r"C:\OD\Foursquare ITP\Projects - WMATA Datamart\Task 3 - Bus Priority"
    path_source_data = os.path.join(path_sp,"data","00-Raw")
    path_processed_data = os.path.join(path_sp, "Data","02-Processed")
    # Server credentials
    config = dotenv_values(os.path.join(path_working, '.env'))
    # other things for wylie's dev environment            

# Globals
tsp_route_list = ['30N','30S','33','31']
analysis_routes = ['30N']
analysis_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
# EPSG code for WMATA-area work
wmata_crs = 2248

import wmatarawnav as wr

# %% Reload the data

rawnav_raw = pd.DataFrame()

for yr in [
    '202102'#,
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
    )

    rawnav_raw = rawnav_raw.append(rawnav_raw_temp, ignore_index = True)

del rawnav_raw_temp 

# Reload the used wisconsin stops
rawnav_used_trips = (
    pq.read_table(
        source = os.path.join(path_sp,"data","01-Interim","wisconsin_decomp_mt_used_trips.parquet"),
        use_pandas_metadata = True
    )
    .to_pandas()
    .reset_index()
)

# Filter the rawnav data to those used trips

rawnav_fil = (
    rawnav_raw
    .pipe(
        wr.semi_join,
        right = rawnav_used_trips,
        on = ['filename','index_run_start']
    )    
)

# Load the stop data
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

# %% Start decomposition

# TODO: replace with amit's methods
rawnav_fil2 = (
    rawnav_fil
    # maybe we look a little bit closer at the southbound trips only
    # i think 30N02 is southbound
    .query('pattern == 2')
)

# aggregate so we only have one observation for each second
rawnav_fil3 = wr.agg_sec(rawnav_fil2)

# quick check of how many pings we will have repeated seconds values
(rawnav_fil3.shape[0] - rawnav_fil2.shape[0]) / rawnav_fil2.shape[0]

# This is a separate step again; there are some places where we'll want to interpolate that
# aren't just the ones where we aggregated seconds. In general, we'll probably want to 
# revisit the aggregation/interpolation process, so I'm going to try not to touch this too much
# more for now.
rawnav_fil4 = wr.interp_over_sec(rawnav_fil3)

rawnav_fil5 = wr.calc_speed(rawnav_fil4)

# this includes calculating the accel and such based on smoothed speed values
rawnav_fil6 = wr.smooth_speed(rawnav_fil5)

rawnav_fil7 = wr.calc_rolling(rawnav_fil6,['filename','index_run_start'])

# Add in the decomposition
rawnav_fil8 = (
    rawnav_fil7
    .pipe(
          wr.decompose_mov,
          stopped_fps = 3, #upped from 2
          slow_fps = 14.67, # upped default to 10mph
          steady_accel_thresh = 2 #based on some casual observations
     )
)

# Identify the stops in the decomp
rawnav_fil9 = wr.match_stops(rawnav_fil8, stop_index)

# TODO: would normally include some test or filter that if there was a door open event, it was 
# matched to a stop to make sure our stop inputs and such are appropriate
# door_open_no_stop = (
#     rawnav_fil9
#     .groupby(['filename','index_run_start','stopped_changes_collapse'])
#     .filter(
#         lambda x: any(x.stop_type.eq('pax')) & any(x.stop_id_group.isna())       
#     )
# )

# this also associates accel/decel with a stop, so we probably need to fix that too
rawnav_fil10 = wr.create_stop_segs(rawnav_fil9)

rawnav_fil11 = (
    rawnav_fil10
    .groupby(['filename','index_run_start'])
    .apply(lambda x: wr.reset_odom(x,indicator_val = 58, indicator_var = 'stop_id_group'))
)
