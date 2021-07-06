# -*- coding: utf-8 -*-
"""
Created by: wytimmerman
Purpose: Merge wmata_schedule and rawnav data
Created on Wed Jun 16 2021
"""
# % Environment Setup
import os, sys, pandas as pd, pyarrow.parquet as pq

# For postgresql
# TODO: for now, skipping, as amit says it's a bit slow
from dotenv import dotenv_values
# import pg8000.native # not strictly required to load, but is used by sqlalchemy below
# import sqlalchemy

if os.getlogin() == "WylieTimmerman":
    # Working Paths
    # the following needed to run in vscode jupyter interpreter
    os.environ["GDAL_DATA"] = os.environ["CONDA_PREFIX"] + "\Library\share\gdal"
    path_working = r"C:\Users\WylieTimmerman\Documents\projects_local\WMATA_AVL_datamart"
    os.chdir(os.path.join(path_working))
    sys.path.append(r"C:\Users\WylieTimmerman\Documents\projects_local\WMATA_AVL_datamart")
    path_sp = r"C:\OD\Foursquare ITP\Projects - WMATA Datamart\Task 3 - Bus Priority"
    path_source_data = os.path.join(path_sp,"data","00-raw")
    path_processed_data = os.path.join(path_sp, "data","02-processed")
    # Server credentials
    config = dotenv_values(os.path.join(path_working, '.env'))

# Globals
tsp_route_list = ['30N','30S','33','31']
analysis_routes = ['30N']
analysis_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
# EPSG code for WMATA-area work
wmata_crs = 2248

import wmatarawnav as wr

# Connect to postgres
# TODO: for now, i'm going to skip postgres work, as amit says it's a bit slow

# % Reload Data
# %% Rawnav Data
rawnav_raw = pd.DataFrame()

for yr in [
    '202102' #,
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

del rawnav_raw_temp # i'm not sure why this doesn't always delete

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

# % Process
# %% Join Datasets
# At this point, if the odom reading of that nearest point is found in multiple places 
# (sometimes odom reading will repeat), some stops will show up twice. I think this is okay 
# for the calculations that follow.

rawnav_raw_fil = rawnav_raw
rawnav_fil = (
    rawnav_raw_fil
    .merge(
        stop_index
        .filter(items = ['filename','index_run_start','odom_ft_stop','stop_id']),
        left_on = ['filename','index_run_start','odom_ft'],
        right_on = ['filename','index_run_start','odom_ft_stop'],
        how = "left"
    )
)

rawnav_fil = (
    rawnav_fil
    .loc[rawnav_fil.route.isin(['30N','30S','33'])]
)

# %% Run the basic decomposition

# just testing out the two approaches
rawnav_window = (
    wr.assign_stop_area(
        rawnav_fil,
        stop_field = "stop_window",
        upstream_ft = 150,
        downstream_ft = 150
    )
)

rawnav_wstops = (
    wr.assign_stop_area(
        rawnav_fil,
        stop_field = "stop_id",
        upstream_ft = 150,
        downstream_ft = 150
    )
)

# %% Calculate the stop-level free-flow times
rawnav_ff_window_mt = (
    wr.get_stop_ff(
        rawnav_window,
        method = "mt"
    )
)

# %% Summarize each run
# Because we're now differencing out delay and free-flow time, we have to aggregate above 
# individual rawnav records
rawnav_run_decomp = (
    wr.decompose_full(

    )

)
