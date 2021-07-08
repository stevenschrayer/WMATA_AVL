# -*- coding: utf-8 -*-
"""
Created by: wytimmerman
Purpose: Merge wmata_schedule and rawnav data
Created on Wed Jun 16 2021
"""
# % Environment Setup
import os, sys, pandas as pd, pyarrow.parquet as pq

# For postgresql
# TODO: for now, skipping server, as amit says it's a bit slow
from dotenv import dotenv_values
import pyarrow as pa

if os.getlogin() == "WylieTimmerman":
    # Working Paths
    # the following needed to run in vscode jupyter interpreter
    os.environ["GDAL_DATA"] = os.environ["CONDA_PREFIX"] + "\Library\share\gdal"
    path_working = r"C:\Users\WylieTimmerman\Documents\projects_local\WMATA_AVL_datamart"
    os.chdir(os.path.join(path_working))
    sys.path.append(r"C:\Users\WylieTimmerman\Documents\projects_local\WMATA_AVL_datamart")
    path_sp = r"C:\OD\Foursquare ITP\Projects - WMATA Datamart\Task 3 - Bus Priority"
    path_source_data = os.path.join(path_sp,"data","00-Raw")
    path_processed_data = os.path.join(path_sp, "data","02-Processed")
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

# % Reload Data
# %% Rawnav Data
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
                "lat",
                "long",
                "heading",
                "sat_cnt",
                "blank",
                "lat_raw",
                "long_raw",
                "index_run_end",
                "wday",
                "veh_state",
                "pattern",
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
    pq.read_table(source=os.path.join(path_processed_data,"stop_summary.parquet"),
                    # columns = [ 'route',
                    #             'pattern',
                    #             'direction', #same as pattern_name_wmata_schedule
                    #             'stop_id',
                    #             'filename',
                    #             'index_run_start',
                    #             'index_loc',
                    #             'odom_ft',
                    #             'sec_past_st',
                    #             'geo_description'],
                    use_pandas_metadata = True
    )
    .to_pandas()
    # As a bit of proofing, we confirm this is int32 and not string, may remove later
    .assign(pattern = lambda x: x.pattern.astype('int32')) 
    .reset_index()
)
    
# % Process
# %% Join Datasets
# At this point, if the odom reading of that nearest point is found in multiple places 
# (sometimes odom reading will repeat), some stops will show up twice. I think this is okay 
# for the calculations that follow.

rawnav_fil = (
    rawnav_raw
    .merge(
        stop_index
        # TODO: join on index_loc as well
        .filter(items = ['filename','index_run_start','odom_ft_stop','stop_id']),
        left_on = ['filename','index_run_start','odom_ft'],
        right_on = ['filename','index_run_start','odom_ft_stop'],
        how = "left"
    )
)

del rawnav_raw

# %% Run the basic decomposition

# just testing out the two approaches
# rawnav_window = (
#     wr.assign_stop_area(
#         rawnav_fil,
#         stop_field = "stop_window",
#         upstream_ft = 150,
#         downstream_ft = 150
#     )
# )

# Until the stops data gets a little better, we are leaving this as NA
rawnav_window = (
    wr.assign_stop_area(
        rawnav_fil,
        stop_field = "stop_id",
        upstream_ft = 150,
        downstream_ft = 150
    )
)

# del rawnav_fil

# %% Run the basic decomposition
rawnav_window_basic = pd.DataFrame()

for rt in analysis_routes:
    print(rt)
    rawnav_window_rt = rawnav_window.query('route == @rt')
    
    rawnav_window_basic_temp = (
        wr.decompose_basic_mt(
            rawnav_window_rt
        )
    )
    
    rawnav_window_basic = pd.concat([rawnav_window_basic,rawnav_window_basic_temp])
    
# del rawnav_window_basic
# del rawnav_window
    

# %% Calculate the stop-level free-flow times
# We need to wait to do this until after the passenger and non-passenger delay are separated

rawnav_ff_window_all = (
    wr.get_stop_ff(
        rawnav_window_basic,
        method = "all"
    )
)

rawnav_ff_window_all.to_csv('rawnav_ff_window_all.csv')

rawnav_ff_window_95 = (
    rawnav_ff_window_all
     .loc[rawnav_ff_window_all['ntile'] == "mph_p95",]
)

# %% Summarize each run
# Because we're now differencing out delay and free-flow time, we have to aggregate above 
# individual rawnav records
rawnav_run_decomp = (
    wr.decompose_full_mt(
        rawnav_window_basic,
        rawnav_ff_window_95
    )
)

# Reattach run info
rawnav_run_info =(
   stop_summary
   .filter(
       ['filename','index_run_start',
        'direction_wmata_schedule',
        'pattern_destination_wmata_schedule',
        'route',
        'start_date_time',
        'start_end_time'
        ],
       axis = 'columns'
    )
)

# Export the result
rawnav_run_decomp_exp = (
    rawnav_run_decomp
    .merge(
        rawnav_run_info,
        on = ['filename','index_run_start'],
        how = "left"
    )
)

path_output = os.path.join(path_sp,"data","01-Interim","wisconsin_decomp_mt.parquet")

pq.write_to_dataset(
    table = pa.Table.from_pandas(rawnav_run_decomp_exp),
    root_path = path_output
)

