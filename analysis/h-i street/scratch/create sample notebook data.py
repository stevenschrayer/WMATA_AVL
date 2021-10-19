# -*- coding: utf-8 -*-
"""
Created on Thu Oct 14 05:17:35 2021

@author: WylieTimmerman
"""

keepfiles = ["rawnav02205171017.txt","rawnav02502171005.txt"]
keepindex = [2327, 3685, 5092, 2175, 3450, 4681]

test = (
    rawnav_route
    .loc[
        rawnav_route.filename.isin(keepfiles) & 
        rawnav_route.index_run_start.isin(keepindex)
    ]
    .filter(
        [
            "filename",	
            "index_run_start",	
            "route",
            "pattern",	
            "route_pattern",	
            "wday",	
            "start_date_time",	
            "end_date_time",	
            "index_loc",
            "sec_past_st",
            "odom_ft",
            "door_state",
            "row_before_apc",
            "stop_window",
            "veh_state",	
            "heading",
            "lat",
            "long",
            "sat_cnt",
            "blank"
        ],
        axis = "columns"
    )
)

test.to_parquet('datain.parquet')


teststop = (
    stop_index
    .loc[
        stop_index.filename.isin(keepfiles) & 
        stop_index.index_run_start.isin(keepindex)
    ]
)

teststop.to_parquet('stop_index.parquet')

oneti = (
    rawnav_route
    .query("filename == 'rawnav02205171017.txt' & index_run_start == 2327")
)

rawnav_route.to_csv(os.path.join(path_sp,"Data","01-Interim","test_decomp_mov20.csv"))

