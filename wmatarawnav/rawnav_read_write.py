# -*- coding: utf-8 -*-
"""
Created on Thu Jul  9 12:20:18 2020

@author: WylieTimmerman, Apoorba Bibeka
"""

import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from itertools import product
from . import low_level_fns as ll


def read_cleaned_rawnav(path, analysis_routes_, analysis_days_ = None):
    """
    Parameters
    ----------
    path: str,
       path where the parquet files for cleaned data is kept
    analysis_routes_: list,
        routes for which rawnav data is needed. Should be a subset of following:
        ['S1', 'S2', 'S4', 'S9', '70', '79', '64', 'G8', 'D32', 'H1', 'H2', 'H3', 'H4',
                         'H8','W47']
    analysis_days_: list,
        days of the week for which data is needed. Should be a subset of following:
        ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']

    Returns
    -------
    rawnav_dat: pd.DataFrame,
      rawnav data
    """
    
    # Parameter Checks
    day_of_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    if analysis_days_ is None:
        analysis_days_ = day_of_week
    
    # Convert back to list if not already
    analysis_routes_ = ll.check_convert_list(analysis_routes_)    
    analysis_days_ = ll.check_convert_list(analysis_days_)
    
    day_of_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    assert (set(analysis_days_).issubset(set(day_of_week))),\
        print("""analysis_days_ is a subset of following days: 
              ('Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday')
              """)
    assert (len(analysis_days_) == len(set(analysis_days_))),\
        print("analysis_days_ entries cannot be duplicated")     
         
    # Function Body
    
    combo = pd.DataFrame(list(product(analysis_routes_, analysis_days_)), columns = ['route','wday'])
    combo_zip = zip(combo.route, combo.wday)
    filter_parquet = [[('route','=',route),('wday', '=', day)] for route, day in combo_zip]
    
    try:
        rawnav_temp_dat = (
            pq.read_table(source=os.path.join(path),
                          filters=filter_parquet,
                          use_pandas_metadata = True)
            .to_pandas())
        
        # NOTE: could selectt fewer columns to save memory/time on read in, but don't expect
        # performance boost to be that great, could be obnoxious
    except Exception as e:
        if str(type(e)) == "<class 'IndexError'>":
            raise ValueError('No data found for any of given filter conditions')
        else:
            print(e)
            raise
    else:
        # In case partition was written multiple times, we take the last entry added.
        
        # A little hack used depending on the dataset read in
        check_data =  all(item in list(rawnav_temp_dat.columns) for item in ['index_loc','filename','index_run_start'])
        check_summary = all(item in list(rawnav_temp_dat.columns) for item in ['filename','index_run_start'])
        
        if check_data:
            rawnav_temp_dat = rawnav_temp_dat[
                ~rawnav_temp_dat.duplicated(['index_loc', 'filename', 'index_run_start'], keep='last')] 
        elif check_summary:
            rawnav_temp_dat = rawnav_temp_dat[
                ~rawnav_temp_dat.duplicated(['filename', 'index_run_start'], keep='last')] 
        else: 
            print("Doesn't match expected input")
            raise
        
        # Even after defining the schema on parquet write, we're still seeing some strings 
        # read in as categories rather than as strings. Very odd.
        rawnav_temp_dat.route = rawnav_temp_dat.route.astype('str') 
        rawnav_temp_dat.wday = rawnav_temp_dat.wday.astype('str') 
        # Even though we could store as int, in case the values have NA's, we store as float
        # and then convert to int after the fact. If you happen to run into a problem here, do an adhoc
        # load of the parquet file and then after filtering NA values, convert pattern to 
        # integer.
        rawnav_temp_dat.pattern = rawnav_temp_dat.pattern.astype('int') 

    return rawnav_temp_dat

def rawnav_data_schema():
    """
    Returns
    -------
    rawnav_data_schema: pa.schema,
      a schema for rawnav data, put here to keep code a bit tidier
    """
    
    rawnav_data_schema = pa.schema([
        pa.field('index_loc', pa.float64()),
        pa.field('lat', pa.float64()),
        pa.field('long', pa.float64()),
        pa.field('heading', pa.float64()),
        pa.field('door_state', pa.string()),
        pa.field('veh_state', pa.string()),
        pa.field('odom_ft',pa.float64()),
        pa.field('sec_past_st', pa.float64()),
        pa.field('sat_cnt', pa.float64()),
        pa.field('stop_window', pa.string()),
        pa.field('blank', pa.float64()),
        pa.field('lat_raw', pa.float64()),
        pa.field('long_raw',pa.float64()),
        pa.field('row_before_apc', pa.float64()),
        pa.field('route_pattern', pa.string()),
        pa.field('route', pa.string()),
        pa.field('pattern', pa.float64()),
        pa.field('index_run_start', pa.float64()),
        pa.field('index_run_end', pa.float64()),
        pa.field('filename', pa.string()),
        pa.field('wday', pa.string()),
        pa.field('start_date_time', pa.timestamp('us'))
    ])
        
    return rawnav_data_schema

def rawnav_summary_schema():
    """
    Returns
    -------
    rawnav_summary_schema: pa.schema,
      a schema for rawnav data, put here to keep code a bit tidier
    """
    
    rawnav_summary_schema = pa.schema([
        pa.field('fullpath', pa.string()),
        pa.field('filename', pa.string()),
        pa.field('file_busid', pa.int64()),
        pa.field('file_id', pa.string()),
        pa.field('taglist', pa.string()),
        pa.field('route_pattern', pa.string()),
        pa.field('tag_busid', pa.float64()),
        pa.field('route', pa.string()),
        pa.field('pattern', pa.int32()),
        pa.field('wday', pa.string()),
        pa.field('start_date_time', pa.timestamp(unit = 'ns')),
        pa.field('end_date_time', pa.timestamp(unit = 'ns')),
        pa.field('index_run_start_original', pa.int32()),
        pa.field('index_run_start', pa.int32()),
        pa.field('index_run_end_original', pa.float64()),
        pa.field('index_run_end', pa.int32()),
        pa.field('sec_start', pa.int32()),
        pa.field('odom_ft_start', pa.int32()),
        pa.field('sec_end', pa.int32()),
        pa.field('odom_ft_end', pa.int32()),
        pa.field('run_duration_from_sec', pa.int32()),
        pa.field('run_duration_from_tags', pa.string()),
        pa.field('dist_odom_mi', pa.float64()),
        pa.field('mph_odom',pa.float64()),
        pa.field('mph_run_tag',pa.float64()),
        pa.field('dist_crow_fly_mi',pa.float64()),
        pa.field('lat_start',pa.float64()),
        pa.field('long_start',pa.float64()),
        pa.field('lat_end',pa.float64()),
        pa.field('long_end',pa.float64())
    ])
    
    return rawnav_summary_schema
