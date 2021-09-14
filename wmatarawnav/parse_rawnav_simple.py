# -*- coding: utf-8 -*-
"""
Created on Sat Sep  4 04:35:35 2021

@author: WylieTimmerman
"""

import pandas as pd
import numpy as np
from . import parse_rawnav as pr
import re
from . import low_level_fns as ll
import pyarrow as pa

def clean_rawnav_data_alt(data_dict, filename, analysis_routes = None):
    '''
    Parameters
    ----------
    filename: rawnav file name
    data_dict : dict
        dict of raw data and the data on tag lines.
    Returns
    -------
    Cleaned data without any tags.
    '''
    rawnavdata = data_dict['RawData'].copy(deep = True)
    tagline_data = data_dict['tagLineInfo'].copy(deep = True)

    # Check the location of taglines from tagline_data data match the locations in rawnavdata
    try:
        temp = tagline_data.new_line_no.values
        tag_indices = np.delete(temp, np.where(temp == -1))
        # Essentially, we reconstitute the tags in the file from the separated columns in rawnavdata and 
        #   make sure that they all match the separate tag data that came from the rawnav_inventory
        if (len(tag_indices)) != 0:
            check_tag_line_data = rawnavdata.loc[tag_indices, :]
            check_tag_line_data[[1, 4, 5]] = check_tag_line_data[[1, 4, 5]].astype(int)
            check_tag_line_data.loc[:, 'taglist'] = \
                (check_tag_line_data[[0, 1, 2, 3, 4, 5]].astype(str) + ',').sum(axis=1).str.rsplit(",", 1, expand=True)[
                    0]
            check_tag_line_data.loc[:, 'taglist'] = check_tag_line_data.loc[:, 'taglist'].str.strip()
            infopat = '^\s*(\S+),(\d{1,5}),(\d{2}\/\d{2}\/\d{2}),(\d{2}:\d{2}:\d{2}),(\S+),(\S+)'
            assert ((~check_tag_line_data.taglist.str.match(infopat, re.S)).sum() == 0)
    except:
        print("TagLists Did not match in file {}".format(filename))
    
    rawnavdata.reset_index(inplace=True)
    rawnavdata.rename(columns={"index": "index_loc"}, inplace=True)
    
    # Get End of route Info
    tagline_data, delete_indices1 = pr.add_end_route_info(rawnavdata, tagline_data)
    rawnavdata = rawnavdata[~rawnavdata.index.isin(np.append(tag_indices, delete_indices1))]
    
    # Remove APC and CAL labels and keep APC locations. 
    rawnavdata, apc_tag_loc = pr.remove_apc_cal_tags(rawnavdata)
    rawnavdata = rawnavdata[rawnavdata.apply(pr.check_valid_data_entry, axis=1)]
    apc_loc_dat = pd.Series(apc_tag_loc, name='apc_tag_loc')
    apc_loc_dat = \
        pd.merge_asof(apc_loc_dat, rawnavdata[["index_loc"]], left_on="apc_tag_loc", right_on="index_loc")
    rawnavdata.loc[:, 'row_before_apc'] = False
    rawnavdata.loc[apc_loc_dat.index_loc, 'row_before_apc'] = True
    tagline_data.rename(columns={'new_line_no': "index_run_start_original"}, inplace=True)
       
    # Rename vals
    column_nm_map = {0: 'lat', 
                     1: 'long', 
                     2: 'heading', 
                     3: 'door_state', 
                     4: 'veh_state', 
                     5: 'odom_ft', 
                     6: 'sec_past_st',
                     7: 'sat_cnt',
                     8: 'stop_window',
                     9: 'blank', 
                     10: 'lat_raw', 
                     11: 'long_raw'}
    
    rawnavdata.rename(columns=column_nm_map, inplace=True)
    # Apply column transformations
    rawnavdata = rawnavdata.assign(lat=lambda x: x.lat.astype('float'),
                                   long=lambda x: x.long.astype('float'),
                                   heading=lambda x: x.heading.astype('float'))
    
    rawnavdata = add_run_dividers_simple(rawnavdata, tagline_data)
    
    # Final clean up matters
    # This was done differently in the older version, just trying to get it to run now.
    rawnavdata['pattern'] = rawnavdata['pattern'].str.strip().astype('float').astype('Int64')
    
    # Filter to analysis_routes
    rawnavdata = (
        rawnavdata
        .loc[rawnavdata.route.isin(analysis_routes)]
    )

    return rawnavdata

def add_run_dividers_simple(data, tagline_data):
    '''
    Parameters
    ----------
    data : pd.DataFrame
        rawnav data without tags.
    summary_data : pd.DataFrame
        Tagline data.
    Returns
    -------
    rawnav data with composite keys.

    '''
    # TODO: replace this with something based on merge_asof; right now, we 
    # need to steal the index reset pieces from get_run_summary, then use that
    # to essentially replace the between join.
    tagline_data = (
        tagline_data 
        .assign(
            index_run_start_original = lambda x: x.index_run_start_original.astype('int64'),
            index_run_end_original = lambda x: x.index_run_end_original.astype('int64')
        )
        .assign(
            # TODO: move this somewhere else so we can recalculate at scale
            tag_key = lambda x: 
                x.index_run_start_original.astype(str) + 
                "_" +
                x.index_run_end_original.astype(str)
        )
        .rename(columns ={
                    "tag_datetime" : "start_date_time",
                }
        )
        .assign(
            end_date_time = lambda x: 
                pd.to_datetime(
                    x.tag_date.astype(str) + 
                    " " + 
                    x.run_end_time,
                    errors='coerce'
                )
        )
    )
        
    data_joined = (
        pd.merge_asof(
            data, 
            tagline_data
            .filter(['tag_key', 'index_run_start_original'], axis = "columns")
            .sort_values(['index_run_start_original']), 
            left_on="index_loc", 
            right_on="index_run_start_original", 
            direction='backward'
        )
        .rename(columns = {'tag_key':'tag_key_start'})
        .drop(columns = ['index_run_start_original'])
    )
    
    data_joined = (
        pd.merge_asof(
            data_joined, 
            tagline_data
            .filter(['tag_key', 'index_run_end_original'], axis = "columns")
            .sort_values(['index_run_end_original']), 
            left_on="index_loc", 
            right_on="index_run_end_original", 
            direction='forward'
        )
        .rename(columns = {'tag_key':'tag_key_end'})
        .drop(columns = ['index_run_end_original'])
    )
    
    data_joined = (
        data_joined
        .loc[data_joined.tag_key_start == data_joined.tag_key_end]
    )
    
    data_joined = (
        data_joined
        .merge(
            tagline_data
            .filter([
                'tag_key',
                'filename',
                'wday',
                'route',
                'pattern',
                'route_pattern',
                'start_date_time',
                'end_date_time'
                ], 
                axis = "columns"
            ),
            left_on = 'tag_key_start',
            right_on = "tag_key",
            how = "left"
        )
    )
    
    data_joined['index_run_start'] = (
        data_joined
        .groupby(['tag_key_start'])['index_loc']
        .transform(
            lambda x: x.min()
        )      
    )
    
    data_joined = (
        data_joined
        .drop(columns = ['tag_key_start','tag_key_end','tag_key','run_end_time'], errors = 'ignore')
    )

    data_joined  = (
        data_joined 
        .pipe(
            ll.reorder_first_cols,
            [
                'filename',
                'index_run_start',
                'route',
                'pattern',
                'route_pattern',
                'start_date_time',
                'end_date_time',
                'wday',
                'index_loc',
                'sec_past_st',
                'odom_ft',
                'door_state',
                'row_before_apc',
                'stop_window',
                'veh_state',
                'heading',
                'lat',
                'long',
                'lat_raw',
                'long_raw',
                'sat_cnt'
            ]
        )
    )

    return data_joined


def rawnav_data_simple_schema():
    """
    Returns
    -------
    rawnav_data_schema: pa.schema,
      a schema for rawnav data, put here to keep code a bit tidier
    """
    
    rawnav_data_schema = pa.schema([
        pa.field('filename', pa.string()),
        pa.field('index_run_start', pa.float64()), #converting because of lack of support for int64
        pa.field('route', pa.string()),
        pa.field('pattern', pa.float64()),
        pa.field('route_pattern', pa.string()),
        pa.field('wday', pa.string()),
        pa.field('start_date_time', pa.timestamp('us')),
        pa.field('end_date_time', pa.timestamp('us')),
        pa.field('index_loc', pa.float64()),
        pa.field('sec_past_st', pa.float64()),
        pa.field('odom_ft',pa.float64()),
        pa.field('door_state', pa.string()),
        pa.field('row_before_apc', pa.float64()),
        pa.field('stop_window', pa.string()),
        pa.field('veh_state', pa.string()),
        pa.field('heading', pa.float64()),
        pa.field('lat', pa.float64()),
        pa.field('long', pa.float64()),
        pa.field('lat_raw', pa.float64()),
        pa.field('long_raw',pa.float64()),
        pa.field('sat_cnt', pa.float64()),
        pa.field('blank', pa.float64())
    ])
        
    return rawnav_data_schema


def rawnav_decomp_schema():
    """
    Returns
    -------
    rawnav_data_schema: pa.schema,
      a schema for rawnav data, put here to keep code a bit tidier
    """
    # NOTE: this silently drops index_run_end, since we didn't bring that along in the 
    # parse rawnav schema above
    rawnav_data_schema = pa.schema([
        pa.field('filename', pa.string()),
        pa.field('index_run_start', pa.float64()), #converting because of lack of support for int64
        pa.field('route', pa.string()),
        pa.field('pattern', pa.float64()),
        pa.field('route_pattern', pa.string()),
        pa.field('wday', pa.string()),
        pa.field('start_date_time', pa.timestamp('us')),
        pa.field('end_date_time', pa.timestamp('us')),
        pa.field('index_loc', pa.float64()),
        pa.field('sec_past_st', pa.float64()),
        pa.field('odom_ft',pa.float64()),
        pa.field('door_state', pa.string()),
        pa.field('veh_state', pa.string()),
        pa.field('heading', pa.float64()),
        pa.field('lat', pa.float64()),
        pa.field('long', pa.float64()),
        pa.field('lat_raw', pa.float64()),
        pa.field('long_raw',pa.float64()),
        pa.field('sat_cnt', pa.float64()),
        pa.field('collapsed_rows', pa.float64()),  #was int64         
        pa.field('odom_ft_min', pa.float64()),      
        pa.field('odom_ft_max', pa.float64()),      
        pa.field('door_state_all', pa.string()),      
        pa.field('stop_window_e', pa.string()),      
        pa.field('stop_window_x', pa.string()),      
        pa.field('row_before_apc', pa.string()),      
        pa.field('blank', pa.string()),      
        pa.field('odom_ft_next', pa.float64()),      
        pa.field('sec_past_st_next', pa.float64()),      
        pa.field('secs_marg', pa.float64()),      
        pa.field('odom_ft_marg', pa.float64()),      
        pa.field('fps_next', pa.float64()),      
        pa.field('fps_next_sm', pa.float64()),      
        pa.field('accel_next', pa.float64()),      
        pa.field('jerk_next', pa.float64()),      
        pa.field('fps3', pa.float64()),      
        pa.field('accel3', pa.float64()),      
        pa.field('jerk3', pa.float64()),      
        pa.field('accel9', pa.float64()),      
        pa.field('veh_state_calc', pa.string()),      
        pa.field('basic_decomp', pa.string()),      
        pa.field('stopped_changes_collapse', pa.float64()),   # was int32         
        pa.field('any_door_open', pa.bool_()),       
        pa.field('any_veh_stopped', pa.bool_()),       
        pa.field('door_changes', pa.float64()),      
        pa.field('relative_to_firstdoor', pa.string()),      
        pa.field('door_case', pa.string()),      
        pa.field('pax_activity', pa.string()),      
        pa.field('stop_decomp', pa.string()),      
        pa.field('stop_id_loc', pa.float64()),      
        pa.field('stop_sequence_loc', pa.float64()),      
        pa.field('stop_id_group', pa.float64()),      
        pa.field('stop_case', pa.string()),      
        pa.field('stop_decomp_ext', pa.string()),      
        pa.field('trip_seg', pa.string()),      
        pa.field('basic_decomp_ext', pa.string()),      
        pa.field('stop_id_group_ext', pa.float64()),      
        pa.field('odom_ft_og', pa.float64()),      
        pa.field('sec_past_st_og', pa.float64())       
    ])
        
    return rawnav_data_schema

