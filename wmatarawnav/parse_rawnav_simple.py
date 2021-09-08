# -*- coding: utf-8 -*-
"""
Created on Sat Sep  4 04:35:35 2021

@author: WylieTimmerman
"""

import pandas as pd
import numpy as np
from . import parse_rawnav as pr
import re
import pandasql as ps

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
    
    rawnavdata.loc[:, "filename"] = filename
    
    # Filter to analysis_routes
    rawnavdata = (
        rawnavdata
        .loc[rawnavdata.route.str.isin(analysis_routes)]
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
    )
    # TOOD: not working
    data_joined = (
        pd.merge_asof(
            data, 
            tagline_data
            .filter(['tag_key', 'index_run_start_original'], axis = "columns")
            .sort_values(['index_run_start_original']), 
            left_on="index_loc", 
            right_on="index_run_start_original", 
            direction='forward'
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
            direction='backward'
        )
        .rename(columns = {'tag_key':'tag_key_end'})
        .drop(columns = ['index_run_end_original'])
    )
    
    breakpoint()
    

    return data