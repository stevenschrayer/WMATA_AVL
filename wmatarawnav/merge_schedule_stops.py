# -*- coding: utf-8 -*-
"""
Create by: abibeka, wytimmerman
Created on Tue Apr 28 15:07:59 2020
Purpose: Functions for processing rawnav & wmata_schedule data
"""
import os
import inflection
import pandas as pd
import pyodbc
import fiona
import geopandas as gpd
from shapely.geometry import Point
from shapely.geometry import LineString
import numpy as np
import folium
from folium import plugins
import re
from . import low_level_fns as ll


def read_sched_db_patterns(path,
                           analysis_routes,
                           UID="",
                           PWD=""):
    """
    Parameters
    ----------
    path: str,
        full path to the wmata schedule db
    analysis_routes: list,
        list of route names to filter schedule db to
    UID: str,
        user id used to access db, if needed
    PWD: str,
        password used to access db, if needed
    Returns
    -------
    wmata_schedule_dat: pd.DataFrame, wmata_schedule data
    """
    # Open Connection
    pyodbc_available_drivers = [x for x in pyodbc.drivers() if x.startswith('Microsoft')]
    if 'Microsoft Access Driver (*.mdb, *.accdb)' not in pyodbc_available_drivers:
        link1 = "https://ginesys.atlassian.net/wiki/spaces/PUB/pages/66617405/You+cannot+install+the+64-bit+version+of+Microsoft+Access+Database+Engine+because+you+currently+have+32-bit+Office+Product+installed+-+Error+message+shows+when+user+tries+to+install+a+64-bit+version+of+Microsoft+Access+Database+Engine"
        link2 = "https://knowledge.autodesk.com/support/autocad/learn-explore/caas/sfdcarticles/sfdcarticles/How-to-install-64-bit-Microsoft-Database-Drivers-alongside-32-bit-Microsoft-Office.html"
        raise pyodbc.InterfaceError("Likely issue with access database engine. Try the following links:\n"
                                    "link1 = {},\nlink2 = {}".format(link1,link2))
  
    cnxn = pyodbc.connect(r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + path + \
                          r';UID="' + UID + \
                          r'";PWD="' + PWD + r'";')

    # Load Tables
    # NOTE: The creation of the table returned by this function could largely be done with SQL, 
    # but we instead just load the tables as dataframes for some of the convenience of working
    # with Python and pandas syntax.
    with cnxn:
        stop_dat = pd.read_sql("SELECT * FROM Stop",
                               cnxn)

        pattern_dat = pd.read_sql("SELECT * FROM Pattern",
                                  cnxn)

        pattern_detail_dat = pd.read_sql("SELECT * FROM PatternDetail",
                                         cnxn)

        # We let Pandas close the connection automatically

    # Lightly Clean Tables
    stop_dat = stop_dat.dropna(axis=1)
    stop_dat.columns = [inflection.underscore(col_nm) for col_nm in stop_dat.columns]
    stop_dat.rename(
        columns={'longitude': 'stop_lon',
                 'latitude': 'stop_lat',
                 'heading': 'stop_heading'}, inplace=True)

    pattern_dat = pattern_dat[['PatternID', 'TARoute', 'PatternName', 'Direction',
                               'Distance', 'CDRoute', 'CDVariation', 'PatternDestination',
                               'RouteText', 'RouteKey', 'PubRouteDir', 'DirectionID']]
    pattern_dat.columns = [inflection.underscore(col_nm) for col_nm in pattern_dat.columns]
    pattern_dat.cd_route = pattern_dat.cd_route.astype(str).str.strip()
    pattern_dat.cd_variation = pattern_dat.cd_variation.astype('int32')
    pattern_dat.rename(columns={
        'cd_route': 'route',
        'cd_variation': 'pattern',
        'distance': 'trip_length'},
        inplace=True)

    pattern_detail_dat = pattern_detail_dat[pattern_detail_dat.TimePointID.isna()]
    pattern_detail_dat = pattern_detail_dat.drop(columns=['SortOrder', 'GeoPathID', 'TimePointID'])
    pattern_detail_dat.columns = [inflection.underscore(col_nm) for col_nm in pattern_detail_dat.columns]
    pattern_detail_dat.rename(columns={'distance': 'dist_from_previous_stop'}, inplace=True)

    # Filter to Relevant Routes
    q_jump_route_list = analysis_routes
    pattern_q_jump_route_dat = pattern_dat.query('route in @q_jump_route_list')
    if set(pattern_q_jump_route_dat.route.unique()) != set(q_jump_route_list):
        miss_routes = set(q_jump_route_list) - set(pattern_q_jump_route_dat.route.unique())
        print("Schedule data does not include the following route(s): " + miss_routes)

    # Join tables
    pattern_pattern_detail_stop_q_jump_route_dat = (
        pattern_q_jump_route_dat
        .merge(pattern_detail_dat, on='pattern_id', how='left')
        .merge(stop_dat, on='geo_id', how='left')
    )

    (pattern_pattern_detail_stop_q_jump_route_dat
     .sort_values(by=['route', 'pattern', 'order'], inplace=True)
    )

    # Check for Missing Lat Long In Stops   
    mask_nan_latlong = (
        pattern_pattern_detail_stop_q_jump_route_dat[['stop_lat', 'stop_lon']].isna().all(axis=1)
    )
    
    assert_stop_sort_order_zero_has_nan_latlong = (
        sum(pattern_pattern_detail_stop_q_jump_route_dat[mask_nan_latlong].stop_sort_order - 0)
    )
        
    assert (assert_stop_sort_order_zero_has_nan_latlong == 0), \
        print("Missing LatLong values found for stops, please address in source database")

    # Ensure Table Values are Consistent and then Drop Superfluous Cols
    assert (0 == sum(~ pattern_pattern_detail_stop_q_jump_route_dat.
                     eval('''direction==pub_route_dir& route==ta_route''')))
    pattern_pattern_detail_stop_q_jump_route_dat.drop(columns=['pub_route_dir', 'ta_route'], inplace=True)

    return pattern_pattern_detail_stop_q_jump_route_dat


def merge_rawnav_wmata_schedule(analysis_route_,
                                analysis_day_,
                                rawnav_dat_,
                                rawnav_sum_dat_,
                                wmata_schedule_dat_):
    """
    Parameters
    ----------
    analysis_route_: str,
        route for which rawnav data is analyzed. Should be an element of:
        ['S1', 'S2', 'S4', 'S9', '70', '79', '64', 'G8', 'D32', 'H1', 'H2', 'H3', 'H4',
                         'H8','W47']
    analysis_day_: str,
        day for which rawnav data is analyzed. Should be an element of:
        ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
    rawnav_dat_: pd.DataFrame, rawnav data
    rawnav_sum_dat_: pd.DataFrame, rawnav summary data
    wmata_schedule_dat_: pd.DataFrame, wmata schedule data
    Returns
    -------
    wmata_schedule_based_sum_dat: pd.DataFrame
        trip summary data with additional information from wmata schedule data
    nearest_rawnav_point_to_wmata_schedule_correct_stop_order_dat: gpd.GeoDataFrame
        cleaned data on nearest rawnav point to wmata schedule data where
            - stops whose ordering does not correspond to the index_loc/ time/ odometer are removed i.e. stops are
            removed if  order does not increase with index_loc or time or distance.
            - where all stops with closest rawnav point > 100 ft. are removed.
    """

    # Filter to relevant inputs
    rawnav_subset_dat = rawnav_dat_.query('route==@analysis_route_ & wday==@analysis_day_')
    rawnav_sum_subset_dat = rawnav_sum_dat_.query('route==@analysis_route_ & wday==@analysis_day_')

    if (rawnav_sum_subset_dat.shape[0] == 0): return None, None

    # Find rawnav point nearest each stop
    nearest_rawnav_point_to_wmata_schedule_dat = (
        merge_rawnav_target(
            target_dat=wmata_schedule_dat_,
            rawnav_dat=rawnav_subset_dat)
    )
    
    # Trialing resetting index as suggested by Benjamin Malnor
    # In general indices past the initial read-in don't matter much, so this seems like a safe
    # way of addressing the issue he hit
    nearest_rawnav_point_to_wmata_schedule_dat.reset_index(drop=True, inplace=True)

    # Assert and clean stop data
    nearest_rawnav_point_to_wmata_schedule_dat = (
        remove_stops_with_dist_over_100ft(nearest_rawnav_point_to_wmata_schedule_dat)
    )

    nearest_rawnav_point_to_wmata_schedule_correct_stop_order_dat = (
        assert_clean_stop_order_increase_with_odom(nearest_rawnav_point_to_wmata_schedule_dat)
    )

    # Generate Summary
    wmata_schedule_based_sum_dat = include_wmata_schedule_based_summary(
        rawnav_q_dat=rawnav_subset_dat,
        rawnav_sum_dat=rawnav_sum_subset_dat,
        nearest_stop_dat=nearest_rawnav_point_to_wmata_schedule_correct_stop_order_dat
    )
    
    wmata_schedule_based_sum_dat = add_num_missing_stops_to_sum(
        rawnav_wmata_schedule_dat = nearest_rawnav_point_to_wmata_schedule_correct_stop_order_dat,
        wmata_schedule_dat_ = wmata_schedule_dat_,
        wmata_schedule_based_sum_dat_=wmata_schedule_based_sum_dat
    )
    
    #do we need to set this index?
    # wmata_schedule_based_sum_dat.set_index(
    #     ['fullpath', 'filename', 'file_id', 'wday', 'start_date_time', 'end_date_time',
    #      'index_run_start', 'taglist', 'route_pattern', 'route', 'pattern'],
    #     inplace=True,
    #     drop=True)
    
    return wmata_schedule_based_sum_dat, nearest_rawnav_point_to_wmata_schedule_correct_stop_order_dat


def add_num_missing_stops_to_sum(rawnav_wmata_schedule_dat, 
                                 wmata_schedule_dat_,
                                 wmata_schedule_based_sum_dat_):

    rawnav_wmata_schedule_num_stops = (
        rawnav_wmata_schedule_dat
        .groupby(['filename', 'index_run_start'])
        .agg(
            route=('route','first'),
            pattern=('pattern','first'),
            num_stops_in_run=('stop_id','count')
        )
        .reset_index()
    )
            
    rawnav_wmata_schedule_num_stops = pd.DataFrame(rawnav_wmata_schedule_num_stops)
        
    wmata_schedule_stops_all = (
        wmata_schedule_dat_
        .groupby(['route','pattern'])
        .agg(wmata_stops_all=('stop_id','count'))
        .reset_index()
    )
    
    rawnav_wmata_schedule_num_stops = (
        rawnav_wmata_schedule_num_stops.merge(wmata_schedule_stops_all,
                                              on=['route','pattern'],
                                              how='left')
    )
    
    rawnav_wmata_schedule_num_stops = (
        rawnav_wmata_schedule_num_stops
        .assign(num_missing_stops=lambda x: x.wmata_stops_all - x.num_stops_in_run)
    )
    
    wmata_schedule_based_sum_dat_with_missing_stop = (
        wmata_schedule_based_sum_dat_.merge(rawnav_wmata_schedule_num_stops,
                                            on=['filename',
                                                'index_run_start',
                                                'route',
                                                'pattern'],
                                            how='left')
    )
    
    return wmata_schedule_based_sum_dat_with_missing_stop


def merge_rawnav_target(target_dat, rawnav_dat, quiet=True):
    """
    Parameters
    ----------
    target_dat : gpd.GeoDataFrame
        wmata schedule data with unique stops per route and info on short/long and direction.
    rawnav_dat :gpd.GeoDataFrame
        rawnav data.
    Returns
    -------
    nearest_rawnav_point_to_target_data : gpd.GeoDataFrame
        A geopandas dataframe with nearest rawnav point to each of the wmata 
        schedule stops on that route.
    """

    assert (bool(re.search("US survey foot", target_dat.crs.to_wkt()))),\
        print('Need a CRS with feet as units')
    assert (bool(re.search("US survey foot", rawnav_dat.crs.to_wkt()))),\
        print('Need a CRS with feet as units')
    assert (target_dat.crs == rawnav_dat.crs), print("CRS must match between objects")

    # Iterate over groups of routes and patterns in rawnav data and target object
    target_groups = target_dat.groupby(['route', 'pattern'])
    rawnav_groups = rawnav_dat.groupby(
        ['route', 'pattern', 'filename', 'index_run_start'])

    nearest_rawnav_point_to_target_dat = pd.DataFrame()
    
    for name, rawnav_group in rawnav_groups:
        try:
            target_dat_relevant = (
                target_groups
                .get_group(
                    (name[0], name[1])
                )
            )
            
            nearest_rawnav_point_to_target_dat = pd.concat([
                nearest_rawnav_point_to_target_dat,
                ll.ckdnearest(target_dat_relevant, rawnav_group)
            ])
            
        except:
            if (quiet == False):
                print("No target geometry found for {} - {}".format(name[0],name[1]))
    
    nearest_rawnav_point_to_target_dat = (
        ll.reorder_first_cols(
            nearest_rawnav_point_to_target_dat,
            ['filename',
             'index_run_start',
             'index_loc'
             ]
        )
    )

    return(nearest_rawnav_point_to_target_dat)


def remove_stops_with_dist_over_100ft(nearest_rawnav_point_to_wmata_schedule_data_):
    """
    Parameters
    ----------
    nearest_rawnav_point_to_wmata_schedule_data_: gpd.GeoDataFrame
        A geopandas dataframe with nearest rawnav point to each of the wmata schedule stops on that route.
    Returns
    -------
    nearest_rawnav_point_to_wmata_schedule_data_: gpd.GeoDataFrame
        cleaned data on nearest rawnav point to wmata schedule data where all stops with closest rawnav point > 100 ft.
        are removed.
    """
    row_before = nearest_rawnav_point_to_wmata_schedule_data_.shape[0]
    
    nearest_rawnav_point_to_wmata_schedule_data_ = (
        nearest_rawnav_point_to_wmata_schedule_data_
        .query('dist_to_nearest_point < 100')
    )

    row_after = nearest_rawnav_point_to_wmata_schedule_data_.shape[0]
    
    row_diff = row_before - row_after    
    print('deleted {} rows of {} rows with distance to the nearest stop > 100 ft. from index table'
          .format(row_diff,row_before)
    )
    return nearest_rawnav_point_to_wmata_schedule_data_


def remove_stops_with_dist_over_threshold(nearest_rawnav_point_to_wmata_schedule_data_, threshold):
    """
    Parameters
    ----------
    nearest_rawnav_point_to_wmata_schedule_data_: gpd.GeoDataFrame
        A geopandas dataframe with nearest rawnav point to each of the wmata schedule stops on that route.
    Returns
    -------
    nearest_rawnav_point_to_wmata_schedule_data_: gpd.GeoDataFrame
        cleaned data on nearest rawnav point to wmata schedule data where all stops with closest rawnav point > 100 ft.
        are removed.
    """
    row_before = nearest_rawnav_point_to_wmata_schedule_data_.shape[0]

    nearest_rawnav_point_to_wmata_schedule_data_ = (
        nearest_rawnav_point_to_wmata_schedule_data_
        .query(f'dist_to_nearest_point < {threshold}')
    )

    row_after = nearest_rawnav_point_to_wmata_schedule_data_.shape[0]

    row_diff = row_before - row_after
    print('deleted {} rows of {} rows with distance to the nearest stop > 100 ft. from index table'
          .format(row_diff,row_before)
    )
    return nearest_rawnav_point_to_wmata_schedule_data_


def assert_clean_stop_order_increase_with_odom(nearest_rawnav_point_to_wmata_schedule_data_):
    """
    Parameters
    ----------
    nearest_rawnav_point_to_wmata_schedule_data_: gpd.GeoDataFrame
        cleaned data on nearest rawnav point to wmata schedule data where all stops with
        closest rawnav point > 100 ft. are removed.
    Returns
    -------
    nearest_rawnav_point_to_wmata_schedule_data_: gpd.GeoDataFrame
        cleaned data on nearest rawnav point to wmata schedule data where
            - stops whose ordering does not correspond to the index_loc/ time/ odometer are removed 
              i.e. stops are removed if  order does not increase with index_loc or time or distance.
            - where all stops with closest rawnav point > 100 ft. are removed.
    """
    #NOTE - BAM - I changed "stop_sort_order" to "stop_sequence" sorting below
    # this should be equivalent except stop_sequence starts at 1 instead of 0
    row_before = nearest_rawnav_point_to_wmata_schedule_data_.shape[0]
    nearest_rawnav_point_to_wmata_schedule_data_. \
        sort_values(['filename', 'index_run_start', 'stop_sequence'], inplace=True)
    assert (nearest_rawnav_point_to_wmata_schedule_data_.duplicated(
        ['filename', 'index_run_start', 'stop_sequence']).sum() == 0)
    while (sum(nearest_rawnav_point_to_wmata_schedule_data_.
                       groupby(['filename', 'index_run_start']).index_loc.diff().dropna() < 0) != 0):
        nearest_rawnav_point_to_wmata_schedule_data_ = \
            delete_rows_with_incorrect_stop_order(nearest_rawnav_point_to_wmata_schedule_data_)
    row_after = nearest_rawnav_point_to_wmata_schedule_data_.shape[0]
    row_diff = row_before - row_after
    print('deleted {} of {} stops with incorrect order from index table'.format(row_diff,row_before))
    return nearest_rawnav_point_to_wmata_schedule_data_


def delete_rows_with_incorrect_stop_order(nearest_rawnav_point_to_wmata_schedule_data_):
    """
    delete stop where the index location does not increase with stop order
    Parameters
    ----------
    nearest_rawnav_point_to_wmata_schedule_data_: gpd.GeoDataFrame
    Returns
    -------
    nearest_rawnav_point_to_wmata_schedule_data_: gpd.GeoDataFrame
        input data with the offending row deleted.
    """

    nearest_rawnav_point_to_wmata_schedule_data_.loc[:, 'diff_index'] = \
        nearest_rawnav_point_to_wmata_schedule_data_.groupby(['filename', 'index_run_start']). \
            index_loc.diff().fillna(0)
                        
    wrong_snapping_dat = nearest_rawnav_point_to_wmata_schedule_data_.query('diff_index<0')
    nearest_rawnav_point_to_wmata_schedule_data_ = (
        nearest_rawnav_point_to_wmata_schedule_data_
        .query('diff_index>=0')
        .drop(columns = ['diff_index'])
    )
    return nearest_rawnav_point_to_wmata_schedule_data_


def include_wmata_schedule_based_summary(rawnav_q_dat, rawnav_sum_dat, nearest_stop_dat):
    '''
    Parameters
    ----------
    rawnav_q_dat: pd.DataFrame, rawnav data 
    rawnav_sum_dat: pd.DataFrame, rawnav summary data
    nearest_stop_dat: gpd.GeoDataFrame
        cleaned data on nearest rawnav point to wmata schedule data where
            - stops whose ordering does not correspond to the index_loc/ time/ odometer are removed i.e. stops are
            removed if  order does not increase with index_loc or time or distance.
            - where all stops with closest rawnav point > 100 ft. are removed.
    Returns
    -------
    rawnav_q_stop_sum_dat: pd.DataFrame
        trip summary data with additional information from wmata schedule data
    '''
    first_last_stop_dat = get_first_last_stop_rawnav(nearest_stop_dat)
    
    rawnav_q_stop_dat = (
        rawnav_q_dat
        .merge(first_last_stop_dat
               .drop(['odom_ft','sec_past_st'], axis = 1),
               on=['filename', 'index_run_start'], 
               how='right')
    )
    
    rawnav_q_stop_dat = (
        rawnav_q_stop_dat
        .query('index_loc >= index_loc_first_stop & index_loc <= index_loc_last_stop')
    )
    
    #removed heading from the column list - it does not get aggregated
    rawnav_q_stop_dat = (
        rawnav_q_stop_dat[
            ['filename', 
             'index_run_start', 
             'lat', 
             'long',
             'odom_ft', 
             'sec_past_st',
             'first_stop_dist_nearest_point', 
             'trip_length', 
             'route_text',
             'pattern_name', 
             'direction',
             'pattern_destination', 
             'direction_id']
        ]    
    )
        
    Map1 = lambda x: max(x) - min(x)
    
    rawnav_q_stop_sum_dat = (
        rawnav_q_stop_dat
        .groupby(['filename', 'index_run_start'])
        .agg({'odom_ft': ['min', 'max', Map1],
              'sec_past_st': ['min', 'max', Map1],
              'lat': ['first', 'last'],
              'long': ['first', 'last'],
              'first_stop_dist_nearest_point': ['first'],
              'trip_length': ['first'],
              'route_text': ['first'],
              'pattern_name': ['first'],
              'direction': ['first'],
              'pattern_destination': ['first'],
              'direction_id': ['first']})
    )
    
    rawnav_q_stop_sum_dat.columns = ["_".join(x) for x in rawnav_q_stop_sum_dat.columns.ravel()]

    rawnav_q_stop_sum_dat = (
        rawnav_q_stop_sum_dat
        .rename(columns = {'odom_ft_min':'start_odom_ft_wmata_schedule',
                           'odom_ft_max':'end_odom_ft_wmata_schedule',
                           'odom_ft_<lambda_0>':'run_dist_mi_odom_wmata_schedule', 
                           'sec_past_st_min':'start_sec_wmata_schedule',
                           'sec_past_st_max':'end_sec_wmata_schedule', 
                           'sec_past_st_<lambda_0>':'run_dur_sec_wmata_schedule',
                           'lat_first':'start_lat_wmata_schedule', 
                           'lat_last':'end_lat_wmata_schedule',
                           'long_first':'start_long_wmata_schedule', 
                           'long_last': 'end_long_wmata_schedule',
                           'first_stop_dist_nearest_point_first':'dist_first_stop_wmata_schedule', 
                           'trip_length_first':'trip_dist_mi_direct_wmata_schedule',
                           'route_text_first':'route_text_wmata_schedule', 
                           'pattern_name_first':'pattern_name_wmata_schedule',
                           'direction_first':'direction_wmata_schedule', 
                           'pattern_destination_first':'pattern_destination_wmata_schedule',
                           'direction_id_first':'direction_id_wmata_schedule'})
    )
        
    # Mutate columns and add original summary information
    rawnav_q_stop_sum_dat = (
        rawnav_q_stop_sum_dat
        .assign(
            run_dist_mi_odom_wmata_schedule = lambda x: round(x.run_dist_mi_odom_wmata_schedule / 5280, 2),
            trip_dist_mi_direct_wmata_schedule = lambda x: round(x.trip_dist_mi_direct_wmata_schedule / 5280, 2),
            dist_first_stop_wmata_schedule = lambda x: round(x.dist_first_stop_wmata_schedule, 2),
            trip_speed_mph_wmata_schedule = lambda x: round(
                3600 * 
                x.run_dist_mi_odom_wmata_schedule /
                x.run_dur_sec_wmata_schedule, 
                2)
        )
        .merge(rawnav_sum_dat, on=['filename', 'index_run_start'], how='left')
    )

    return rawnav_q_stop_sum_dat


def get_first_last_stop_rawnav(nearest_rawnav_stop_dat):
    '''

    Parameters
    ----------
    nearest_rawnav_stop_dat: gpd.GeoDataFrame
        cleaned data on nearest rawnav point to wmata schedule data where
            - stops whose ordering does not correspond to the index_loc/ time/ odometer are removed i.e. stops are
            removed if  order does not increase with index_loc or time or distance.
            - where all stops with closest rawnav point > 100 ft. are removed.
    Returns
    -------
    first_last_stop_dat: pd.DataFrame
        first and last stop information for a trip.
    '''
    last_stop_dat = nearest_rawnav_stop_dat.copy()
    
    last_stop_dat.loc[:, "tempCol"] = (
        last_stop_dat
        .groupby(['filename', 'index_run_start'])
        .index_loc
        .transform(max)
    )
        
    last_stop_dat = (
        last_stop_dat
        .query('index_loc==tempCol')
        .reset_index(drop=True)
        .filter(items=['filename', 'index_run_start', 'index_loc', 'dist_to_nearest_point'])
        .rename(columns={'index_loc': 'index_loc_last_stop',
                         'dist_to_nearest_point': 'last_stop_dist_nearest_point'})
    )

    first_stop_dat = nearest_rawnav_stop_dat.copy()
    
    first_stop_dat.loc[:, "tempCol"] = ( 
        first_stop_dat
        .groupby(['filename', 'index_run_start'])
        .index_loc
        .transform(min)
    )
    
    first_stop_dat = (
        first_stop_dat
        .query('index_loc==tempCol')
        .reset_index(drop=True)
        .drop(columns='tempCol')
        .rename(
            columns={'index_loc': 'index_loc_first_stop',
                     'dist_to_nearest_point': 'first_stop_dist_nearest_point'
            }
        )
        .sort_values(['filename', 'index_run_start'])
    )
    
    first_last_stop_dat = (
        first_stop_dat
        .merge(
            last_stop_dat, 
            on=['filename', 'index_run_start'],
            how='left'
        )
        .drop(columns=['geometry', 'lat', 'long', 'pattern', 'route'])
    )
        
    return first_last_stop_dat


def make_target_rawnav_linestring(index_table):
    '''
    Parameters
    ----------
    index_table: pd.DataFrame
        rawnav idnex data with cols lat, long, stop_lon, stop_lat
    Returns
    -------
    index_table_mod: gpd.DataFrame
        rawnav index table with linestring geometry between rawnav point and nearest
    Notes
    -----
    This function creates a linestring geometry between the rawnav point and target point 
    for visualization of the merge process. 
    '''
    # Note: Alternate approach here provided by Benjamin Malnor, could implement.
    # geometry = (
    #     index_table
    #     .loc[:,['stop_lat','stop_lon','lat','long']]
    #     .apply(lambda x: 
    #         LineString(
    #             [
    #             Point(x['long'],x['lat']),
    #             Point(x['stop_lon'],x['stop_lat'])
    #             ]
    #         ), 
    #         axis=1
    #     )
    #     .tolist()
    # )
    # index_table_mod = gpd.GeoDataFrame(index_table, crs="EPSG:4326",geometry=geometry)
    
    geometry_nearest_rawnav_point = (
        gpd.points_from_xy(
            index_table.long,
            index_table.lat
        )
    )


    geometry_stop_on_route = (
        gpd.points_from_xy(
            index_table.stop_lon,
            index_table.stop_lat
        )
    )

    geometry = [LineString(list(xy)) for xy in zip(geometry_nearest_rawnav_point, geometry_stop_on_route)]

    index_table_mod = (
        gpd.GeoDataFrame(
            index_table,
            geometry=geometry,
            crs='EPSG:4326'
        )
    )

    return index_table_mod


def plot_rawnav_trajectory_with_wmata_schedule_stops(rawnav_dat, index_table_line):
    '''
    Parameters
    ----------
    rawnav_dat: pd.DataFrame
        rawnav data
    index_table_line: gpd.GeoDataFrame
        cleaned data on nearest rawnav point to wmata schedule data where
            - stops whose ordering does not correspond to the index_loc/ time/ odometer are removed i.e. stops are
            removed if  order does not increase with index_loc or time or distance.
            - where all stops with closest rawnav point > 100 ft. are removed.
            - line to nearest stop is geometry
    Returns
    -------
    this_map: folium.Map
        map of rawnav trajectory and stops with nearest rawnav point
    Notes 
    -----
    See documentation of use in the methodology notebook.
    '''

    ## Link to Esri World Imagery service plus attribution
    # https://www.esri.com/arcgis-blog/products/constituent-engagement/constituent-engagement/esri-world-imagery-in-openstreetmap/
    esri_imagery = "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"
    esri_attribution = \
        "Tiles &copy; Esri &mdash; Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, " \
        "UPR-EGP, and the GIS User Community"
    this_map = folium.Map(tiles='cartodbdark_matter', zoom_start=16, max_zoom=25, control_scale=True)
    folium.TileLayer(name="EsriImagery", tiles=esri_imagery, attr=esri_attribution,
                     zoom_start=16, max_zoom=25, control_scale=True).add_to(this_map)
    folium.TileLayer('cartodbpositron', zoom_start=16, max_zoom=20, control_scale=True).add_to(this_map)
    folium.TileLayer('openstreetmap', zoom_start=16, max_zoom=20, control_scale=True).add_to(this_map)
    fg = folium.FeatureGroup(name="Rawnav Pings")
    this_map.add_child(fg)
    line_grp = folium.FeatureGroup(name="WMATA Schedule Stops and Nearest Rawnav Pings")
    this_map.add_child(line_grp)
    plot_marker_clusters(this_map, rawnav_dat, "lat", "long", fg)
    plot_lines_clusters(this_map, index_table_line, line_grp)
    lat_longs = [[x, y] for x, y in zip(rawnav_dat.lat, rawnav_dat.long)]
    this_map.fit_bounds(lat_longs)
    folium.LayerControl(collapsed=True).add_to(this_map)
    return (this_map)


def plot_marker_clusters(this_map, dat, lat, long, feature_grp, fill_color='#e942f5'):
    '''
    Plot rawnav trajectory points.
    Parameters
    ----------
    this_map: folium.Map
        folium base map with feature groups.
    dat: pd.DataFrame
        rawnav data
    lat: str
        latitude column name
    long: str
        longitude column name
    feature_grp : folium.plugin.FeatureGroup
        feature group used for rawnav trajectory
    fill_color : default pink :)
    Returns
    -------
    None
    '''

    popup_field_list = list(dat.columns)
    for i, row in dat.iterrows():
        label = '<br>'.join([field + ': ' + str(row[field]) for field in popup_field_list])
        # https://deparkes.co.uk/2019/02/27/folium-lines-and-markers/
        folium.CircleMarker(
            location=[row[lat], row[long]], radius=2,
            popup=folium.Popup(html=label, parse_html=False, max_width='200'),
            fill=True, fill_color=fill_color, color=fill_color).add_to(feature_grp)


def plot_lines_clusters(this_map, dat, feature_grp):
    '''
    Plot stops along a route with a line to the closest rawnav point
    Parameters
    ----------
    this_map: folium.Map
        folium base map with feature groups.
    dat : gpd.GeoDataFrame
        cleaned data on nearest rawnav point to wmata schedule data where
            - stops whose ordering does not correspond to the index_loc/ time/ odometer are removed 
            i.e. stops are  removed if  order does not increase with index_loc or time or distance.
            - where all stops with closest rawnav point > 100 ft. are removed.
    feature_grp : folium.plugin.FeatureGroup
        feature group used for stops with nearest rawnav point.
    Returns
    -------
    None.
    '''
    popup_field_list = list(dat.columns)
    popup_field_list.remove('geometry')
    for i, row in dat.iterrows():
        temp_grp = \
            plugins.FeatureGroupSubGroup(feature_grp,
                                         "{}-{}-{}".format(row.stop_sort_order,
                                                           row.geo_description,
                                                           row.pattern))
        this_map.add_child(temp_grp)
        label = '<br>'.join([field + ': ' + str(row[field]) for field in popup_field_list])
        # https://deparkes.co.uk/2019/02/27/folium-lines-and-markers/
        line_points = [(tuples[1], tuples[0]) for tuples in list(row.geometry.coords)]
        folium.PolyLine(line_points, color="red", weight=4, opacity=1 \
                        , popup=folium.Popup(html=label, parse_html=False, max_width='300')).add_to(temp_grp)
