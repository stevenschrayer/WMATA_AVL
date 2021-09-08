# -*- coding: utf-8 -*-
"""
Create by: abibeka, wytimmerman, jmcdowell
Purpose: Merge wmata_schedule and rawnav data
Created on Wed Jun 16 2021
"""

# 0 Housekeeping. Clear variable space
########################################################################################################################
from IPython import get_ipython  # run magic commands
ipython = get_ipython()
ipython.magic("reset -f")
ipython = get_ipython()
#https://stackoverflow.com/questions/36572282/ipython-autoreload-magic-function-not-found
ipython.magic("load_ext autoreload")
ipython.magic("autoreload 2")

# 1 Import Libraries and Set Global Parameters
########################################################################################################################
# 1.1 Import Python Libraries
############################################
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import shutil
print("Run Section 1 Import Libraries and Set Global Parameters...")
begin_time = datetime.now()
import os, sys, pandas as pd, geopandas as gpd

# For postgresql
from dotenv import dotenv_values
import pg8000.native # not strictly required to load, but is used by sqlalchemy below
import sqlalchemy

if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")  # Stop Pandas warnings

# 1.2 Set Global Parameters
############################################
if os.getlogin() == "WylieTimmerman":
    # Working Paths
    path_working = r"C:\OD\OneDrive - Foursquare ITP\Projects\WMATA_AVL"
    os.chdir(os.path.join(path_working))
    sys.path.append(r"C:\OD\OneDrive - Foursquare ITP\Projects\WMATA_AVL")
    path_sp = r"C:\Users\WylieTimmerman\Documents\projects_local\wmata_avl_local"
    path_source_data = os.path.join(path_sp,"data","00-raw")
    path_processed_data = os.path.join(path_sp, "data","02-processed")
elif os.getlogin() == "abibeka":
    # Working Paths
    path_working = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\Github\WMATA_AVL"
    os.chdir(os.path.join(path_working))
    sys.path.append(path_working)
    # Source data
    path_source_data = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\WMATA-AVL\Data"
    # Processed data
    path_processed_data = os.path.join(path_source_data, "ProcessedData")
elif os.getlogin() == "JackMcDowell":
    # Working paths
    path_working = r"C:\OD\OneDrive - Foursquare ITP\Documents\0002 R\WMATA Datamart\WMATA_AVL"
    os.chdir(os.path.join(path_working))
    sys.path.append(r"C:\OD\OneDrive - Foursquare ITP\Documents\0002 R\WMATA Datamart\WMATA_AVL")
    path_sp = r"C:\OD\Foursquare ITP\Foursquare ITP SharePoint Site - Shared Documents\WMATA Datamart Program Support\Task 3 - Bus Priority"
    # Data paths
    path_source_data = os.path.join(r"C:\Users\JackMcDowell\Downloads")
    path_processed_data = os.path.join(path_sp, "Data", "02-Processed") 
    # Server credentials
    config = dotenv_values(os.path.join(path_working, '.env'))
else:
    raise FileNotFoundError("Define the path_working, path_source_data, and"
                            " path_processed_data in a new elif block")

# Globals
q_jump_route_list = ['30N','30S','33','31']
analysis_routes = q_jump_route_list
analysis_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
# EPSG code for WMATA-area work
wmata_crs = 2248

# Connect to postgres
sqlUrl = sqlalchemy.engine.url.URL(
    drivername="postgresql+pg8000",
    username=config['pg_user'],
    password=config['pg_pass'],
    host=config['pg_host'],
    port= config['pg_port'],
    database=config['pg_db']
)

engine = (
    sqlalchemy.create_engine(
        sqlUrl, 
        connect_args={"ssl_context": True}, 
        echo = True
    )
)

# 1.3 Import User-Defined Package
############################################
import wmatarawnav as wr

executionTime = str(datetime.now() - begin_time).split('.')[0]
print("Run Time Section 1 Import Libraries and Set Global Parameters : {}".format(executionTime))
print("*" * 100)

# 2 Read, analyze and summarize Schedule data
########################################################################################################################
print("Run Section 2: Read, analyze and summarize rawnav, WMATA schedule data...")
begin_time = datetime.now()
# Read the Wmata_Schedule data
wmata_schedule_dat = wr.read_sched_db_patterns(
    path = os.path.join(path_source_data,
                        "Schedule_082719-201718.mdb"),
    analysis_routes = analysis_routes)

wmata_schedule_gdf = (
    gpd.GeoDataFrame(
        wmata_schedule_dat, 
        geometry = gpd.points_from_xy(wmata_schedule_dat.stop_lon,wmata_schedule_dat.stop_lat),
        crs='EPSG:4326'
    )
    .to_crs(epsg=wmata_crs)
)

# Make Output Directory
path_stop_summary = os.path.join(path_processed_data, "stop_summary.parquet")
if not os.path.isdir(path_stop_summary):
    os.mkdir(path_stop_summary)

path_stop_index = os.path.join(path_processed_data, "stop_index.parquet")
if not os.path.isdir(path_stop_index):
    os.mkdir(path_stop_index)

for analysis_route in analysis_routes:
    print("*" * 100)
    print('Processing analysis route {}'.format(analysis_route))
                
    # Pull data from SQL server
    try:
        rawnav_dat_query = "SELECT * FROM wisconsin_tsp.rawnav WHERE \"route_pattern\" ~ \' {}".format(analysis_route) + ".{2}\'"
        rawnav_dat = (
            pd.read_sql(
                sql = rawnav_dat_query,
                con = engine
            )
            .drop(columns=['blank', 'lat_raw', 'long_raw', 'sat_cnt'])
        )
            
        rawnav_dat['route'] = analysis_route
        
    except Exception as e:
        print(e)  # usually no data found or something similar

    else:

        rawnav_summary_dat_query = "SELECT * FROM wisconsin_tsp.rawnav_summary WHERE \"route_pattern\" ~ \' {}".format(analysis_route) + ".{2}\'"
        rawnav_summary_dat = (
            pd.read_sql(
                sql = rawnav_summary_dat_query,
                con = engine
            )
        )
            
        rawnav_summary_dat['route'] = analysis_route

        # Subset Rawnav Data to Records Desired
        rawnav_summary_dat = rawnav_summary_dat.query('not (run_duration_from_sec < 600 | dist_odom_mi < 2)')
        
        rawnav_summary_keys_col = rawnav_summary_dat[['filename', 'index_run_start']]
        
        rawnav_qjump_dat = rawnav_dat.merge(rawnav_summary_keys_col,
                                            on=['filename', 'index_run_start'],
                                            how='right')

        rawnav_qjump_gdf = (
            gpd.GeoDataFrame(
                rawnav_qjump_dat,
                geometry=gpd.points_from_xy(rawnav_qjump_dat.long, rawnav_qjump_dat.lat),
                crs='EPSG:4326'
            )
            .to_crs(epsg=wmata_crs)
        )

        
    # Find rawnav point nearest each stop
    nearest_rawnav_point_to_wmata_schedule_dat = (
        wr.merge_rawnav_target(
            target_dat=wmata_schedule_gdf,
            rawnav_dat=rawnav_qjump_gdf)
    )
    
    # Trialing resetting index as suggested by Benjamin Malnor
    # In general indices past the initial read-in don't matter much, so this seems like a safe
    # way of addressing the issue he hit
    nearest_rawnav_point_to_wmata_schedule_dat.reset_index(drop=True, inplace=True)

    # Assert and clean stop data
    nearest_rawnav_point_to_wmata_schedule_dat = (
        wr.remove_stops_with_dist_over_100ft(nearest_rawnav_point_to_wmata_schedule_dat)
    )

    nearest_rawnav_point_to_wmata_schedule_dat['stop_sequence'] = nearest_rawnav_point_to_wmata_schedule_dat['stop_sort_order'] + 1

    stop_index = (
        wr.assert_clean_stop_order_increase_with_odom(nearest_rawnav_point_to_wmata_schedule_dat)
    )

    # Generate Summary
    stop_summary = wr.include_wmata_schedule_based_summary(
        rawnav_q_dat=rawnav_qjump_gdf,
        rawnav_sum_dat=rawnav_summary_dat,
        nearest_stop_dat=stop_index
    )
    
    stop_summary = wr.add_num_missing_stops_to_sum(
        rawnav_wmata_schedule_dat = stop_index,
        wmata_schedule_dat_ = wmata_schedule_gdf,
        wmata_schedule_based_sum_dat_=stop_summary
    )
    
    
    if type(stop_summary) == type(None):
        print('No data on analysis route {}'.format(analysis_route))
        
    
    # Write Summary Table 
    shutil.rmtree(
        os.path.join(
            path_stop_summary,
            "route={}".format(analysis_route)
        ),
        ignore_errors=True
    ) 
    
    pq.write_to_dataset(
        table=pa.Table.from_pandas(stop_summary),
        root_path=path_stop_summary,
        partition_cols=['route']
    )
    
    # Write Index Table
    shutil.rmtree(
        os.path.join(
            path_stop_index,
            "route={}".format(analysis_route)
        ),
        ignore_errors=True
    ) 
    
    stop_index = wr.drop_geometry(stop_index)
    
#    stop_index = stop_index.assign(wday=analysis_day)
            
    pq.write_to_dataset(
        table=pa.Table.from_pandas(stop_index),
        root_path=path_stop_index,
        partition_cols=['route']
    )

executionTime = str(datetime.now() - begin_time).split('.')[0]
print(
      "Run Time Section Section 2: Read, analyze and summarize rawnav, WMATA schedule data : {}"
      .format(executionTime)
)
print("*" * 100)
