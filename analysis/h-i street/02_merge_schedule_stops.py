# -*- coding: utf-8 -*-
"""
Create by: abibeka, wytimmerman, jmcdowell
Purpose: Merge wmata_schedule and rawnav data
Created on Wed Jun 16 2021
"""

# 1 Import Libraries and Set Global Parameters
########################################################################################################################
# 1.1 Import Python Libraries
############################################
import pyarrow as pa
import pyarrow.parquet as pq
import shutil
import os, sys, pandas as pd, geopandas as gpd

# For postgresql
from dotenv import dotenv_values

# 1.2 Set Global Parameters
############################################
if os.getlogin() == "WylieTimmerman":
    # Working Paths
    path_working = r"C:\Users\WylieTimmerman\Documents\projects_local\WMATA_AVL_datamart"
    os.chdir(os.path.join(path_working))
    sys.path.append(r"C:\Users\WylieTimmerman\Documents\projects_local\WMATA_AVL_datamart")
    path_sp = r"C:\OD\Foursquare ITP\Projects - WMATA Datamart\Task 3 - Bus Priority"
    path_source_data = os.path.join(path_sp,"Data","00-raw")
    path_processed_data = os.path.join(path_sp, "Data","02-processed")
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
hi_routes = ['30N','30S','32','33','36','37','39','42','43','G8']
analysis_routes = hi_routes
# EPSG code for WMATA-area work
wmata_crs = 2248

# 1.3 Import User-Defined Package
############################################
import wmatarawnav as wr

# Read the Wmata_Schedule data
wmata_schedule_dat = (
    wr.read_sched_db_patterns(
        path = os.path.join(
            path_source_data,
            "Schedule_082719-201718.mdb"
        ),
        analysis_routes = analysis_routes
    )
)

wmata_schedule_gdf = (
    gpd.GeoDataFrame(
        wmata_schedule_dat, 
        geometry = gpd.points_from_xy(wmata_schedule_dat.stop_lon,wmata_schedule_dat.stop_lat),
        crs='EPSG:4326'
    )
    .to_crs(epsg=wmata_crs)
)

# Make Output Directory
path_stop_index = os.path.join(path_processed_data, "stop_index_hi.parquet")

if not os.path.isdir(path_stop_index):
    os.mkdir(path_stop_index)

for analysis_route in analysis_routes:
    print("*" * 100)
    print('Processing analysis route {}'.format(analysis_route))
    
    rawnav_route = (
        pq.read_table(
            source = os.path.join(path_processed_data,"rawnav_data_hi.parquet"),
            filters = [('route', '=', analysis_route)],
            use_pandas_metadata = True
        )
        .to_pandas()
        # Correcting for weirdness when writing to/ returning from parquet
        .assign(
            route = lambda x: x.route.astype(str),
            pattern = lambda x: x.pattern.astype('int32', errors = "ignore")
        )
    )
    
    rawnav_route_gdf = (
        gpd.GeoDataFrame(
            rawnav_route, 
            geometry = gpd.points_from_xy(rawnav_route.long,rawnav_route.lat),
            crs='EPSG:4326'
        )
        .to_crs(epsg=wmata_crs)
    )
        
    # Find rawnav point nearest each stop
    nearest_rawnav_point_to_wmata_schedule_dat = (
        wr.merge_rawnav_target(
            target_dat = wmata_schedule_gdf,
            rawnav_dat = rawnav_route_gdf,
            quiet = False
        )
        .reset_index(
            drop = True
        )
    )
    
    # Assert and clean stop data
    nearest_rawnav_point_to_wmata_schedule_dat = (
        wr.remove_stops_with_dist_over_100ft(
            nearest_rawnav_point_to_wmata_schedule_dat
        )
    )

    # From schedule DB we don't add 1 here
    nearest_rawnav_point_to_wmata_schedule_dat['stop_sequence'] = nearest_rawnav_point_to_wmata_schedule_dat['stop_sort_order'] 

    stop_index = (
        wr.assert_clean_stop_order_increase_with_odom(nearest_rawnav_point_to_wmata_schedule_dat)
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
                
    pq.write_to_dataset(
        table = pa.Table.from_pandas(stop_index),
        root_path = path_stop_index,
        partition_cols = ['route']
    )

print("*" * 100)
