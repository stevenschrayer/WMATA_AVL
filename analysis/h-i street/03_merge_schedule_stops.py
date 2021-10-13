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
import os
import sys
import pandas as pd
import geopandas as gpd
import numpy as np
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
    path_source_data = os.path.join(path_sp, "Data", "00-raw")
    path_processed_data = os.path.join(path_sp, "Data", "02-processed")
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

import wmatarawnav as wr

# Globals
hi_routes = ['30N', '30S', '32', '33', '36', '37', '39', '42', '43', 'G8']
analysis_routes = hi_routes
# EPSG code for WMATA-area work
wmata_crs = 2248

# 1.3 Import User-Defined Package
############################################

# Read the Wmata_Schedule data
wmata_schedule_dat_all = pd.read_csv(
    os.path.join(path_processed_data, "schedule_data_allroutes_oct17_oct19.csv")
)

wmata_schedule_dat_hi = pd.read_csv(
    os.path.join(path_processed_data, "schedule_data_H_I_buslanes.csv")
)

wmata_schedule_dat = (
    pd.concat(
        [
            wmata_schedule_dat_all,
            wmata_schedule_dat_hi
        ]
    )
    # kind of a cheeky way to address the issues above.
    .drop_duplicates(
        subset=['PATTERN_ID', 'VERSIONID', 'STOP_ID', 'STOP_SEQUENCE', 'DIRECTION']
    )
)

# we can still run into cases where the same stop is repeated twice in the stop sequence order.
# this can cause some havoc in the matching process, so we filter these out early.
# a little worried that this renders us incompatible with other WMATA data sources, so maybe
# should think on this more.
wmata_schedule_dat['NEXT_STOP_ID'] = (
    wmata_schedule_dat
    .groupby(['VERSIONID', 'PATTERN_ID', 'DIRECTION'])['STOP_ID']
    .shift(-1)
)

wmata_schedule_dat = (
    wmata_schedule_dat
    .loc[(wmata_schedule_dat.STOP_ID != wmata_schedule_dat.NEXT_STOP_ID)]
)

wmata_schedule_dat['STOP_SEQUENCE'] = (
    wmata_schedule_dat
    .groupby(['VERSIONID', 'PATTERN_ID', 'DIRECTION'])
    .cumcount() + 1
)

wmata_schedule_dat = (
    wmata_schedule_dat
    .drop(['NEXT_STOP_ID'], axis="columns")
)

wmata_schedule_gdf = (
    gpd.GeoDataFrame(
        wmata_schedule_dat,
        geometry=gpd.points_from_xy(wmata_schedule_dat.STOP_LON, wmata_schedule_dat.STOP_LAT),
        crs='EPSG:4326'
    )
    .to_crs(epsg=wmata_crs)
)

# updates to start date are because the intervals are overlapping, this is a little hack to
# Set them straight
wmata_schedule_versions = (
    wmata_schedule_dat
    .filter(['VERSIONID', 'VERSIONNAME', 'VERSION_START_DATE', 'VERSION_END_DATE'])
    .drop_duplicates()
    .assign(
        VERSION_START_DATE=lambda x:
            pd.to_datetime(x.VERSION_START_DATE),
        VERSION_END_DATE=lambda x: pd.to_datetime(x.VERSION_END_DATE)
    )
    .assign(
        VERSION_START_DATE=lambda x:
            np.where(
                x.VERSIONID == 70,
                x.VERSION_START_DATE + pd.Timedelta(hours=4, seconds=2),
                x.VERSION_START_DATE
            )
    )
)
# Make Output Directory
path_stop_index = os.path.join(path_processed_data, "stop_index_matched_hi.parquet")

if not os.path.isdir(path_stop_index):
    os.mkdir(path_stop_index)

for analysis_route in analysis_routes:
    print("*" * 100)
    print('Processing analysis route {}'.format(analysis_route))

    stop_index = pd.DataFrame()

    for sched_version in wmata_schedule_versions['VERSIONID']:
        # Subset schedule data
        sched_start_date = wmata_schedule_versions['VERSION_START_DATE'].loc[
            wmata_schedule_versions['VERSIONID'] == sched_version].iloc[0]
        sched_end_date = wmata_schedule_versions['VERSION_END_DATE'].loc[wmata_schedule_versions['VERSIONID']
                                                                         == sched_version].iloc[0]

        wmata_schedule_version_gdf = (
            wmata_schedule_gdf
            .loc[(wmata_schedule_gdf['VERSIONID'] == sched_version) &
                 (wmata_schedule_gdf['ROUTE'] == analysis_route)]
            .rename(columns=str.lower)
        )

        rawnav_route = (
            pq.read_table(
                source=os.path.join(path_processed_data, "rawnav_matched_hi.parquet"),
                filters=[('route', '=', analysis_route)],
                use_pandas_metadata=True
            )
            .to_pandas()
            # Correcting for weirdness when writing to/ returning from parquet
            .assign(
                route=lambda x: x.route.astype(str),
                pattern=lambda x: x.pattern.astype('int32', errors="ignore")
            )
        )

        rawnav_route = (
            rawnav_route
            .loc[
                (rawnav_route['start_date_time'] >= sched_start_date) &
                (rawnav_route['start_date_time'] < sched_end_date)
            ]
        )

        rawnav_route_gdf = (
            gpd.GeoDataFrame(
                rawnav_route,
                # note we're using the matched points here
                geometry=gpd.points_from_xy(rawnav_route.longmatch, rawnav_route.latmatch),
                crs='EPSG:4326'
            )
            .to_crs(epsg=wmata_crs)
        )
        # Find rawnav point nearest each stop
        nearest_rawnav_point_to_wmata_schedule_dat = (
            wr.merge_rawnav_target(
                target_dat=wmata_schedule_version_gdf,
                rawnav_dat=rawnav_route_gdf)
        )

        # Trialing resetting index as suggested by Benjamin Malnor
        # In general indices past the initial read-in don't matter much, so this seems like a safe
        # way of addressing the issue he hit
        nearest_rawnav_point_to_wmata_schedule_dat.reset_index(drop=True, inplace=True)

        # Assert and clean stop data
        nearest_rawnav_point_to_wmata_schedule_dat = (
            wr.remove_stops_with_dist_over_100ft(nearest_rawnav_point_to_wmata_schedule_dat)
        )

        nearest_rawnav_point_to_wmata_schedule_dat['stop_sort_order'] = (
            nearest_rawnav_point_to_wmata_schedule_dat['stop_sequence'] - 1
        )

        stop_index_temp = (
            wr.assert_clean_stop_order_increase_with_odom(
                nearest_rawnav_point_to_wmata_schedule_dat)
            .assign(versionid=sched_version)
        )

        # Append to outputs for this route
        stop_index = stop_index.append(stop_index_temp)
        if type(stop_index_temp) == type(None):
            print('No data on analysis route {}'.format(analysis_route))
        del stop_index_temp

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
        table=pa.Table.from_pandas(stop_index),
        root_path=path_stop_index,
        partition_cols=['route']
    )

    print("*" * 100)
