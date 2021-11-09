# -*- coding: utf-8 -*-
"""
Create by: abibeka, wytimmerman, jmcdowell
Purpose: Merge wmata_schedule and rawnav data
Created on Mon Nov 01 2021
"""

# Import libraries
import pyarrow as pa
import pyarrow.parquet as pq
import shutil
import os
import sys
import pandas as pd
import geopandas as gpd
import numpy as np
import cx_Oracle
# For postgresql
from dotenv import dotenv_values

# Set directory and import project package
path_working = r"C:\Users\C053460\OneDrive - WMATA\Documents\code\WMATA Datamart\WMATA_AVL"
os.chdir(os.path.join(path_working))
import wmatarawnav as wr

# For Oracle
os.environ['PATH'] = 'C:\\Users\\C053460\\oracle\\instantclient_19_11;C:\\Users\\C053460\\Anaconda3\\envs\\wmatarawnavproj;C:\\Users\\C053460\\Anaconda3\\envs\\wmatarawnavproj\\Library\\mingw-w64\\bin;C:\\Users\\C053460\\Anaconda3\\envs\\wmatarawnavproj\\Library\\usr\\bin;C:\\Users\\C053460\\Anaconda3\\envs\\wmatarawnavproj\\Library\\bin;C:\\Users\\C053460\\Anaconda3\\envs\\wmatarawnavproj\\Scripts;C:\\Users\\C053460\\Anaconda3\\envs\\wmatarawnavproj\\bin;C:\\Users\\C053460\\Anaconda3\\condabin;C:\\Users\\C053460\\Anaconda3;C:\\Users\\C053460\\Anaconda3\\Library\\mingw-w64\\bin;C:\\Users\\C053460\\Anaconda3\\Library\\usr\\bin;C:\\Users\\C053460\\Anaconda3\\Library\\bin;C:\\Users\\C053460\\Anaconda3\\Scripts;C:\\WINDOWS\\system32;C:\\WINDOWS;C:\\WINDOWS\\System32\\Wbem;C:\\WINDOWS\\System32\\WindowsPowerShell\\v1.0;C:\\Program Files (x86)\\Pulse Secure\\VC142.CRT\\X64;C:\\Program Files (x86)\\Pulse Secure\\VC142.CRT\\X86;C:\\Users\\C053460\\AppData\\Local\\Microsoft\\WindowsApps;.'

wmata_crs = 2248

# %% Connect to PLANAPI

# Create dsn
dsn_plan = cx_Oracle.makedsn('jgx4-scan', 
                             '1521', 
                             service_name="NCSDPRD1.wmata.com")

# Connect
conn_plan = cx_Oracle.connect(user=os.getenv("PLAN_DB_USER"), 
                              password=os.getenv("PLAN_DB_PASS"), 
                              dsn=dsn_plan)


# %% Get version info

# This is a small table, so we can pull the whole thing

# Pull version ID dates
query = '''select
versionid, versionname, version_start_date, version_end_date 
from PLANAPI.BUS_SCHED_VERSION_V'''

bus_sched_version = pd.read_sql_query(query, conn_plan)


# %% Pull PLANAPI data

# Get dates from rawnav
start_date = ping_facts['start_date'].min()
# TODO: update to use end_date
end_date = ping_facts['start_date'].max()

# Get patterns from rawnav
routes_list = ping_facts['route'].drop_duplicates().tolist()
patterns_list = ping_facts['route_pattern'].drop_duplicates().tolist()

# Get version IDs for dates in rawnav file
bus_sched_version_file = (
    bus_sched_version
    .loc[bus_sched_version['VERSION_END_DATE'] > start_date]
    .loc[bus_sched_version['VERSION_START_DATE'] < end_date]
    )

bus_sched_df = []

for pattern_id in patterns_list:
    
    query = f'''with version_id_range as (
    select versionid from PLANAPI.BUS_SCHED_VERSION_V
    where versionid IN ({bus_sched_version_file['VERSIONID'].astype(str).str.cat(sep = ", ")}))
    select 
    seq.versionid, seq.route, seq.variation as pattern, seq.pattern_id, seq.directiondescription direction, seq.routename route_text
    , seq.routevarname pattern_name, seq.geostopid geoid, seq.stopid stop_id, seq.directionid direction_id, route.pattern_destination
    ,seq.geostopdescription as geo_description, seq.latitude stop_lat, seq.longitude stop_lon, seq.stop_sequence, route.pattern_distance trip_length
    from PLANAPI.BUS_SCHED_STOP_SEQUENCE_V seq 
    join version_id_range rang on (rang.versionid = seq.versionid)
    left join planapi.bus_sched_route_v route on (seq.versionid = route.versionid and seq.routevarid = route.ROUTEVARID)
    where seq.PATTERN_ID = '{pattern_id.strip()}' '''

    # Send query to database
    bus_sched_df_temp = pd.read_sql_query(query, conn_plan)
    
    bus_sched_df.append(bus_sched_df_temp)
    del bus_sched_df_temp

bus_sched_df = pd.concat(bus_sched_df, ignore_index = True)


# %% Fix duplicated stops

# we can still run into cases where the same stop is repeated twice in the stop sequence order.
# this can cause some havoc in the matching process, so we filter these out early.
# a little worried that this renders us incompatible with other WMATA data sources, so maybe
# should think on this more.
bus_sched_df['NEXT_STOP_ID'] = (
    bus_sched_df
    .groupby(['VERSIONID', 'PATTERN_ID', 'DIRECTION'])['STOP_ID']
    .shift(-1)
)

# Drop duplicate stops
bus_sched_df = (
    bus_sched_df
    .loc[(bus_sched_df.STOP_ID != bus_sched_df.NEXT_STOP_ID)]
)

# Update stop sequence
bus_sched_df['STOP_SEQUENCE'] = (
    bus_sched_df
    .groupby(['VERSIONID', 'PATTERN_ID', 'DIRECTION'])
    .cumcount() + 1
)

# Drop column
bus_sched_df = (
    bus_sched_df
    .drop(['NEXT_STOP_ID'], axis="columns")
)

# Change column names to lowercase
bus_sched_df.columns=bus_sched_df.columns.str.lower()


# %% Create spatial dataframe

bus_sched_gdf = (
    gpd.GeoDataFrame(
        bus_sched_df,
        geometry=gpd.points_from_xy(bus_sched_df.stop_lon, bus_sched_df.stop_lat),
        crs='EPSG:4326'
    )
    .to_crs(epsg=wmata_crs)
)

# # updates to start date are because the intervals are overlapping, this is a little hack to
# # Set them straight
# wmata_schedule_versions = (
#     bus_sched_df
#     .filter(['VERSIONID', 'VERSIONNAME', 'VERSION_START_DATE', 'VERSION_END_DATE'])
#     .drop_duplicates()
#     .assign(
#         VERSION_START_DATE=lambda x:
#             pd.to_datetime(x.VERSION_START_DATE),
#         VERSION_END_DATE=lambda x: pd.to_datetime(x.VERSION_END_DATE)
#     )
#     .assign(
#         VERSION_START_DATE=lambda x:
#             np.where(
#                 x.VERSIONID == 70,
#                 x.VERSION_START_DATE + pd.Timedelta(hours=4, seconds=2),
#                 x.VERSION_START_DATE
#             )
#     )
# )
    
    
    
# %% Match to rawnav data

# Initialize output
stop_index = []

for analysis_route in routes_list:
    print("*" * 100)
    print('Processing analysis route {}'.format(analysis_route))

    for sched_version in bus_sched_df['versionid'].drop_duplicates():
        # Subset schedule data
        sched_start_date = bus_sched_version['VERSION_START_DATE'].loc[
            bus_sched_version['VERSIONID'] == sched_version].iloc[0]
        sched_end_date = bus_sched_version['VERSION_END_DATE'].loc[
            bus_sched_version['VERSIONID'] == sched_version].iloc[0]
    
        wmata_schedule_version_gdf = (
            bus_sched_gdf
            .loc[(bus_sched_gdf['versionid'] == sched_version) &
                 (bus_sched_gdf['route'] == analysis_route)]
            .rename(columns=str.lower)
        )
    
        rawnav_route = (
            ping_facts
            .query('route == @analysis_route')
        )
    
        rawnav_route = (
            rawnav_route
            .loc[
                (rawnav_route['instance_timestamp'] >= sched_start_date) &
                (rawnav_route['instance_timestamp'] < sched_end_date)
            ]
        )
    
        rawnav_route_gdf = (
            gpd.GeoDataFrame(
                rawnav_route,
                geometry=gpd.points_from_xy(rawnav_route.longitude, rawnav_route.latitude),
                crs='EPSG:4326'
            )
            .to_crs(epsg=wmata_crs)
        )
        
        # Find rawnav point nearest each stop
        nearest_rawnav_point_to_wmata_schedule_dat = (
            wr.merge_rawnav_target(
                target_dat=bus_sched_gdf,
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
    
        # Print if there is no data
        if type(stop_index_temp) == type(None):
            print('No data on analysis route {}'.format(analysis_route))
        del stop_index_temp
        
        # Drop geometry and append to outputs for this route
        stop_index_temp = wr.drop_geometry(stop_index_temp)
        stop_index.append(stop_index_temp)
        
        del stop_index_temp
    
    print("*" * 100)
    
stop_index = pd.concat(stop_index, ignore_index = True)

    
# %% Disconnect from PLANAPI, no longer need it

conn_plan.close()
