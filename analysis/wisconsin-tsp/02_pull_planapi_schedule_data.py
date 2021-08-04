# -*- coding: utf-8 -*-
"""
Created on Mon Jul 12 15:23:50 2021

@author: C053460
"""

# %% Load packages and env variables

import cx_Oracle
import os, sys, os.path
import pandas as pd
from dotenv import load_dotenv

# Working path
path_working = r"C:\Users\C053460\OneDrive - WMATA\Documents\code\WMATA Datamart\WMATA_AVL"
os.chdir(os.path.join(path_working))
sys.path.append(r"C:\Users\C053460\OneDrive - WMATA\Documents\code\WMATA Datamart\WMATA_AVL")
path_processed_data = os.path.join(path_working, "data", "02-processed")

# Parameters for oracle db
load_dotenv(os.path.join(path_working, '.env'))
os.environ['PATH'] = 'C:\\Users\\C053460\\oracle\\instantclient_19_11;C:\\Users\\C053460\\Anaconda3\\envs\\wmatarawnavproj;C:\\Users\\C053460\\Anaconda3\\envs\\wmatarawnavproj\\Library\\mingw-w64\\bin;C:\\Users\\C053460\\Anaconda3\\envs\\wmatarawnavproj\\Library\\usr\\bin;C:\\Users\\C053460\\Anaconda3\\envs\\wmatarawnavproj\\Library\\bin;C:\\Users\\C053460\\Anaconda3\\envs\\wmatarawnavproj\\Scripts;C:\\Users\\C053460\\Anaconda3\\envs\\wmatarawnavproj\\bin;C:\\Users\\C053460\\Anaconda3\\condabin;C:\\Users\\C053460\\Anaconda3;C:\\Users\\C053460\\Anaconda3\\Library\\mingw-w64\\bin;C:\\Users\\C053460\\Anaconda3\\Library\\usr\\bin;C:\\Users\\C053460\\Anaconda3\\Library\\bin;C:\\Users\\C053460\\Anaconda3\\Scripts;C:\\WINDOWS\\system32;C:\\WINDOWS;C:\\WINDOWS\\System32\\Wbem;C:\\WINDOWS\\System32\\WindowsPowerShell\\v1.0;C:\\Program Files (x86)\\Pulse Secure\\VC142.CRT\\X64;C:\\Program Files (x86)\\Pulse Secure\\VC142.CRT\\X86;C:\\Users\\C053460\\AppData\\Local\\Microsoft\\WindowsApps;.'

# FITP Sharepoint
path_sp = r"C:\OD\Foursquare ITP\Projects - WMATA Datamart\Task 3 - Bus Priority"
path_source_data = os.path.join(path_sp,"data","00-Raw")
# path_processed_data = os.path.join(path_sp, "data","02-Processed")

# Load wmatarawnav library
import wmatarawnav as wr


# %% Set Wisconsin TSP parameters

# Globals
tsp_route_list = ['30N','30S','33','31']
analysis_routes = tsp_route_list
analysis_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']


# %% Load in rawnav data
rawnav_raw = pd.DataFrame()

for yr in [
    '202102',
    '202103',
    '202104',
    '202105'
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
                # "pattern", #keep pattern
                "start_date_time"
            ],
            axis = "columns"
        )
    )

    rawnav_raw = rawnav_raw.append(rawnav_raw_temp, ignore_index = True)

del rawnav_raw_temp 


# %% Get pattern_ids needed to pull data

patterns_all = (
    rawnav_raw
    .filter(
        ['route','pattern','route_pattern'],
        axis = "columns"
        )
    .drop_duplicates()
    )

patterns_list = patterns_all['route_pattern']


# %% Connect to database

# Create dsn
dsn_tns = cx_Oracle.makedsn('ctx4-scan', 
                            '1521', 
                            service_name="NCSDPRD1.wmata.com")

# Connect
conn = cx_Oracle.connect(user=os.getenv("PLAN_DB_USER"), 
                         password=os.getenv("PLAN_DB_PASS"), 
                         dsn=dsn_tns)


# %% Pull version ID dates

query = '''select
versionid, versionname, version_start_date, version_end_date 
from PLANAPI.BUS_SCHED_VERSION_V'''

bus_sched_version = pd.read_sql_query(query, conn)

# Get version IDs between February 1 and April 30, 2021
bus_sched_version_wisconsin = (
    bus_sched_version
    .loc[bus_sched_version['VERSION_END_DATE'] > pd.Timestamp("2021-02-01 00:00:00")]
    .loc[bus_sched_version['VERSION_START_DATE'] < pd.Timestamp("2021-04-30 00:00:00")]
    )


# %% Pull PLANAPI data

bus_sched_df = pd.DataFrame()

for pattern_id in patterns_list:
    
    query = f'''with version_id_range as (
    select versionid from PLANAPI.BUS_SCHED_VERSION_V
    where versionid IN ({bus_sched_version_wisconsin['VERSIONID'].astype(str).str.cat(sep = ", ")}))
    select 
    seq.versionid, seq.route, seq.variation as pattern, seq.pattern_id, seq.directiondescription direction, seq.routename route_text
    , seq.routevarname pattern_name, seq.geostopid geoid, seq.stopid stop_id, seq.directionid direction_id, route.pattern_destination
    ,seq.geostopdescription as geo_description, seq.latitude stop_lat, seq.longitude stop_lon, seq.stop_sequence, route.pattern_distance trip_length
    from PLANAPI.BUS_SCHED_STOP_SEQUENCE_V seq 
    join version_id_range rang on (rang.versionid = seq.versionid)
    left join planapi.bus_sched_route_v route on (seq.versionid = route.versionid and seq.routevarid = route.ROUTEVARID)
    where seq.PATTERN_ID = '{pattern_id.strip()}' '''

    # Send query to database
    bus_sched_df_temp = pd.read_sql_query(query, conn)
    
    bus_sched_df = bus_sched_df.append(bus_sched_df_temp, ignore_index = True)

del bus_sched_df_temp

# %% Join dates to schedule data

bus_sched_dates_df = pd.merge(
    bus_sched_df,
    bus_sched_version_wisconsin,
    how = "left",
    on = ['VERSIONID']
    )

# %% Export to CSV

bus_sched_dates_df.to_csv(os.path.join(path_processed_data, "schedule_data_wisconsin_tsp.csv"), index=False)


# %% Close Connection

#close connection
conn.close()