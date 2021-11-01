# -*- coding: utf-8 -*-
"""
Created on Mon Jul 12 15:23:50 2021

@author: C053460
"""

# %% Load packages and env variables

import cx_Oracle
import os, sys, os.path
import pandas as pd, numpy as np
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
analysis_routes = ['30N','30S','32','33','42','43']
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
dsn_tns = cx_Oracle.makedsn('jgx4-scan', 
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

# Get version IDs between October 1 and October 31, 2017
bus_sched_version_oct17 = (
    bus_sched_version
    .loc[bus_sched_version['VERSION_END_DATE'] > pd.Timestamp("2017-10-01 00:00:00")]
    .loc[bus_sched_version['VERSION_START_DATE'] < pd.Timestamp("2017-10-31 00:00:00")]
    )

bus_sched_version_oct19 = (
    bus_sched_version
    .loc[bus_sched_version['VERSION_END_DATE'] > pd.Timestamp("2019-10-01 00:00:00")]
    .loc[bus_sched_version['VERSION_START_DATE'] < pd.Timestamp("2019-10-31 00:00:00")]
    )

bus_sched_version_bind = pd.concat([bus_sched_version_oct17, bus_sched_version_oct19])

version_list = bus_sched_version_bind['VERSIONID']

# %% List all routes

query = '''select ROUTE from PLANAPI.BUS_ROUTE_LINE_CORRIDOR'''

all_routes = pd.read_sql_query(query, conn).drop_duplicates()['ROUTE'].to_list()


# %% Pull PLANAPI data

bus_sched_df = pd.DataFrame()

for route in all_routes:
    
    query = f'''with version_id_range as (
    select versionid from PLANAPI.BUS_SCHED_VERSION_V
    where versionid IN ({bus_sched_version_bind['VERSIONID'].astype(str).str.cat(sep = ", ")}))
    select 
    seq.versionid, seq.route, seq.variation as pattern, seq.pattern_id, seq.directiondescription direction, seq.routename route_text
    , seq.routevarname pattern_name, seq.geostopid geoid, seq.stopid stop_id, seq.directionid direction_id, route.pattern_destination
    ,seq.geostopdescription as geo_description, seq.latitude stop_lat, seq.longitude stop_lon, seq.stop_sequence, route.pattern_distance trip_length
    from PLANAPI.BUS_SCHED_STOP_SEQUENCE_V seq 
    join version_id_range rang on (rang.versionid = seq.versionid)
    left join planapi.bus_sched_route_v route on (seq.versionid = route.versionid and seq.routevarid = route.ROUTEVARID)
    where seq.ROUTE = '{route.strip()}' '''

    # Send query to database
    bus_sched_df_temp = pd.read_sql_query(query, conn)
    
    bus_sched_df = bus_sched_df.append(bus_sched_df_temp, ignore_index = True)

del bus_sched_df_temp

# %% Join dates to schedule data

bus_sched_dates_df = pd.merge(
    bus_sched_df,
    bus_sched_version_bind,
    how = "left",
    on = ['VERSIONID']
    )

# %% Export to CSV

bus_sched_dates_df.to_csv(os.path.join(path_processed_data, "schedule_data_allroutes_oct17_oct19.csv"), index=False)


# %% Pull lookup of route and line info from 2017 to present

# ORBCAD_ROUTE_TEXT, LINE_NAMES, ROUTES_PER_LINE, LINE_SCHED_DAYS, ROUTE_SCHEDULE_DAYS, 
# CORRIDOR_ID, CORRIDOR_DESCRIPTION, ROUTES_PER_CORRIDOR, STATUS, COMMENTS, EFFECTIVE_DATE

query = '''select *
from PLANAPI.BUS_NET_ROUTE_LINE_MATRIX'''

# query = '''select *
# from PLANAPI.BUS_SCHED_STOP_SEQUENCE_V
# FETCH FIRST 10 ROWS ONLY'''

bus_route_line = pd.read_sql_query(query, conn)


bus_route_line_summary = (
    bus_route_line
    # .drop(['ORBCAD_ROUTE_TEXT','ROUTE_SCHEDULE_DAYS','COMMENTS',
    #        'CORRIDOR_ID','CORRIDOR_DESCRIPTION','ROUTES_PER_CORRIDOR'],
    #       axis = 'columns')
    # .drop_duplicates()
    .assign(date_dt = lambda x: pd.to_datetime(x.EFFECTIVE_DATE))
    .groupby(['LINE_NAMES','ROUTES_PER_LINE'])['date_dt']
    .agg(start_date = 'min',
         end_date = 'max')
    .reset_index()
    .assign(start_date = lambda x: x.start_date.dt.strftime('%Y-%m-%d'),
            end_date = lambda x:
            np.where(
                x.end_date == pd.to_datetime('2021-09-05'),
                'Current',
                x.end_date.dt.strftime('%Y-%m-%d')
            ))
)
    
    
    
# %% write to csv

bus_route_line.to_csv(os.path.join(path_processed_data, "bus_route_line_history.csv"), index = False)


# %% Close Connection

#close connection
conn.close()