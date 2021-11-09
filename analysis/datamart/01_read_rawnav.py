# -*- coding: utf-8 -*-
"""
Create by: jmcdowell
Purpose: Process rawnav data and output summary and processed dataset.
Created on: Tue Nov 02 2021
"""

# Import libraries
import pandas as pd
import os
import cx_Oracle

# cx_Oracle.init_oracle_client(lib_dir=r"C:\Users\C053460\Downloads\instantclient-basic-windows.x64-21.3.0.0.0\instantclient_21_3")
os.environ['PATH'] = 'C:\\Users\\C053460\\oracle\\instantclient_19_11;C:\\Users\\C053460\\Anaconda3\\envs\\wmatarawnavproj;C:\\Users\\C053460\\Anaconda3\\envs\\wmatarawnavproj\\Library\\mingw-w64\\bin;C:\\Users\\C053460\\Anaconda3\\envs\\wmatarawnavproj\\Library\\usr\\bin;C:\\Users\\C053460\\Anaconda3\\envs\\wmatarawnavproj\\Library\\bin;C:\\Users\\C053460\\Anaconda3\\envs\\wmatarawnavproj\\Scripts;C:\\Users\\C053460\\Anaconda3\\envs\\wmatarawnavproj\\bin;C:\\Users\\C053460\\Anaconda3\\condabin;C:\\Users\\C053460\\Anaconda3;C:\\Users\\C053460\\Anaconda3\\Library\\mingw-w64\\bin;C:\\Users\\C053460\\Anaconda3\\Library\\usr\\bin;C:\\Users\\C053460\\Anaconda3\\Library\\bin;C:\\Users\\C053460\\Anaconda3\\Scripts;C:\\WINDOWS\\system32;C:\\WINDOWS;C:\\WINDOWS\\System32\\Wbem;C:\\WINDOWS\\System32\\WindowsPowerShell\\v1.0;C:\\Program Files (x86)\\Pulse Secure\\VC142.CRT\\X64;C:\\Program Files (x86)\\Pulse Secure\\VC142.CRT\\X86;C:\\Users\\C053460\\AppData\\Local\\Microsoft\\WindowsApps;.'


# %% Connect to database

# Create dsn
dsn_rawnav = cx_Oracle.makedsn('jgx2-scan', 
                            '1521', 
                            service_name="BDWQA1.wmata.com")

# Connect
conn_rawnav = cx_Oracle.connect(user="RAWNAV", 
                         password="PrAw_QaA_2021", 
                         dsn=dsn_rawnav)

# %% Import rawnav data

# Get list of processed files
files = pd.read_sql_query('''select * 
                          from FILES
                          where "status" = \'LOADED\'''', conn_rawnav)

# TODO: At some point, compare to logging to only do new files

ping_facts = []

# Loop over files to read in pings and join data
for file_id in files.files_pk:
    print(file_id)
    query = '''select 
        PING_FACTS."ping_facts_pk", 
        PING_FACTS."_file_id", 
        PING_FACTS."line_number", 
        PING_FACTS."_tripInstance_id",
        PING_FACTS."_audit_id", 
        PING_FACTS."latitude", 
        PING_FACTS."longitude", 
        PING_FACTS."heading", 
        PING_FACTS."door_state",
        PING_FACTS."vehicle_state", 
        PING_FACTS."odometer_reading", 
        PING_FACTS."seconds_past_start",
        PING_FACTS."satellite_count", 
        PING_FACTS."stop_window_info",
        PING_FACTS."raw_latitude",
        PING_FACTS."raw_longitude",
        TRIP_INSTANCES."route_pattern", 
        TRIP_INSTANCES."bus_id", 
        TRIP_INSTANCES."start_date", 
        TRIP_INSTANCES."start_time",
        TRIP_INSTANCES."instance_timestamp"
        from TRIP_INSTANCES 
        right join PING_FACTS
        on PING_FACTS."_tripInstance_id" = TRIP_INSTANCES."trip_instances_pk" 
        and PING_FACTS."_file_id" = TRIP_INSTANCES."_file_id"
        where PING_FACTS."_file_id" = {file}'''.format(file = file_id)
        
    ping_facts_temp = pd.read_sql_query(query, conn_rawnav)
    ping_facts.append(ping_facts_temp)
    del ping_facts_temp
    
ping_facts = pd.concat(ping_facts, ignore_index = True)


# %% Remove pull-in/pull-out

ping_facts = (
    ping_facts
    .assign(pi_po = lambda x: x.route_pattern.str.slice(stop = 2))
    .query('pi_po != "PI" & pi_po != "PO"')
    .drop(['pi_po'], axis = "columns")
)

# %% Create additional columns

# need route, pattern, wday, end_date_time, row_before_apc, blank, 
ping_facts['route'] = ping_facts['route_pattern'].str.slice(stop = -2)
ping_facts['pattern'] = pd.to_numeric(ping_facts['route_pattern'].str.slice(start = -2))
ping_facts['wday'] = ping_facts['start_date'].dt.day_name()
# TODO: See if Scott can parse the rest on his end


