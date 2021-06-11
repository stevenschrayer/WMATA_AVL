# -*- coding: utf-8 -*-
"""
Created on Fri Jun 11 05:10:18 2021

@author: WylieTimmerman


"""
# %% Setup
import os, sys
from dotenv import dotenv_values
import pg8000.native


if not sys.warnoptions:
    import warnings

    warnings.simplefilter("ignore")  # Stop Pandas warnings

if os.getlogin() == "WylieTimmerman":
    # Working Paths

    path_working = r"C:\Users\WylieTimmerman\Documents\projects_local\WMATA_AVL_datamart"
    os.chdir(os.path.join(path_working))
    sys.path.append(r"C:\Users\WylieTimmerman\Documents\projects_local\WMATA_AVL_datamart")
    path_sp = r"C:\OD\Foursquare ITP\Foursquare ITP SharePoint Site - Shared Documents\WMATA Datamart Program Support\Task 3 - Bus Priority"
    path_source_data = os.path.join(r"C:\Downloads")
    path_processed_data = os.path.join(path_sp, "Data", "02-Processed") 
    config = dotenv_values(os.path.join(path_working, '.env'))

else:
    raise FileNotFoundError("Define the path_working, path_source_data, gtfs_dir, \
                            ZippedFilesloc, and path_processed_data in a new elif block")
# Globals

con = pg8000.native.Connection(
        host = config['pg_host'],
        database = config['pg_db'],
        user = config['pg_user'],
        password = config['pg_pass'],
        port = config['pg_port'],
        ssl_context = (config['pg_sslmode'] == 'require')
)

# Define the tables
# %% The Rawnav data ###

tablename = 'rawnav'
# Note: I think we could define index_run_start as integer, and indeed we might need to.
# but python/numpy is picky about na's in integers, so may be better to leave NA for now.
statement = (
    """
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        'filename' TEXT,
        'index_run_start' DOUBLE PRECISION,
        'index_run_end' DOUBLE PRECISION,
        'index_loc' DOUBLE PRECISION,
        'lat' DOUBLE PRECISION,
        'long' DOUBLE PRECISION,
        'heading' DOUBLE PRECISION,
        'door_state' TEXT,
        'veh_state' TEXT,
        'odom_ft' DOUBLE PRECISION,
        'sec_past_st' DOUBLE PRECISION,
        'sat_cnt' DOUBLE PRECISION,
        'stop_window' TEXT,
        'blank' DOUBLE PRECISION,
        'lat_raw' DOUBLE PRECISION,
        'long_raw' DOUBLE PRECISION,
        'row_before_apc' DOUBLE PRECISION,
        'route_pattern' TEXT,
        'pattern' DOUBLE PRECISION,
        'start_date_time' TIMESTAMPTZ
        PRIMARY KEY (filename, index_run_start)
    )
    """
    .format(
        schema = config['pg_schema'],
        table = tablename  # is this confusing because it matches db name? ohopefully note
    )
)



con.run(statement)

statement = (
  "ALTER TABLE {schema}.{table} OWNER TO {groupname}"
  .format(
           schema = config['pg_schema'],
           table = tablename,
           groupname = config['pg_group']
   )
)

con.run(statement)

#update the index
statement = (
    """
    CREATE INDEX {indexname} 
           ON {schema}.{table} ("filename", "index_run_start")
    """
  .format(
      indexname = (tablename + "_fileindex_" + "idx"),
      schema = config['pg_schema'],
      table = tablename
   )
)

con.run(statement)

statement = (
    """
    CREATE INDEX {indexname} 
           ON {schema}.{table} ("route")
    """
  .format(
      indexname = (tablename + "_route_" + "idx"),
      schema = config['pg_schema'],
      table = tablename
   )
)

con.run(statement)

# %% Summary Table
tablename = 'rawnav_summary'

statement = (
    """
    CREATE TABLE IF NOT EXISTS "rawnav_summary" (
      'fullpath' TEXT,
      'filename' TEXT,
      'file_busid' INTEGER,
      'file_id' TEXT,
      'taglist' TEXT,
      'route_pattern' TEXT,
      'tag_busid' DOUBLE PRECISION,
      'pattern' INTEGER,
      'start_date_time' TIMESTAMPTZ,
      'end_date_time' TIMESTAMPTZ,
      'index_run_start_original' INTEGER,
      'index_run_start' DOUBLE PRECISION,
      'index_run_end_original' DOUBLE PRECISION,
      'index_run_end' DOUBLE PRECISION,
      'sec_start' DOUBLE PRECISION,
      'odom_ft_start' DOUBLE PRECISION,
      'sec_end' DOUBLE PRECISION,
      'odom_ft_end' DOUBLE PRECISION,
      'run_duration_from_sec' INTEGER,
      'run_duration_from_tags' TEXT,
      'dist_odom_mi' DOUBLE PRECISION,
      'mph_odom' DOUBLE PRECISION,
      'mph_run_tag' DOUBLE PRECISION,
      'dist_crow_fly_mi' DOUBLE PRECISION,
      'lat_start' DOUBLE PRECISION,
      'long_start' DOUBLE PRECISION,
      'lat_end' DOUBLE PRECISION,
      'long_end' DOUBLE PRECISION
      PRIMARY KEY (filename, index_run_start)
)
    """
    .format(
        schema = config['pg_schema'],
        table = tablename  # is this confusing because it matches db name? ohopefully note
    )
)

statement = (
  "ALTER TABLE {schema}.{table} OWNER TO {groupname}"
  .format(
           schema = config['pg_schema'],
           table = tablename,
           groupname = config['pg_group']
   )
)

con.run(statement)

#update the index
statement = (
    """
    CREATE INDEX {indexname} 
           ON {schema}.{table} ("filename", "index_run_start")
    """
  .format(
      indexname = (tablename + "_fileindex_" + "idx"),
      schema = config['pg_schema'],
      table = tablename
   )
)

con.run(statement)

statement = (
    """
    CREATE INDEX {indexname} 
           ON {schema}.{table} ("route")
    """
  .format(
      indexname = (tablename + "_route_" + "idx"),
      schema = config['pg_schema'],
      table = tablename
   )
)

con.run(statement)

con.close()