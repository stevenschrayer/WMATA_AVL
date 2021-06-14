# -*- coding: utf-8 -*-
"""
Created on Mon Jun 14 09:00:19 2021

@author: WylieTimmerman
"""

# %% Setup
import os, sys
from dotenv import dotenv_values
import pandas as pd
import pg8000.native # not strictly required to load, but is used by sqlalchemy below
import sqlalchemy


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
# Connect
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

# %% Pull
# Note: With a little more effort, we can incorporate sqlalchemy more fully, but this is 
# just a proof of concept
test_pull = (
    pd.read_sql(
        sql = "SELECT * FROM wisconsin_tsp.rawnav LIMIT 100",
        con = engine
    )
)            
           
# %% Close out 
engine.dispose()