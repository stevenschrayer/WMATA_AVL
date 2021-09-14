# -*- coding: utf-8 -*-
"""
Created on Thu Sep  2 02:38:13 2021

@author: WylieTimmerman
"""
from datetime import datetime
import pandas as pd, os, sys, glob, shutil
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import dotenv_values


#### Set Globals
if os.getlogin() == "WylieTimmerman":
    # Working Paths
    # the following needed to run in vscode jupyter interpreter
    os.environ["GDAL_DATA"] = os.environ["CONDA_PREFIX"] + "\Library\share\gdal"
    path_working = r"C:\Users\WylieTimmerman\Documents\projects_local\WMATA_AVL_datamart"
    os.chdir(os.path.join(path_working))
    sys.path.append(r"C:\Users\WylieTimmerman\Documents\projects_local\WMATA_AVL_datamart")
    path_sp = r"C:\OD\Foursquare ITP\Projects - WMATA Datamart\Task 3 - Bus Priority"
    path_source_data = os.path.join(r"C:\Users\WylieTimmerman\Documents\projects_local\wmata_avl_local\data\00-raw")
    path_processed_data = os.path.join(path_sp, "Data","02-Processed")
    # Server credentials
    config = dotenv_values(os.path.join(path_working, '.env'))
else:
    raise FileNotFoundError("Define the path_working, path_source_data, gtfs_dir, \
                            ZippedFilesloc, and path_processed_data in a new elif block")

import wmatarawnav as wr

# Time Indicators
begin_time = datetime.now()  ##
print("Begin Time : {}".format(begin_time))
begin_time = datetime.now()  

#### Define Universe
# Create a list of zipped rawnavfiles (ala 'rawnav06544171027.txt.zip') as 
# file_universe. 
file_universe = glob.glob(os.path.join(path_source_data, "**/*.txt.zip"),recursive=True)

#### Parse Universe
rawnav_inventory = wr.find_rawnav_routes(file_universe, quiet=True)

#### Save Out Inventory
path_rawnav_inventory = os.path.join(path_processed_data,"rawnav_inventory_mult.parquet")
shutil.rmtree(path_rawnav_inventory, ignore_errors=True) 
os.mkdir(path_rawnav_inventory)
    
# Note: partitioning required, using filename avoids resorting of values, filename column
# will be sorted to end on reload however.
rawnav_inventory.to_parquet(
    path = path_rawnav_inventory,
    partition_cols = ['filename'],
    index = False
)

execution_time = str(datetime.now() - begin_time).split('.')[0]
print("Run Time Section 2 Identify Relevant Files for Analysis Routes : {}".format(execution_time))

