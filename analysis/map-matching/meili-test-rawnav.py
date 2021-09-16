# -*- coding: utf-8 -*-
"""
Created on Wed Sep 15 02:53:44 2021

@author: WylieTimmerman
"""
# NOTE: This is python 2.7 only, apparently
# may be better to dump files to CSV and then bring back to parquet later?
# 

# % Environment Setup
import os, sys, pandas as pd, pyarrow.parquet as pq
from datetime import datetime

# For postgresql
# TODO: for now, skipping server, as amit says it's a bit slow
from dotenv import dotenv_values

if os.getlogin() == "WylieTimmerman":
    # Working Paths
    # the following needed to run in vscode jupyter interpreter
    os.environ["GDAL_DATA"] = os.environ["CONDA_PREFIX"] + "\Library\share\gdal"
    path_working = r"C:\Users\WylieTimmerman\Documents\projects_local\WMATA_AVL_datamart"
    os.chdir(os.path.join(path_working))
    sys.path.append(r"C:\Users\WylieTimmerman\Documents\projects_local\WMATA_AVL_datamart")
    path_sp = r"C:\OD\Foursquare ITP\Projects - WMATA Datamart\Task 3 - Bus Priority"
    path_source_data = os.path.join(path_sp,"data","00-Raw")
    path_processed_data = os.path.join(path_sp, "data","02-Processed")
    # Server credentials
    config = dotenv_values(os.path.join(path_working, '.env'))
    # other things for wylie's dev environment            


# EPSG code for WMATA-area work
wmata_crs = 2248

import wmatarawnav as wr

#### Read decomp
rawnav = (
    pq.read_table(
        source=os.path.join(path_processed_data,"decomp_hi.parquet"),
        use_pandas_metadata = True,
        filters = [('route','=','43')]
    )
    .to_pandas()
)

begin_time = datetime.now()  ##
print("Begin Time : {}".format(begin_time))

rawnav_matched = (
    rawnav
    .groupby(['filename','index_run_start'])
    .apply(lambda x: wr.mapmatch(x))    
)

execution_time = str(datetime.now() - begin_time).split('.')[0]
print("map match runtime for route oct17 and oct19 : {}".format(execution_time))

