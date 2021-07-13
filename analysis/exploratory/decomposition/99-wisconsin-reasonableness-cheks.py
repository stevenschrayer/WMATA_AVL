# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

# Libraries
import os, sys, glob, pandas as pd
from dotenv import dotenv_values
import pyarrow.parquet as pq
import numpy as np

# Paths
os.environ["GDAL_DATA"] = os.environ["CONDA_PREFIX"] + "\Library\share\gdal"
path_working = r"C:\Users\WylieTimmerman\Documents\projects_local\WMATA_AVL_datamart"
os.chdir(os.path.join(path_working))
sys.path.append(r"C:\Users\WylieTimmerman\Documents\projects_local\WMATA_AVL_datamart")
path_sp = r"C:\OD\Foursquare ITP\Projects - WMATA Datamart\Task 3 - Bus Priority"
path_source_data = os.path.join(path_sp,"data","00-Raw")
path_processed_data = os.path.join(path_sp, "data","02-Processed")
# Server credentials
config = dotenv_values(os.path.join(path_working, '.env'))

# Globals
wmata_crs = 2248

# Load wmatarawnav library
import wmatarawnav as wr

rawnav_run_decomp = (
    pq.read_table(
        source=os.path.join(path_sp,"data","01-Interim","wisconsin_decomp_mt.parquet"),
        use_pandas_metadata = True
    )
    .to_pandas()
    # As a bit of proofing, we confirm this is int32
    .reset_index()
)

# We parsed through the end of may, but want a month and a half on each end only.
rawnav_run_decomp = (
    rawnav_run_decomp
    .loc[(rawnav_run_decomp.start_date_time.dt.tz_localize(None) < np.datetime64('2021-04-30'))]

)