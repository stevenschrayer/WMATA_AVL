# -*- coding: utf-8 -*-
"""
Created on Wed July 22 02:45:31 2021

@author: WylieTimmerman

We'll use this to test out some new options for decomposing the bus's movement
"""

# % Environment Setup
import os, sys, pandas as pd, pyarrow.parquet as pq

from dotenv import dotenv_values
import pyarrow as pa

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

# Globals
tsp_route_list = ['30N','30S','33','31']
analysis_routes = ['30N']
analysis_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
# EPSG code for WMATA-area work
wmata_crs = 2248

import wmatarawnav as wr

# % Reload Data
# %% Rawnav Data
rawnav_raw = pd.DataFrame()

for yr in [
    '202102'#,
    # '202103',
    # '202104',
    # '202105'
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
                "pattern",
                "start_date_time"
            ],
            axis = "columns"
        )
    )

    rawnav_raw = rawnav_raw.append(rawnav_raw_temp, ignore_index = True)

del rawnav_raw_temp 

# %% Run the Movement Decomp

# Apply Amit's approach on finding start of trip instance.




