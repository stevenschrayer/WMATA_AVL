# -*- coding: utf-8 -*-
"""
Created on Thu Aug 12 11:36:10 2021

@author: WylieTimmerman
"""


# % Environment Setup
import os, sys, pandas as pd, numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
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
    path_processed_data = os.path.join(path_sp, "Data","02-Processed")
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

# %% Reload the data

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
    )

    rawnav_raw = rawnav_raw.append(rawnav_raw_temp, ignore_index = True)

del rawnav_raw_temp 

# Reload the used wisconsin stops
rawnav_used_trips = (
    pq.read_table(
        source = os.path.join(path_sp,"data","01-Interim","wisconsin_decomp_mt_used_trips.parquet"),
        use_pandas_metadata = True
    )
    .to_pandas()
    .reset_index()
)

# Filter the rawnav data to those used trips

rawnav_fil = (
    rawnav_raw
    .pipe(
        wr.semi_join,
        right = rawnav_used_trips,
        on = ['filename','index_run_start']
    )    
)

# %% Filter to waht we had before

# TODO: replace with amit's methods
rawnav_fil2 = (
    rawnav_fil
    # maybe we look a little bit closer at the southbound trips only
    # i think 30N02 is southbound
    .query('pattern == 2')
    # for now, reset odometers to 1000 ft after start 
    .query("odom_ft >= 1000")
)

rawnav_fil3 = wr.reset_odom(rawnav_fil2)

# %% filter to cases with repeated observations

rawnav_fil3[['odom_ft_next','sec_past_st_next']] = (
    rawnav_fil3
    .groupby(['filename','index_run_start'], sort = False)[['odom_ft','sec_past_st']]
    .transform(lambda x: x.shift(-1))
)

rawnav_rep = (
    rawnav_fil3
    .assign(
            odom_ft_marg = lambda x: x.odom_ft_next - x.odom_ft,
            dupes = lambda x:
                x.duplicated(
                    subset = ['filename','index_run_start','sec_past_st'],
                    keep = False
            )
    )
    .query('dupes == True')
)

rawnav_rep_agg = (
    rawnav_rep
    .groupby(['filename','index_run_start','sec_past_st'], sort = False)
    .agg(
        mean = ('odom_ft','mean'),
        sd = ('odom_ft','std'),
        odom_range = ('odom_ft', lambda x: x.max() - x.min()),
        marg_mean = ('odom_ft_marg','mean'),
        marg_sd = ('odom_ft_marg','std'),
    )    
)

rawnav_rep_agg_fil = (
    rawnav_rep_agg
    .query("odom_range < 100")    
)

# seems like the standard deviation is around 12.45 for the within observation differences,
# at least looking at this small sample of things. Seems like that would change a lot depending
# on the speed of the vehicle.
rawnav_rep_agg_fil.odom_range.std()
