# -*- coding: utf-8 -*-
"""
Create by: jmcdowell
Purpose: Calculate reliability metrics for decomposed rawnav data
Created on Thu Jul 22 2021
"""

# %% Import libraries

# Libraries
import os, sys, pandas as pd, numpy as np
from dotenv import dotenv_values


# %% Set paths

# Paths
os.environ["GDAL_DATA"] = os.environ["CONDA_PREFIX"] + "\Library\share\gdal"
path_working = r"C:\OD\OneDrive - Foursquare ITP\Documents\0002 R\WMATA Datamart\WMATA_AVL"
os.chdir(os.path.join(path_working))
sys.path.append(r"C:\OD\OneDrive - Foursquare ITP\Documents\0002 R\WMATA Datamart\WMATA_AVL")
path_sp = r"C:\OD\Foursquare ITP\Projects - WMATA Datamart\Task 3 - Bus Priority"
path_source_data = os.path.join(path_sp,"data","00-Raw")
path_processed_data = os.path.join(path_sp, "data","02-Processed")
# Server credentials
config = dotenv_values(os.path.join(path_working, '.env'))

# Globals
wmata_crs = 2248

# Load wmatarawnav library
import wmatarawnav as wr


# %% Read in decomposed rawnav data for Wisconsin Ave corridor

# Only read in data of complete trips
rawnav_run_decomp_2_wisc = pd.read_csv(os.path.join(path_sp,
                                                    "data",
                                                    "01-interim",
                                                    "rawnav_run_decomp_2_wisc.csv"),
    dtype = {'route': str}
    )

# %% Filter data

# Only look at TSP periods and TSP directions
# Filter out segments leading to/from Friendship Heights
rawnav_run_decomp_2_fil = (
    rawnav_run_decomp_2_wisc
    #stop id for friendship heights bay used by these buses; this will remove segments leading to and from as well
    .loc[~rawnav_run_decomp_2_wisc.trip_seg.str.contains('32089', na = False)] 
    .query('tsp_dir_time == True')
    .query('basic_decomp != "End of Trip Pings"')
    .assign(basic_decomp = lambda x: 
                np.where(
                    x.basic_decomp.isin(['<5 mph','>= 5mph']),
                    "In Motion",
                    x.basic_decomp
                )
            )
)


# %% Function to calculate reliability metrics

# Need to calculate stdev, mean, median, 95pct, MAD

def rely_stats(rawnav_decomp,
               grouping_vars):
    
    output = (
        rawnav_decomp
        # Sum runtimes per trip
        .groupby(['filename','index_run_start'] + grouping_vars)
        .agg(
            secs_tot = ('secs_tot', 'sum')
        )
        .reset_index()
        # Calculate metrics on trip-level runtimes
        .groupby(grouping_vars)
        .agg(
            secs_tot_mean = ('secs_tot', 'mean'),
            secs_tot_p50 = ('secs_tot', lambda x: x.quantile(.50)),
            secs_tot_p95 = ('secs_tot', lambda x: x.quantile(.95)),
            secs_tot_stdev = ('secs_tot', 'std'),
            secs_tot_mad = ('secs_tot', 'mad')
        )
        .reset_index()
        .assign(
            secs_tot_cov = lambda x: x.secs_tot_stdev / x.secs_tot_mean,
            secs_tot_buffer = lambda x: (x.secs_tot_p95 - x.secs_tot_mean) / x.secs_tot_mean
        )            
    )
        
    return output
    
    
    
# %% Reliability by route-pattern

grouping_route = ['route','overall_dir','tsp_period','basic_decomp','full_decomp']

rely_stats_wisc_route = rely_stats(rawnav_run_decomp_2_fil, grouping_route)


# %% Reliability by stop segment

grouping_stop_seg = ['route','overall_dir','trip_seg','tsp_period','basic_decomp','full_decomp']

rely_stats_wisc_stop = rely_stats(rawnav_run_decomp_2_fil, grouping_stop_seg)


# %% Visualize

rely_stats_wisc_route.to_csv('rely_stats_wisc_route.csv')











