# -*- coding: utf-8 -*-
"""
Create by: jmcdowell
Purpose: Calculate reliability metrics for decomposed rawnav data
Created on Thu Jul 22 2021
"""

# %% Import libraries

# Libraries
import os, sys, pandas as pd
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
)


# %% Reliability by route-pattern

# Need to calculate stdev, mean, median, 95pct, MAD

rely_stats_wisc_route = (
    rawnav_run_decomp_2_fil
    # Sum runtimes per trip
    .groupby(['filename','index_run_start','route','overall_dir','basic_decomp','full_decomp'])
    .agg(
        secs_tot = ('secs_tot', 'sum')
    )
    .reset_index()
    # Calculate metrics on trip-level runtimes
    .groupby(['route','overall_dir','basic_decomp','full_decomp'])
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

# %% Reliability by stop segment

rely_stats_wisc_stop = (
    rawnav_run_decomp_2_fil
    # Sum runtimes per stop segment
    .groupby(['filename','index_run_start','route','overall_dir','trip_seg','basic_decomp','full_decomp'])
    .agg(
        secs_tot = ('secs_tot', 'sum')
    )
    .reset_index()
    # Calculate metrics on stop segment runtimes
    .groupby(['route','overall_dir','trip_seg','basic_decomp','full_decomp',])
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



# %% Visualize

rely_stats_wisc_route.to_csv('rely_stats_wisc_route.csv')











