# -*- coding: utf-8 -*-
"""
Created on Wed July 22 02:45:31 2021

@author: WylieTimmerman

We'll use this to test out some new options for decomposing the bus's movement
"""

# % Environment Setup
import os, sys, pandas as pd, numpy as np
import shutil
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
hi_routes = ['37','39','42','43','G8','30N','30S','32','33','36']
analysis_routes = hi_routes
# EPSG code for WMATA-area work
wmata_crs = 2248

import wmatarawnav as wr

# Make Output Directory
path_decomp = os.path.join(path_processed_data, "decomp_match_hi.parquet")

if not os.path.isdir(path_decomp):
    os.mkdir(path_decomp)

for analysis_route in analysis_routes:
    print(analysis_route)
    #### Reload the data
    rawnav_route = (
        pq.read_table(
            source = os.path.join(path_processed_data,"rawnav_matched_hi.parquet"),
            filters = [('route', '=', analysis_route)],
            use_pandas_metadata = True
        )
        .to_pandas()
        # Correcting for weirdness when writing to/ returning from parquet
        .assign(
            route = lambda x: x.route.astype(str),
            pattern = lambda x: x.pattern.astype('int32', errors = "ignore"),
            index_run_end = np.nan
        )
    )
     
    # Load the stop data
    stop_index = (
        pq.read_table(
            source=os.path.join(path_processed_data,"stop_index_matched_hi.parquet"),
            filters = [('route', '=', analysis_route)],
            columns = [
                'route',
                'pattern',
                'direction', #same as pattern_name_wmata_schedule
                'stop_id',
                'stop_sequence',
                'filename',
                'index_run_start',
                'index_loc',
                'odom_ft',
                'sec_past_st',
                'geo_description'
            ],
            use_pandas_metadata = True
        )
        .to_pandas()
        .assign(
            route = lambda x: x.route.astype(str),
            pattern = lambda x: x.pattern.astype('int32', errors = "ignore")
        )
        .rename(columns = {'odom_ft' : 'odom_ft_stop'})
        .reset_index()
    )

    #### Start decomposition    
    # aggregate so we only have one observation for each second
    print('agg')
    rawnav_route = wr.agg_sec(rawnav_route)
    
    # This is a separate step again; there are some places where we'll want to interpolate that
    # aren't just the ones where we aggregated seconds. In general, we'll probably want to 
    # revisit the aggregation/interpolation process, so I'm going to try not to touch this too much
    # more for now.
    print('interp')
    rawnav_route = wr.interp_over_sec(rawnav_route)
    print('calc speed')
    rawnav_route = wr.calc_speed(rawnav_route)
    print('smooth speed')
    # this includes calculating the accel and such based on smoothed speed values
    rawnav_route = wr.smooth_speed(rawnav_route)
    print('calc rolling')
    rawnav_route = wr.calc_rolling(rawnav_route,['filename','index_run_start'])
    
    # Add in the decomposition
    print('decompose basic')

    rawnav_route = (
        rawnav_route
        .pipe(
              wr.decompose_mov,
              stopped_fps = 3, #upped from 2
              slow_fps = 14.67, # upped default to 10mph
              steady_accel_thresh = 2 #based on some casual observations
         )
    )
    
    # Identify the stops in the decomp
    print('match stops')
    # NOTE: this can drop trips entirely in some cases if there are no matched 
    # stops. There are other todos in the code to clean this up later.
    rawnav_route = wr.match_stops(rawnav_route, stop_index)
    
    # this also associates accel/decel with a stop, so we probably need to fix that too
    print('create stop segs')
    rawnav_route = wr.create_stop_segs(rawnav_route)

    print('trim ends')
    rawnav_route = (
        rawnav_route
        .groupby(['filename','index_run_start'])
        # reset odometer to be zero at second stop in order in the pattern
        # note that if you don't have a second stop, you just get ditched.
        # picking one a little ways into trip, since even first or second stop may be missing
        # TODO: may revisit this in light of the fact that we now mapmatch
        .apply(lambda x: wr.trim_ends(x))
        .reset_index(drop = True)
    )
        
    # Reset odometer
    print('reset odom')
    rawnav_route = (
        rawnav_route
        .groupby(['filename','index_run_start'])
        # reset odometer to be zero at second stop in order in the pattern
        # note that if you don't have a second stop, you just get ditched.
        # picking one a little ways into trip, since even first or second stop may be missing
        # TODO: may revisit this in light of the fact that we now mapmatch
        .apply(lambda x: wr.reset_odom(x, indicator_val = 6, indicator_var = 'stop_sequence_loc'))
        .reset_index(drop = True)
    )
    
    # TODO: maybe add some reasonableness checks here. 
    
    ##### Export Data
    print('export')
    # Write Index Table
    shutil.rmtree(
        os.path.join(
            path_decomp,
            "route={}".format(analysis_route)
        ),
        ignore_errors=True
    ) 

    pq.write_to_dataset(
        table = (
            pa.Table.from_pandas(
                rawnav_route,
                # this may be unnecessary--found a few issues at end where arrow would
                # try to convert objects to other types if i didn't explicitly cast
                schema = pa.Schema.from_pandas(rawnav_route)
            )
        ),
        root_path = path_decomp,
        partition_cols = ['route']
    )
    

