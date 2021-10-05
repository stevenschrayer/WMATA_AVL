# -*- coding: utf-8 -*-
"""
Created on Mon Oct  4 23:41:06 2021

@author: WylieTimmerman

Aggregate to year and street network id for later visualization
"""

# % Environment Setup
import os, sys, pandas as pd, numpy as np
import shutil
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import dotenv_values
import geopandas as gpd

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
rawnav_agged = pd.DataFrame()

# we really need a database, this is so dumb
for analysis_route in analysis_routes:
    print(analysis_route)
    rawnav = (
        pq.read_table(
            source=os.path.join(path_processed_data,"decomp_match_hi.parquet"),
            use_pandas_metadata = True,
            filters = [('route','=',analysis_route)]
        )
        .to_pandas()
        .assign(
            year = lambda x: pd.DatetimeIndex(x.start_date_time).year    
        )
    )
        
    rawnav_nofirstdoor = (
        rawnav
        # remove first door open time for now, will leave in other stopped time around things
        # TODO: should also do something where we filter or otherwise flag which street ids
        # are valid for a particular route, since some might be valid for some but not others
        .loc[rawnav.stop_decomp.ne("doors_at_O_S")]
    )
    # stupid pandas can't handle filters upstream in pipes when you need to reference dataframe name
    # later in a pipe
    
    # stupid pandas doesn't do agg lambdas using multiple columns cleanly
    # TODO: we got this from SO, but we should confirm that this actually works as expected
    def wavg(y, df):
        return np.average(y, weights = df.loc[y.index,'secs_marg'])
    
    rawnav_agged_route_ti_speed = (
        rawnav_nofirstdoor
        .groupby(
            # even though we want to group by other things, we can get some memory errors 
            # with more groups. we'll rejoin this info later
            [
                'filename',
                'index_run_start', 
                'id'
            ], 
            sort = False
        ) 
        # cheating a little, but basically, since pings don't quite occur every second,
        # we will weight values a little more heavily when they have a 'gap' afterwards and those
        # values persist for a while.
        # the agg version of this with a custom function died in memory terms the first go around
        # 
        .agg(
            fps_next_sm = ('fps_next_sm', lambda x, df = rawnav_nofirstdoor: wavg(x, df)),
        )
    )
    
    # we're kind of running into the QJ related issue again, where some of this non-door open time
    # is nonetheless time spent serving the stop. But sometimes there is a little extra traffic delay
    # that makes serving that stop slower, so you need to keep track of some sort of minimum time to 
    # serve a stop. That would need to be maintained in a separate table and updated regularly
    rawnav_agged_route_ti_basic_decomp = (
        rawnav_nofirstdoor
        .groupby(
            [
                'filename',
                'index_run_start', 
                'id', 
                'basic_decomp_ext'
            ], 
            sort = False,
            as_index = False
        ) 
        .agg(
            secs_marg = ('secs_marg', 'sum'),
        )
        .pivot(
            index = [
                'filename', 
                'index_run_start', 
                'id'
            ],
            columns = 'basic_decomp_ext',
            values = 'secs_marg'
        )
        .add_suffix('_secs')
        .fillna(0)
        .assign(
            tot_secs = lambda x: x.sum(axis = 1),
            # this is kinda dicey, now have some 0s here. this is all activity not at all related to 
            # doors
            tot_nonstoprel_secs = lambda x: 
                x.accel_nodoors_secs + 
                x.decel_nodoors_secs + 
                x.other_delay_secs +
                x.steady_secs + 
                x.stopped_nodoors_secs
        )
    )
    
    rawnav_ti_info = (
        rawnav_nofirstdoor
        .drop_duplicates(
            subset =  [
               'route_pattern',
               'year',                
               'filename',
               'index_run_start'
            ]
        )
        .filter(
            [
               'route_pattern',
               'year',                
               'filename',
               'index_run_start'
            ],
            axis = 'columns'
        )
    )        
        
    rawnav_agged_route_ti = (
        rawnav_agged_route_ti_speed
        .merge(
            rawnav_agged_route_ti_basic_decomp,
            left_index = True,
            right_index = True,
            how = 'outer'
        )
        .reset_index()
        .merge(
            rawnav_ti_info,
            left_on = ['filename', 'index_run_start'],
            right_on = ['filename', 'index_run_start'],
            how = 'left'
        )
    )    
    
    rawnav_agged_route = (
        rawnav_agged_route_ti
        .groupby(['route_pattern', 'year', 'id'], sort = False)
        .agg(['mean'])
        .pipe(wr.reset_col_names)
    )
    
    rawnav_agged = pd.concat([rawnav_agged, rawnav_agged_route])
    

# export
# bring in the valid id segments for each route
# right now this was done in R and i am hacking it in, later we will want to do this in python
valid_shapes = (
    gpd.read_file(
        os.path.join(
            path_sp,
            "data",
            "01-Interim",
            "valid_pattern_ids_hi.geojson"
        )
    )
    .drop(['geometry'], axis = 'columns')
)
   
# re-add a few columns, join in flags for 'valid' segments 
rawnav_agged_out = (
    rawnav_agged
    .assign(
        route = lambda x: x.route_pattern.str.strip().str.extract(pat = "^([0-9A-Za-z]{2,3})[0-9]{2}$", expand = False),
        pattern = lambda x: x.route_pattern.str.strip().str.extract(pat = "^[0-9A-Za-z]{2,3}([0-9]{2})$", expand = False).astype(int)
    )
    .merge(
        valid_shapes,
        on = ['route', 'id'],
        how = 'left'
    )
    .assign(
        isvalid = lambda x: x.isvalid.fillna(False)    
    )
)

rawnav_agged_out.to_csv(os.path.join(path_processed_data,"decomp_agg_hi.csv"))

# in case tableau complains, let's also try bringing in the geometry for all streets early
valhalla_shapes_hi = (
    gpd.read_file(
        os.path.join(
            path_sp,
            "data",
            "01-Interim",
            "meili_shapes_hi.geojson"
        )
    )
)

valhalla_shapes_data = (
    valhalla_shapes_hi
    .merge(
        rawnav_agged_out,
        left_on = ['edge_id'],
        right_on = ['id'],
        how = 'left'
    )
)

valhalla_shapes_data = (
   valhalla_shapes_data
   .loc[valhalla_shapes_data.id.notna()]
)

(
    valhalla_shapes_data
    .to_file(
        os.path.join(path_sp,"Data","01-Interim","valhalla_shapes_hi_data.geojson"), 
        driver='GeoJSON'
    )
)
