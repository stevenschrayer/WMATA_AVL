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
import requests
import json

# For postgresql
# TODO: for now, skipping server, as amit says it's a bit slow
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

use_shape = (
    rawnav
    .query('(filename == "rawnav02205171017.txt") & (index_run_start == 3185)')
    .rename(
        {
            'long':'lon',
            'sec_past_st':'time'
        }, 
        axis = 'columns'
    )
    .filter(
        [
            'lat',
            'lon', # trying to drop to see if it sovles weird edge issue
            'time'
        ], 
        axis = "columns"
    )
    .reset_index(drop = True)
)

#### 
# Params
use_costing = "bus"
use_shape_match = "walk_or_snap"
use_directions = dict(
    units = "miles",
    directions_type = "none"
)
# this seemed totally ignored, or else i go 
use_filters = dict(
    attributes = [
        "edge.names",
        "edge.begin_heading",
        "edge.end_heading",
        "edge.begin_shape_index",
        "edge.end_shape_index",
		"edge.id",
        "edge.wayid",
		"edge.weighted_grade",
		"edge.speed",
        "osm_changeset",
        "matched.point",
        "matched.type",
        "matched.edge_index",
        "matched.begin_route_discontinuity",
        "matched.end_route_discontinuity",
        "matched.distance_along_edge",
        "matched.distance_from_trace_point"
    ],
    action = ["include"]
)

# Assemble request
data = (
    json.dumps(
        dict(
            shape = use_shape.to_dict(orient = "records"),
            costing = use_costing,
            shape_match = use_shape_match,
            filters = use_filters,
            directions_options = use_directions
        )
    )
)
url = "http://localhost:8002/trace_attributes"
headers = {'Content-type': 'application/json'}

#### Run request
# TODO : make and parse request
r = requests.get(url, data=data, headers=headers)

if (r.status_code != 200):
    raise NameError("request failed")

rjson = r.json()

#### Extract
rawnav_matched = (
    pd.DataFrame(
        rjson['matched_points']
    )
    .filter(
        [
            'lat',
            'lon',
            'edge_index',
            'type',
            'distance_from_trace_point',
            'distance_along_edge'
        ],
        axis = "columns"
    )
    # TODO: add units to column names
    # TODO: revist name to 'proj'? that confuses me
    .rename(
        {
            "lon" : "lonmatch",
            "lat" : "latmatch"
        },
        axis = "columns"
    )
)

rawnav_edges = (
    pd.DataFrame(
        rjson['edges']   
    )
)

rawnav_edges['street_names'] = (
    rawnav_edges['names']  
    .replace(np.nan,"")
    .transform(
        lambda x: ",".join(map(str,x))
    )
)

rawnav_edges = (
    rawnav_edges
    # TODO: there should be a way to only request certain columns come back,
    # but i have been unsuccessful so far on that with use_filters
    .filter(
        [
            'id',
            'way_id',
            'begin_shape_index',
            'end_shape_index',
            'street_names',
            'lane_count',
            'speed',
            'speed_limit',
            'begin_heading',
            'end_heading',
            'max_upward_grade',
            'max_downward_grade',
            'weighted_grade',
            'length',
            'road_class'
        ],        
        axis = "columns")    
)

rawnav_return = (
    use_shape
    .merge(
        rawnav_matched,
        how = "left",
        left_index = True,
        right_index = True,
    )
    .merge(
        rawnav_edges,
        how = "left",
        left_on = ['edge_index'],
        right_index = True
    )    
)


