# -*- coding: utf-8 -*-
"""
Created on Fri Oct  1 03:39:17 2021

@author: WylieTimmerman
"""

# % Environment Setup
import os, sys, pandas as pd, pyarrow.parquet as pq
from datetime import datetime
from pypolyline.util import encode_coordinates, decode_polyline
import geopandas as gpd
import shapely.geometry as sg
import shapely.ops as sops


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

# DO the locate
testdf = pd.DataFrame(
    {
         "latmatch" : [38.901333, 38.883781],
         "longmatch" : [-77.04216, -76.99366]
     }
)

testloc = wr.locate(testdf)

def run_decode(y):
    z = (
        decode_polyline(
            bytes(
                y,
                "ascii"
            ),
            6
        )
    )
        
    aa = sg.LineString(z)
    # the polyline function flips coords from default order for whatever reason,
    # so it's easier to just flip after we make geometry
    # https://gis.stackexchange.com/questions/354273/flipping-coordinates-with-shapely
    ab = sops.transform(lambda x, y : (y, x), aa)
    
    return(ab)
    
    

testloc['decoded_shape'] = (
    testloc['edge_shape']
    .apply(
        lambda x: run_decode(x)
    )    
)

testgdf = gpd.GeoDataFrame(testloc,geometry = "decoded_shape",crs = "EPSG:4326")

testgdf.to_file("test.geojson", driver='GeoJSON')


# do a full match and loc
rawnav = (
    pq.read_table(
        source=os.path.join(path_processed_data,"decomp_match_hi.parquet"),
        use_pandas_metadata = True,
        filters = [('route','=','36')]
    )
    .to_pandas()
)

id_dist = (
    rawnav
    .groupby(['id'], sort = False, as_index = False)
    .agg(
        latmatch = ('latmatch','first'),
        longmatch = ('longmatch','first')
    )
)

id_shape = wr.locate(id_dist, latcol = 'latmatch', longcol = 'longmatch')

id_shape_dist = (
    id_shape
    .drop_duplicates(['edge_id','edge_shape'])    
)

# not sure why there still seem to be dupes here
check_all = (
    id_dist 
    .merge(
        id_shape_dist,
        left_on = 'id',
        right_on = 'edge_id',
        how = 'outer'
    )
    .assign(
        edge_id = lambda x: x.edge_id.combine_first(x.id)    
    )
)

# TODO: there's at least one case that didn't match,
# 2049706056392

check_all.to_csv('encodedlines36.csv')
