# -*- coding: utf-8 -*-
"""
Created on Fri Oct  1 03:39:17 2021

@author: WylieTimmerman
"""

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

# DO the locate
testloc = wr.locate(lat = 38.901333, long = -77.04216)







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

id_shape = 
