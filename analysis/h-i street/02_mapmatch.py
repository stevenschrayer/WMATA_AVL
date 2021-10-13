# -*- coding: utf-8 -*-
"""
Created on Wed Sep 15 02:53:44 2021

@author: WylieTimmerman
"""
# NOTE: This is python 2.7 only, apparently
# may be better to dump files to CSV and then bring back to parquet later?
#

# % Environment Setup
import os
import sys
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import shutil
from datetime import datetime

# For postgresql
from dotenv import dotenv_values

if os.getlogin() == "WylieTimmerman":
    # Working Paths
    # the following needed to run in vscode jupyter interpreter
    os.environ["GDAL_DATA"] = os.environ["CONDA_PREFIX"] + "\Library\share\gdal"
    path_working = r"C:\Users\WylieTimmerman\Documents\projects_local\WMATA_AVL_datamart"
    os.chdir(os.path.join(path_working))
    sys.path.append(r"C:\Users\WylieTimmerman\Documents\projects_local\WMATA_AVL_datamart")
    path_sp = r"C:\OD\Foursquare ITP\Projects - WMATA Datamart\Task 3 - Bus Priority"
    path_source_data = os.path.join(path_sp, "data", "00-Raw")
    path_processed_data = os.path.join(path_sp, "data", "02-Processed")
    # Server credentials
    config = dotenv_values(os.path.join(path_working, '.env'))
    # other things for wylie's dev environment

import wmatarawnav as wr

# EPSG code for WMATA-area work
wmata_crs = 2248

analysis_routes = ['30N', '30S', '32', '33', '36', '37', '39', '42', '43', 'G8']

#### Iterate

path_matched_index = os.path.join(path_processed_data, "rawnav_matched_hi.parquet")

if not os.path.isdir(path_matched_index):
    os.mkdir(path_matched_index)


for analysis_route in analysis_routes:
    print(analysis_route)
    begin_time = datetime.now()
    print("Begin Time : {}".format(begin_time))

    rawnav_route = (
        pq.read_table(
            source=os.path.join(path_processed_data, "rawnav_data_hi.parquet"),
            filters=[('route', '=', analysis_route)],
            use_pandas_metadata=True
        )
        .to_pandas()
    )

    rawnav_route_matched = (
        rawnav_route
        .groupby(['filename', 'index_run_start'])
        .apply(lambda x: wr.mapmatch(x))
        .reset_index(drop=True)
    )

    shutil.rmtree(
        os.path.join(
            path_matched_index,
            "route={}".format(analysis_route)
        ),
        ignore_errors=True
    )

    pq.write_to_dataset(
        table=pa.Table.from_pandas(rawnav_route_matched),
        root_path=path_matched_index,
        partition_cols=['route']
    )

    execution_time = str(datetime.now() - begin_time).split('.')[0]
    print("map match runtime for route oct17 and oct19 : {}".format(execution_time))
