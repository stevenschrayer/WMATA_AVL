# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

# Libraries
import os, sys, glob, pandas as pd
from dotenv import dotenv_values
import pyarrow.parquet as pq
import numpy as np

# Paths
os.environ["GDAL_DATA"] = os.environ["CONDA_PREFIX"] + "\Library\share\gdal"
path_working = r"C:\Users\WylieTimmerman\Documents\projects_local\WMATA_AVL_datamart"
os.chdir(os.path.join(path_working))
sys.path.append(r"C:\Users\WylieTimmerman\Documents\projects_local\WMATA_AVL_datamart")
path_sp = r"C:\OD\Foursquare ITP\Projects - WMATA Datamart\Task 3 - Bus Priority"
path_source_data = os.path.join(path_sp,"data","00-Raw")
path_processed_data = os.path.join(path_sp, "data","02-Processed")
# Server credentials
config = dotenv_values(os.path.join(path_working, '.env'))

# Globals
wmata_crs = 2248

# Load wmatarawnav library
import wmatarawnav as wr

rawnav_run_decomp = (
    pq.read_table(
        source=os.path.join(path_sp,"data","01-Interim","wisconsin_decomp_mt.parquet"),
        use_pandas_metadata = True
    )
    .to_pandas()
    # As a bit of proofing, we confirm this is int32
    .reset_index()
)

# We parsed through the end of may, but want a month and a half on each end only.
rawnav_run_decomp = (
    rawnav_run_decomp
    .loc[(rawnav_run_decomp.start_date_time.dt.tz_localize(None) < np.datetime64('2021-04-30'))]

)

# do the time
rawnav_run_decomp_2 = (
    rawnav_run_decomp
    .assign(
        has_tsp = lambda x: x.route.isin(['30N','30S','33']),
        tsp_period = lambda x: 
            np.where(
                rawnav_run_decomp.start_date_time.dt.tz_localize(None) < np.datetime64('2021-03-15'),
                "pre_tsp",
                "post_tsp"
            ),
        dow = lambda x: x.start_date_time.dt.dayofweek,
        service_day = lambda x: 
            np.select(
                [
                    x.dow.isin([0,1,2,3,4]),
                    x.dow.eq(5),
                    x.dow.eq(6)
                ],
                [
                    'Weekday',
                    'Saturday',
                    'Sunday'
                ]
            ), 
        trip_hour = lambda x: x.start_date_time.dt.hour,
        # TODO: double check timestamps/timezone on conversion back from 
        # numpy
        time_period = lambda x: 
            pd.cut(
                x.trip_hour,
                bins = pd.IntervalIndex.from_tuples(
                    [
                        (0,4),
                        (4,6),
                        (6,9),
                        (9,15),
                        (15,19),
                        (19,23),
                        (23,999)
                    ]
                ),
                include_lowest=True, 
                retbins = False
            )
            .astype("category")
            .cat.rename_categories(
                ['Late Night1',
                'Early AM',
                'AM Peak',
                'Midday',
                'PM Peak',
                'Evening',
                'Late Night2']
            )
    )
)

# confirmed we're not mucking up timezones by looking into these items in the source data
test = (
    rawnav_run_decomp_2 
    .head(100000)
    .filter(
        ['filename','index_run_start','start_date_time','dow', 'trip_hour','time_period']
    )
    .drop_duplicates(
        ['filename','index_run_start','start_date_time','dow', 'trip_hour','time_period']
    )        
)   
    