# -*- coding: utf-8 -*-
"""
Created on Wed Aug  4 11:19:02 2021

@author: WylieTimmerman
"""

import os, sys, pandas as pd, numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import dotenv_values
import glob, shutil

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
    
import wmatarawnav as wr

run_inventory = True

if run_inventory:
    zipped_files_dir_parent = os.path.join(path_source_data, "052021")
    file_universe = glob.glob(os.path.join(zipped_files_dir_parent, "*.txt.zip"))
    rawnav_inventory = wr.find_rawnav_routes(file_universe, nmax=None, quiet=True)
    
    path_rawnav_inventory = os.path.join(path_processed_data,"rawnav_inventory_202105.parquet")
    shutil.rmtree(path_rawnav_inventory, ignore_errors=True) 
    os.mkdir(path_rawnav_inventory)
        
    # Note: partitioning required, using filename avoids resorting of values, filename column
    # will be sorted to end on reload however.
    rawnav_inventory.to_parquet(
        path = path_rawnav_inventory,
        partition_cols = ['filename'],
        index = False
    )

else:
    # Load data
    rawnav_inventory = (
        pd.read_parquet(path=os.path.join(path_processed_data,"rawnav_inventory_202105.parquet"))
        # .assign(filename = lambda x: x.filename.astype(str)) #returned as categorical
    )

# What is the range of dates in this? it looks like this is what jack processed
# looks like january to part of may; kind of odd, thought we had just february through all
# of march
# wow, some weird dates in there
rawnav_range = (
    rawnav_inventory
    .agg(
        earliest = ('tag_datetime','min'),
        latest= ('tag_datetime','max')
    )    
)

# what are these early ones?
# the data labeled may has a lot of non-may data, wow.
rawnav_inv_bydate = (
    rawnav_inventory
    .sort_values(['tag_datetime'])
    .head(100)
)

rawnav_inv_bydate = (
    rawnav_inventory
    .sort_values(['tag_datetime'])
    .tail(10000)
)


# let's filter to our key months
rawnav_inventory_fil = (
    rawnav_inventory
    .loc[
        (rawnav_inventory.tag_date.dt.tz_localize(None) >= np.datetime64('2021-02-01 00:00:00')) &
        (rawnav_inventory.tag_date.dt.tz_localize(None) < np.datetime64('2021-05-01 00:00:00'))
        ]
)
# this seems like too little data

# what are all of the unique dates in the data and hteir count of obs?
rawnav_date_cnt = (
    rawnav_inventory
    .groupby(['tag_date'])
    .agg(
        tag_cnt = ('taglist','count')    
    )    
    
)
# okay, apparently this is like part of may, let's just pick first two weeks
rawnav_inventory_fil = (
    rawnav_inventory
    .loc[
        (rawnav_inventory.tag_date.dt.tz_localize(None) >= np.datetime64('2021-05-01 00:00:00')) &
        (rawnav_inventory.tag_date.dt.tz_localize(None) < np.datetime64('2021-05-15 00:00:00'))
        ]
)

# how many unique text files in this range
cnt_unique_files= (
    rawnav_inventory_fil
    .assign(filename = lambda x: x.filename.astype(str)) 
    .drop_duplicates('filename')
    .shape[0]
    )
# 13524 files covering that ; seems a little small

cnt_tags = (
    rawnav_inventory_fil
    .shape[0]    
    )
# 198590 tags

cnt_tags_onroute = (
    rawnav_inventory_fil
    .loc[rawnav_inventory_fil.route.notnull()]
    .shape[0]    
    )
# 149681

est_records_tot = (
    rawnav_inventory_fil
    .assign(
        line_num = lambda x: x.line_num.astype('float'),    
        filename = lambda x: x.filename.astype(str)
    )
    .groupby(['filename'])
    .agg(
        high_index = ('line_num','max')    
    )
    .reset_index()
    .assign(
        est_total = lambda x: x.high_index + 2000    
    )
)

# this includes the APC tags
est_records_tot.est_total.sum()
# 274,261,437

# 
unique_routes_check = (
    rawnav_inventory_fil
    .groupby(['route'])
    .agg(
        trips = ('taglist','count')
        )
    .reset_index()
    .sort_values(['route'])
)
# what are the 30N trips we get
routes_check_30N = (
    rawnav_inventory_fil
    .loc[rawnav_inventory_fil.route_pattern.str.contains('30N')]
    .sort_values(['tag_datetime'])
)

# when we ran a check against GTFS in separate script, 
# we should get 40 30N trips a day, 200 a week, and 400 per 2 weeks
# we get 376 in two weeks, which seems about right
# get low of 34 trips to high of 41 trips
routes_check_30N_day = (
    routes_check_30N
    .groupby(['tag_date'])
    .agg(
        count_trips = ('taglist','count')
     )
)


# what is the range of seconds values observed, and how many observations for that do we 
# typically get?
rawnav_raw_temp = (
        wr.read_cleaned_rawnav(
            analysis_routes_= ['30N'],
            analysis_days_ = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'],
            path = os.path.join(
                path_processed_data,
                ("rawnav_data_" +  '202102' + ".parquet")
            )
        )
    )

check_total_secs = (
    rawnav_raw_temp
    .groupby(['filename','index_run_start'])
    .agg(
        secs_min = ('sec_past_st','min'),
        secs_max = ('sec_past_st','max'),
        nrow = ('index_loc','count')        
    )
    .assign(
        secs_tot = lambda x: x.secs_max - x.secs_min
    )
    .assign(
        secs_to_interp = lambda x: x.secs_tot - x.nrow
    )
    .reset_index()
)

# so seems like we'd need to expand  about 45% of the time on route if we did expansion
check_total_secs_agg = (
    check_total_secs
    .assign(grp = 1)
    .groupby(['grp'])
    .agg(
        secs_tot = ('secs_tot','sum'),
        nrow = ('nrow','sum'),
        secs_to_interp = ('secs_to_interp','sum')
    )
    .assign(
        secs_to_interp_pct = lambda x: x.secs_to_interp / x.secs_tot
    )
)

# how many 'apc' tags?
cnt_apc_tags = (
   rawnav_raw_temp
    .groupby(['filename','index_run_start'])
    .agg(
        tot_apc = ('row_before_apc','sum'),
        tot_row = ('index_loc', lambda x: x.max() - x.min()) # this is an estimate with apc,cal tags
    ) 
)

cnt_apc_tags.tot_apc.sum()
# 15033 for 30N for february 

# count trips in that sample
cnt_apc_tags.shape[0]
# 757

# count of rows (including apc and cal and pings and tags is)
cnt_apc_tags.tot_row.sum()
# 2070250

# so rate of APC tags out of all tags is 
rate_apc_rawn = (cnt_apc_tags.tot_apc.sum() / cnt_apc_tags.tot_row.sum())
rate_apc_rawn 

# so if you extrapolate to may data, it may be 
est_records_tot.est_total.sum() * rate_apc_rawn


# what share is 30N pings out of total pings so we can extrapolate from this
# actually, just multiply the rate. leaving htis here if i want it later
# share_30N = (
#     rawnav_inventory_fil
#     .loc[rawnav_inventory_fil.route.notnull()]
#     .assign(
#         nrows = lambda x: x.line_num_next.astype('float') - x.line_num.astype('float'),
#         is_30N = lambda x: np.where(
#             x.route_pattern.str.contains('30N'),
#             'is30N',
#             'all_other'
#         )
#     )
#     .groupby(['is_30N'])
#     .agg(
#         tot_rows = ('nrows','sum')    
#     )
#     .reset_index()
#     .assign(
#         grp = 1
#     )
#     .pivot(
#         index = 'grp',
#         columns = 'is_30N',
#         values = 'tot_rows'
#     )
#     .assign(
#         pct_30N = lambda x: x.is30N / (x.is30N + x.all_other)
#     )
# )
