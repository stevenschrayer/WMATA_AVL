# -*- coding: utf-8 -*-
"""
Created on Fri Oct 22 13:54:40 2021

@author: JackMcDowell
"""

# %% Environment Setup
import os
import sys
import pandas as pd
import numpy as np
import shutil
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import dotenv_values
import folium

if os.getlogin() == "JackMcDowell":
    os.environ["GDAL_DATA"] = os.environ["CONDA_PREFIX"] + "\Library\share\gdal"
    path_working = r"C:\OD\OneDrive - Foursquare ITP\Documents\0002 R\WMATA Datamart\WMATA_AVL"
    os.chdir(os.path.join(path_working))
    sys.path.append(r"C:\OD\OneDrive - Foursquare ITP\Documents\0002 R\WMATA Datamart\WMATA_AVL")
    path_sp = r"C:\OD\Foursquare ITP\Projects - WMATA Datamart\Task 3 - Bus Priority"
    path_source_data = os.path.join(path_sp,"Data","00-Raw")
    path_processed_data = os.path.join(path_sp, "Data","02-Processed")
    # Server credentials
    config = dotenv_values(os.path.join(path_working, '.env'))

import wmatarawnav as wr

# Globals
hi_routes = ['37', '39', '42', '43', 'G8', '30N', '30S', '32', '33', '36']
analysis_routes = hi_routes
# EPSG code for WMATA-area work
wmata_crs = 2248


# %% Import decomposed rawnav data

decomp_route_all = []

for analysis_route in analysis_routes:
    print(analysis_route)
    #### Reload the data
    decomp_route_temp = (
        pq.read_table(
            source=os.path.join(path_processed_data, "decomp_nomm_hi.parquet"),
            filters=[('route', '=', analysis_route)],
            use_pandas_metadata=True
        )
        .to_pandas()
        # Correcting for weirdness when writing to/ returning from parquet
        .assign(
            route=lambda x: x.route.astype(str),
            pattern=lambda x: x.pattern.astype('int32', errors="ignore"),
            index_run_end=np.nan
        )
    )
    
    decomp_route_all.append(decomp_route_temp)
    
# Combine into one dataframe
decomp_route_all = pd.concat(decomp_route_all, ignore_index=True)

del decomp_route_temp

# %% View stops

stop_index = []

for analysis_route in analysis_routes:
    # Load the stop data
    stop_index_temp = (
        pq.read_table(
            source=os.path.join(path_processed_data, "stop_index_nomm_hi.parquet"),
            # note, sometimes 'analysis_route' must be wrapped in int() if you've only
            # processed routes without characters in the name.
            filters=[('route', '=', analysis_route)],
            use_pandas_metadata=True
        )
        .to_pandas()
        .assign(
            route=lambda x: x.route.astype(str),
            pattern=lambda x: x.pattern.astype('int32', errors="ignore")
        )
        .rename(columns={'odom_ft': 'odom_ft_stop'})
        .reset_index()
    )
    
    stop_index.append(stop_index_temp)

stop_index = pd.concat(stop_index, ignore_index=True)

stop_index_distinct = (
    stop_index[['stop_id','stop_lat','stop_lon']]
    .drop_duplicates()
    .reset_index()
)

# Leaflet map to inspect stops
stops_map = folium.Map(zoom_start = 11)
stops_list = stop_index_distinct[['stop_lat','stop_lon']].values.tolist()
for point in range(0, len(stops_list)):
    folium.Marker(stops_list[point], popup=stop_index_distinct['stop_id'][point]).add_to(stops_map)
stops_map.save('stops_map.html')

del stop_index_temp


# %% Filter to H/I segment

# Create list of stop-to-stop segments and stop areas that are on H/I segment
hi_stops = ['36932','34150','30838','39086','10','26215', # I Street
            '70','42108','38247','644'] # H Street

hi_trip_segs = []

for stop_id1 in range(0, len(hi_stops)):
    for stop_id2 in range(0, len(hi_stops)):
        hi_trip_segs.append(hi_stops[stop_id1]+'_'+hi_stops[stop_id2])
        
hi_trip_segs = hi_stops+hi_trip_segs

# Filter all routes
decomp_all_hi = (
    decomp_route_all
    .query('trip_seg == @hi_trip_segs')
)

# Test that we have all routes and all days of the week in filtered dataset
# Actually patterns 4301, G804, and 3205 don't appear on the H/I segment
def hi_filter_complete():
    all_routes = decomp_route_all[['route','wday']].drop_duplicates()
    filter_routes = decomp_all_hi[['route','wday']].drop_duplicates()
    # Antijoin in pandas is stupid
    antijoin_routes = (
        all_routes
        .merge(filter_routes,
               how='outer',
               on=['route','wday'],
               indicator=True)
        .query("_merge != 'both'")
    )
    assert antijoin_routes.size == 0
    
hi_filter_complete()

# Test that we have no gaps in segments
def hi_filter_gaps():
    # Assume each route pattern will require all the same segments
    all_segments = decomp_all_hi[['route_pattern','trip_seg']].drop_duplicates()
    antijoin_segments = (
        decomp_all_hi
        .merge(all_segments,
               how='outer',
               on=['route_pattern','trip_seg'],
               indicator=True)
        # Find anything that didn't join, whether a missing segment or a 
        # segment that shouldn't be there
        .query("_merge != 'both'")
    )
    assert antijoin_segments.size == 0

hi_filter_gaps()


# %% Aggregate for entire H/I bus lane segment

decomp_agg_hi = (
    decomp_all_hi[['start_date_time','index_run_start','filename','year','wday','trip_seg','basic_decomp','secs_marg']]
    # Add a column for year
    .assign(year=lambda x: pd.DatetimeIndex(x.start_date_time).year,
            trip_instance=lambda x: x.index_run_start.to_string()+x.filename)
    .groupby(['year','wday','trip_seg','basic_decomp'])
    # Total seconds and total trip instances for each segment
    .agg(total_secs=('secs_marg','sum'),
         total_trips=('trip_instance', lambda x: x.nunique()))
    .reset_index()
    .assign(avg_secs_per_trip=lambda x: x.total_secs/x.total_trips)
)


