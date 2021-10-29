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


# %% Aggregate to route and stop-segment level first

decomp_agg_route_seg = []
speed_agg_trip_seg = []

# Loop over routes
for analysis_route in analysis_routes:
    print(analysis_route)
    
    # Loop over days or else hitting memory limits
    for day in ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']:
        print(day)
        
        decomp_agg_route_seg_fil = (
            decomp_all_hi[['route','start_date_time','index_run_start','filename','wday','trip_seg','basic_decomp','secs_marg','odom_ft_marg','fps_next','fps_next_sm']]
            .query('route == @analysis_route & wday == @day')
            # Add a column for year
            .assign(year=lambda x: pd.DatetimeIndex(x.start_date_time).year,
                    trip_instance=lambda x: x.index_run_start.to_string()+x.filename)
        )
        
        # Count total trips per segment
        decomp_agg_route_seg_trips = (
            decomp_agg_route_seg_fil
            .groupby(['route','year','wday','trip_seg'])
            # Total trip instances for each segment
            .agg(total_trips=('trip_instance', lambda x: x.nunique()))
            .reset_index()
        )
        
        # Aggregate total seconds and total trips
        decomp_agg_route_seg_temp = (
            decomp_agg_route_seg_fil
            .groupby(['route','year','wday','trip_seg','basic_decomp'])
            # Total seconds for each segment
            .agg(total_secs=('secs_marg','sum'))
            .reset_index()
            # Join trip counts
            .merge(decomp_agg_route_seg_trips, how='left', on=['route','year','wday','trip_seg'])
            .assign(avg_secs_per_trip=lambda x: x.total_secs/x.total_trips)
        )
        
        # Just do the average speed for trip instances on each segment
        speed_agg_trip_seg_temp = (
            decomp_agg_route_seg_fil
            # First find the average speed for each trip instance
            .groupby(['route','year','wday','trip_seg','trip_instance'])
            .agg(total_secs=('secs_marg','sum'),
                 total_feet=('odom_ft_marg','sum'),
                 speed_max=('fps_next_sm','max'),
                 speed_min=('fps_next','min')) # use unsmoothed value, since it will include zero
            .reset_index()
            .assign(speed_avg=lambda x: x.total_feet/x.total_secs)
        )
            
        decomp_agg_route_seg.append(decomp_agg_route_seg_temp)
        speed_agg_trip_seg.append(speed_agg_trip_seg_temp)
        
        del decomp_agg_route_seg_fil, decomp_agg_route_seg_trips, decomp_agg_route_seg_temp, speed_agg_trip_seg_temp
    
# Collapse results into data frames
decomp_agg_route_seg = pd.concat(decomp_agg_route_seg, ignore_index=True)
speed_agg_trip_seg = pd.concat(speed_agg_trip_seg, ignore_index=True)


# %%
# Then aggregate and create route-level stats on the speed
speed_agg_route_seg = (
    speed_agg_trip_seg
    .groupby(['route','year','wday','trip_seg'])
    .agg(total_secs=('total_secs','sum'),
         total_feet=('total_feet','sum'),
         # Actual speed min/max
         speed_max=('speed_max','max'),
         speed_min=('speed_min','min'),
         # Stats on the average speed
         speed_p00=('speed_avg','min'),
         speed_p05=('speed_avg', lambda x: np.percentile(x, q=5)),
         speed_p50=('speed_avg', lambda x: np.percentile(x, q=50)),
         speed_p95=('speed_avg', lambda x: np.percentile(x, q=95)),
         speed_p100=('speed_avg','max'))
    .reset_index()
    .assign(speed_avg=lambda x: x.total_feet/x.total_secs)
)


# %% Aggregate across routes for each segment

decomp_agg_seg_trips = (
    decomp_agg_route_seg
    .groupby(['route','year','wday','trip_seg'])
    # Get one value per year, wday, and trip_seg for each route
    .agg(total_trips=('total_trips','max'))
    # Aggregate across routes
    .groupby(['year','wday','trip_seg'])
    .agg(total_trips=('total_trips','sum'))
    .reset_index()
)

decomp_agg_seg = (
    decomp_agg_route_seg
    .groupby(['year','wday','trip_seg','basic_decomp'])
    # Total seconds for each segment
    .agg(total_secs=('total_secs','sum'))
    .reset_index()
    # Join trip counts
    .merge(decomp_agg_seg_trips, how='left', on=['year','wday','trip_seg'])
    .assign(avg_secs_per_trip=lambda x: x.total_secs/x.total_trips)
)

speed_agg_seg = (
    speed_agg_trip_seg
    .groupby(['year','wday','trip_seg'])
    .agg(total_secs=('total_secs','sum'),
         total_feet=('total_feet','sum'),
         # Actual speed min/max
         speed_max=('speed_max','max'),
         speed_min=('speed_min','min'),
         # Stats on the average speed
         speed_p00=('speed_avg','min'),
         speed_p05=('speed_avg', lambda x: np.percentile(x, q=5)),
         speed_p50=('speed_avg', lambda x: np.percentile(x, q=50)),
         speed_p95=('speed_avg', lambda x: np.percentile(x, q=95)),
         speed_p100=('speed_avg','max'))
    .reset_index()
    .assign(speed_avg=lambda x: x.total_feet/x.total_secs)
)


# %% Aggregate for entire H/I bus lane segment

h_street_segs = ['70','70_38247','70_42108','42108','42108_38247','38247','38247_644','644']
i_street_segs = ['36932','36932_30838','36932_39086','36932_34150','34150','34150_30838',
                 '34150_39086','30838','30838_26215','30838_39086','39086','39086_10',
                 '39086_26215','30838_10','10','10_26215','26215']

decomp_agg_h = (
    decomp_agg_seg
    .query('trip_seg in @h_street_segs')
    .groupby(['year','wday','basic_decomp'])
    # Total seconds for each segment
    .agg(total_secs=('total_secs','sum'),
         avg_secs_per_trip=('avg_secs_per_trip','sum'))
    .reset_index()
)
    
decomp_agg_i = (
    decomp_agg_seg
    .query('trip_seg in @i_street_segs')
    .groupby(['year','wday','basic_decomp'])
    # Total seconds for each segment
    .agg(total_secs=('total_secs','sum'),
         avg_secs_per_trip=('avg_secs_per_trip','sum'))
    .reset_index()
)

speed_agg_h = (
    speed_agg_trip_seg
    .query('trip_seg in @h_street_segs')
    # First aggregate for trip instances, across the entire segment
    .groupby(['route','year','wday','trip_instance'])
    .agg(total_secs=('total_secs','sum'),
         total_feet=('total_feet','sum'),
         # Actual speed min/max
         speed_max=('speed_max','max'),
         speed_min=('speed_min','min'))
    .reset_index()
    .assign(speed_avg=lambda x: x.total_feet/x.total_secs)
    # Then aggregate across all routes and trip instances
    .groupby(['year','wday'])
    .agg(total_secs=('total_secs','sum'),
         total_feet=('total_feet','sum'),
         # Actual speed min/max
         speed_max=('speed_max','max'),
         speed_min=('speed_min','min'),
         # Stats on the average speed
         speed_p00=('speed_avg','min'),
         speed_p05=('speed_avg', lambda x: np.percentile(x, q=5)),
         speed_p50=('speed_avg', lambda x: np.percentile(x, q=50)),
         speed_p95=('speed_avg', lambda x: np.percentile(x, q=95)),
         speed_p100=('speed_avg','max'))
    .reset_index()
    .assign(speed_avg=lambda x: x.total_feet/x.total_secs)
)

speed_agg_i = (
    speed_agg_trip_seg
    .query('trip_seg in @i_street_segs')
    # First aggregate for trip instances, across the entire segment
    .groupby(['route','year','wday','trip_instance'])
    .agg(total_secs=('total_secs','sum'),
         total_feet=('total_feet','sum'),
         # Actual speed min/max
         speed_max=('speed_max','max'),
         speed_min=('speed_min','min'))
    .reset_index()
    .assign(speed_avg=lambda x: x.total_feet/x.total_secs)
    # Then aggregate across all routes and trip instances
    .groupby(['year','wday'])
    .agg(total_secs=('total_secs','sum'),
         total_feet=('total_feet','sum'),
         # Actual speed min/max
         speed_max=('speed_max','max'),
         speed_min=('speed_min','min'),
         # Stats on the average speed
         speed_p00=('speed_avg','min'),
         speed_p05=('speed_avg', lambda x: np.percentile(x, q=5)),
         speed_p50=('speed_avg', lambda x: np.percentile(x, q=50)),
         speed_p95=('speed_avg', lambda x: np.percentile(x, q=95)),
         speed_p100=('speed_avg','max'))
    .reset_index()
    .assign(speed_avg=lambda x: x.total_feet/x.total_secs)
)
    
    
# %% Export files

# Route-stop seg level
decomp_agg_route_seg.to_csv(os.path.join(path_processed_data, "agg_decomp_route_seg.csv"))
speed_agg_route_seg.to_csv(os.path.join(path_processed_data, "agg_speed_route_seg.csv"))

# Stop seg level
decomp_agg_seg.to_csv(os.path.join(path_processed_data, "agg_decomp_seg.csv"))
speed_agg_seg.to_csv(os.path.join(path_processed_data, "agg_speed_seg.csv"))

# Entire seg level
decomp_agg_h.to_csv(os.path.join(path_processed_data, "agg_decomp_H_street.csv"))
decomp_agg_i.to_csv(os.path.join(path_processed_data, "agg_decomp_I_street.csv"))
speed_agg_h.to_csv(os.path.join(path_processed_data, "agg_speed_H_street.csv"))
speed_agg_i.to_csv(os.path.join(path_processed_data, "agg_speed_I_street.csv"))








