# -*- coding: utf-8 -*-
"""
Created on Wed July 22 02:45:31 2021

@author: WylieTimmerman

We'll use this to get a good set of trips that we also used for the wisconsin TSP analysis
"""

# % Environment Setup
import os, sys, pandas as pd, numpy as np
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
tsp_route_list = ['30N','30S','33','31']
analysis_routes = ['30N']
analysis_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
# EPSG code for WMATA-area work
wmata_crs = 2248

import wmatarawnav as wr

# % Reload Data
# %% Load Rawnav Data
# For now, we're just going to reload the processed wisconsin data, since it'll help us
# cut through a lot of bullshit data quality issues to start
rawnav_run_decomp = (
    pq.read_table(
        source=os.path.join(path_sp,"data","01-Interim","wisconsin_decomp_mt.parquet"),
        use_pandas_metadata = True
    )
    .to_pandas()
    # As a bit of proofing, we confirm this is int32
    .reset_index()
)

# %%% Add some details
# not strictly needed here, but just copying over

tsp_conditions = wr.tribble(
        ['route','direction_wmata_schedule', 'service_day','time_period','tsp_dir_time', 'tsp_offdir_time'],
        "30N",                       "EAST",     "Weekday",    "AM Peak",           True,            False,
        "30N",                       "WEST",     "Weekday",    "PM Peak",           True,            False,
        "30S",                       "EAST",     "Weekday",    "AM Peak",           True,            False,
        "30S",                       "WEST",     "Weekday",    "PM Peak",           True,            False,
         "33",                      "SOUTH",     "Weekday",    "AM Peak",           True,            False,
         "33",                      "NORTH",     "Weekday",    "PM Peak",           True,            False,
         "31",                      "SOUTH",     "Weekday",    "AM Peak",           True,            False,
         "31",                      "NORTH",     "Weekday",    "PM Peak",           True,            False,
        "30N",                       "WEST",     "Weekday",    "AM Peak",           False,            True,
        "30N",                       "EAST",     "Weekday",    "PM Peak",           False,            True,
        "30S",                       "WEST",     "Weekday",    "AM Peak",           False,            True,
        "30S",                       "EAST",     "Weekday",    "PM Peak",           False,            True,
         "33",                      "NORTH",     "Weekday",    "AM Peak",           False,            True,
         "33",                      "SOUTH",     "Weekday",    "PM Peak",           False,            True,
         "31",                      "NORTH",     "Weekday",    "AM Peak",           False,            True,
         "31",                      "SOUTH",     "Weekday",    "PM Peak",           False,            True
    )

rawnav_run_decomp_2 = (
    rawnav_run_decomp
    .assign(
        is_tsp_route = lambda x: x.route.isin(['30N','30S','33']),
        tsp_period = lambda x: 
            np.where(
                x.start_date_time.dt.tz_localize(None) < np.datetime64('2021-03-15'),
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
            )
    )
    .assign(
        service_day = lambda x:
            # we'll set President's day to saturday 
            np.where(
                x.start_date_time.dt.tz_localize(None) == np.datetime64('2021-02-15'),
                'Saturday',
                x.service_day
            ),
        trip_hour = lambda x: x.start_date_time.dt.hour,
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
    .merge(
        tsp_conditions,
        on = ['route','direction_wmata_schedule', 'service_day','time_period'],
        how = "left"
    )
    .assign(
        tsp_dir_time = lambda x : x.tsp_dir_time.fillna(False),
        overall_dir = lambda x : 
            np.where(
                x.direction_wmata_schedule.isin(['EAST','SOUTH']),
                "Southbound",
                "Northbound"
            )
    )
)

    
# %%% filter to wisconsin corridor stops
wisconsin_corr_stops = (
    pd.read_csv(os.path.join(path_processed_data,"wisconsin_corridor_stops.csv"))
    .query('stop_id != 32089')
)

wisconsin_corr_grp = wisconsin_corr_stops.groupby(['route','direction'])
rawnav_run_decomp_2_grp = rawnav_run_decomp_2.groupby(['route','direction_wmata_schedule','filename','index_run_start'])

rawnav_run_decomp_2_wisc = pd.DataFrame()

trips_missing_stops = pd.DataFrame()

for name, rawnav_group in rawnav_run_decomp_2_grp:
    # try:
    wisconsin_corr_rtdir_stops = (
        wisconsin_corr_grp
        .get_group((name[0], name[1]))
        .stop_id
        .astype(str)
    )
    
    rawnav_group_stops = (
        rawnav_group
        # filter to just the stops
        .query('(basic_decomp == "Non-Passenger") | (basic_decomp == "Passenger")')
        .trip_seg
    )

    miss_stops = list(set(wisconsin_corr_rtdir_stops) - set(rawnav_group_stops))

    if (len(miss_stops) == 0):
        # if you have all the corridor stops, then we'll filter to those stops and keep
        # that decomposition. Later we may want to separate this part out
        # this regex is a little bespoke, but so i think we'll need to use a different approach for parsing stops and stop segs in the future

        stop_regex = r"(?:^|_)[^\d]*(?:" + "|".join(wisconsin_corr_rtdir_stops) + ")"

        rawnav_group_out = (
            rawnav_group
            .loc[rawnav_group.trip_seg.str.contains(stop_regex, na = False)]
        )

        rawnav_run_decomp_2_wisc = pd.concat([rawnav_run_decomp_2_wisc, rawnav_group_out])
    else:
        trip_miss_deets = pd.DataFrame({"filename" : name[2], "index_run_start" : name[3], 'route' : name[0],'direction_wmata_schedule' : name[1], "miss_stop" : miss_stops})
        trips_missing_stops = pd.concat([trips_missing_stops,trip_miss_deets])

rawnav_run_decomp_2_fil = (
    rawnav_run_decomp_2_wisc
    #stop id for friendship heights bay used by these buses; this will remove segments leading to and from as well
    .loc[~rawnav_run_decomp_2_wisc.trip_seg.str.contains('32089', na = False)] 
)

del rawnav_run_decomp

# %%% Filter out crazy times
trip_times = (
    rawnav_run_decomp_2_fil
    .groupby(['filename','index_run_start'])
    .agg(
        tot_secs = ('secs_tot','sum')
    )
)

trip_times_dist = (
    trip_times
    .agg(
        tot_secs_min = ('tot_secs','min'),
        tot_secs_p01 = ('tot_secs', lambda x: x.quantile(.01)),
        tot_secs_p05 = ('tot_secs', lambda x: x.quantile(.05)),
        tot_secs_p50 = ('tot_secs', lambda x: x.quantile(.50)),
        tot_secs_p95 = ('tot_secs', lambda x: x.quantile(.95)),
        tot_secs_p99 = ('tot_secs', lambda x: x.quantile(.99)),
        tot_secs_max = ('tot_secs', 'max'),
    )
)

trip_times_low = (
    trip_times_dist
    .loc['tot_secs_p01','tot_secs']
)

trip_times_high = (
    trip_times_dist
    .loc['tot_secs_p99','tot_secs']
)

trip_times_fil = (
    trip_times
    .loc[(trip_times.tot_secs >= trip_times_low) & (trip_times.tot_secs <= trip_times_high)]
    .reset_index()
)

rawnav_run_decomp_2_fil = (
    rawnav_run_decomp_2_fil
    .merge(
        trip_times_fil,
        how = 'right',
        on = ['filename','index_run_start']
    )
    .reindex(rawnav_run_decomp_2_fil.columns, axis = "columns")
)

del rawnav_run_decomp_2
del rawanv_run_decomp_2_grp

# %% save out the trips that we will keep
rawnav_used_trips = (
    rawnav_run_decomp_2_fil
    .filter(['filename','index_run_start'], axis = "columns")
    .drop_duplicates()
)


pq.write_to_dataset(
    table = pa.Table.from_pandas(rawnav_used_trips),
    root_path = os.path.join(path_sp,"data","01-Interim","wisconsin_decomp_mt_used_trips.parquet")
)
