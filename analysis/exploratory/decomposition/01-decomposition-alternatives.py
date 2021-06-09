# -*- coding: utf-8 -*-
"""
Created on Wed Jun  9 02:45:31 2021

@author: WylieTimmerman
"""

import os, sys, glob, pandas as pd, geopandas as gpd
import pyarrow as pa, pyarrow.parquet as pq
from itertools import product
import numpy as np
from plotly.offline import plot
import plotly.graph_objs as go
import plotly.express as px

from IPython import get_ipython  

ipython = get_ipython()
ipython.magic("reset -f")
ipython = get_ipython()
ipython.magic("load_ext autoreload")
ipython.magic("autoreload 2")

path_repo = os.path.join('C:/Users/WylieTimmerman/Documents/projects_local/WMATA_AVL_datamart')
sys.path.append(path_repo)
path_data = 'C:/Users/WylieTimmerman/Documents/projects_local/wmata_avl_local/data/02-processed'
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")

wmata_crs = 2248

import wmatarawnav as wr

# %% READ IN
analysis_routes =  ['70','79']
analysis_days = ['Tuesday']
upstream_ft = 150
downstream_ft = 150
speed_thresh_fps = 7.333 #roughly 5mph

rawnav_raw = (
    wr.read_cleaned_rawnav(
        analysis_routes_= analysis_routes,
        analysis_days_ = analysis_days,
        path = os.path.join(
            path_data,
            "rawnav_data_2019.parquet"
        )
    )
)

# %% FILTER SETTINGS
combo = (
    pd.DataFrame(
        list(
            product(
                analysis_routes,
                analysis_days,
                ['georgia_columbia_stub']
            )
        ),
        columns = ['route','wday','seg_name_id']
    )
)


combo_zip = zip(combo.route, combo.wday, combo.seg_name_id)


filter_parquet = [[('route','=',route),('wday', '=', day),('seg_name_id', '=', seg_name_id)] for route, day, seg_name_id in combo_zip]

segment_summary = (
        pq.read_table(
            source = os.path.join(path_data,"segment_summary_2019.parquet"),
            filters = filter_parquet,
            use_pandas_metadata = True
        )
        .to_pandas()
    )

segment_summary_fil = (
        segment_summary
        .query('~(flag_too_far_any\
                  | flag_wrong_order_any\
                  | flag_too_long_odom\
                  | flag_secs_total_mismatch\
                  | flag_odom_total_mismatch)'
        )
)

# %% Filter data 
# This takes longer than i'd like
rawnav_raw_fil = (
    rawnav_raw[rawnav_raw[['filename','index_run_start']].agg(tuple,1).isin(segment_summary_fil[['filename','index_run_start']].agg(tuple,1))]
)

rawnav_fil = wr.calc_rolling_vals(rawnav_raw_fil)

use_runs = (
    wr.tribble(
        ['route', 'filename','index_run_start'],
        # Route 70 trips 
        "70", "rawnav05443191002.txt",	1025,
        "70", "rawnav05462191002.txt",	6993,
        "70", "rawnav05431191002.txt",	4272,
        "70", "rawnav05436191002.txt",	1263,
        "70", "rawnav05446191002.txt",	7970,
    
        # Route 79 trip instances,
        "79", "rawnav06468191002.txt",	1009,
        "79", "rawnav06438191002.txt",	3331,
        "79", "rawnav02807191002.txt",	1477,
        "79", "rawnav02811191002.txt",	2152,
        "79", "rawnav06463191002.txt",	271
    )
)

# %% DECOMPOSE TRIPS
rawnav_decomp = wr.decompose_basic_mt(rawnav_fil)

# %% MAKE PRETTY CHARTS
# %%% Line Chart for One Case
rawnav_trip_instances = (
    rawnav_decomp
    .drop_duplicates(subset = ['filename','index_run_start'])
    )

# i believe this is a 70 trip
file_test = "rawnav06468191002.txt"
index_test = 1009

rawnav_chart_in = (
    rawnav_decomp
    .query('(filename == "rawnav06468191002.txt" & index_run_start == 1009)')
)

# rawnav_chart_in = (
#     wr.semi_join(rawnav_decomp,use_runs, on = ['route', 'filename','index_run_start'])
# )

rawnav_chart_out = wr.prepare_ts_chart_data(rawnav_chart_in)

rawnav_chart_out = (
    rawnav_chart_out 
    .assign(
        odom_mi = lambda x: x.odom_ft / 5280,
        min_past_st = lambda x: x.sec_past_st / 60)
    )

fig2 = (
    px.line(
        rawnav_chart_out,
        x = 'min_past_st',
        y = 'odom_mi',
        color = 'high_level_decomp',
        custom_data = ['route','pattern','start_date_time','high_level_decomp'],
        line_group = 'sequence', # need to make unique id, but for now just showing one,
        labels={ # replaces default labels by column name
                "high_level_decomp": "High-Level Decomposition",  
                "sec_past_st": "Trip Time Elapsed (Minutes)", 
                "odom_mi": "Trip Odometer Reading (Miles)"
            },
            category_orders={
                "high_level_deocmp": 
                    ["<5 mph", 
                     ">= 5mph", 
                     "Non-Passenger",
                     "Passenger"], 
            },
            color_discrete_map={ 
                "<5 mph": "#A34F3F",  # bluish
                ">= 5mph": "#20918d", #green
                "Non-Passenger" : "#962c91", #purple
                "Passenger" :  '#ec7c54' #orange
            },
            template="simple_white"
    )
)
    
fig2.update_traces(
    hovertemplate="<br>".join([
        "Time Past Start (Minutes): %{x}",
        "Odometer (Miles): %{y}",
        "Route: %{customdata[0]}",
        "Pattern: %{customdata[1]}",
        "Start Date-Time: %{customdata[2]}",
        "Time Type: %{customdata[3]}"
    ])
)
    
plot(fig2)

# %%% Compare 70 and 79
rawnav_decomp_70_79 = (
    rawnav_decomp
    .query('high_level_decomp != "you shouldnt see this"')
    .groupby(['route','pattern','filename','index_run_start','high_level_decomp'])
    .agg(
        {'sec_past_st' : ['sum'],
         'odom_ft' : ['sum']   
         }
    )
    .pipe(wr.reset_col_names)
    .groupby(['route','pattern','high_level_decomp'])
    .agg(
        {'sec_past_st_sum' : np.mean,
         'odom_ft_sum' : np.mean  
         }
    )
    .assign(
         secs_pct = lambda x: 
             x.sec_past_st_sum / x.groupby(['route','pattern']).transform('sum')['sec_past_st_sum']
    )
    .reset_index()
)

fig3 = (
    px.bar(
     rawnav_decomp_70_79,
     x = 'route',
     y = 'secs_pct',
     color = 'high_level_decomp',
     labels={ # replaces default labels by column name
                "high_level_decomp": "High-Level Decomposition",  
                "secs_pct" : "Average % of Travel Time",
                "route" : "Route"
            },
            category_orders={
                "high_level_deocmp": 
                    ["<5 mph", 
                     ">= 5mph", 
                     "Non-Passenger",
                     "Passenger"], 
            },
            color_discrete_map={ 
                "<5 mph": "#A34F3F",  # bluish
                ">= 5mph": "#20918d", #green
                "Non-Passenger" : "#962c91", #purple
                "Passenger" :  '#ec7c54' #orange
            },
            template="simple_white"
    )
)
     
       
plot(fig3)
