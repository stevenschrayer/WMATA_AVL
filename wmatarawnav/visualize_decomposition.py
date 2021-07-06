# -*- coding: utf-8 -*-
"""
Created on Mon June 7 03:48 2021

@author: WylieTimmerman
"""
import pandas as pd
import numpy as np
from . import low_level_fns as ll
from plotly.offline import plot
import plotly.graph_objs as go
import plotly.express as px

def prepare_ts_chart_data(rawnav):
    
    # we need to create each colored line as a separate trace, and create extra points between the 
    # lines 
    rawnav_chart = (
        rawnav
        .assign(
            high_level_decomp_int = lambda x: 
                x.high_level_decomp.astype('category').cat.codes
            )
        )
    
    rawnav_chart['sequence'] = (
        	rawnav_chart
        	.groupby(['filename','index_run_start'])['high_level_decomp_int']
        	.transform(lambda x: x.diff().ne(0).cumsum())
        )
    
    rawnav_chart['dupe'] = False
    
    rawnav_chart_dupe = rawnav_chart.copy(deep = True)
    
    rawnav_chart_dupe['sequence_shift'] = (
        rawnav_chart_dupe
        .groupby(['filename','index_run_start'])['sequence']
        .shift(1, fill_value = 0)
    )
    
    rawnav_chart_dupe['high_level_decomp_shift'] = (
        rawnav_chart_dupe
        .groupby(['filename','index_run_start'])['high_level_decomp']
        .shift(1, fill_value = "nada")
    )
    
    rawnav_chart_dupe = (
        rawnav_chart_dupe
        .groupby(['filename','index_run_start','sequence_shift'])
        .tail(1)
        )
    
    rawnav_chart_dupe = (
        rawnav_chart_dupe
        .groupby(['filename','index_run_start'])
        .apply(lambda group: group.iloc[2:])     
        .apply(lambda group: group.iloc[:-1])     
    )
    
    rawnav_chart_dupe = (
        rawnav_chart_dupe
        .drop(columns = ['high_level_decomp','sequence'])
        .rename(columns = {'high_level_decomp_shift' : 'high_level_decomp',
                 'sequence_shift' : 'sequence'},
                errors="raise")
        .assign(
            dupe = True    
        )
    )
    # should be back at 37 cols
    rawnav_chart_all = (
        rawnav_chart
        .append(rawnav_chart_dupe)
        .sort_values(['filename','index_run_start','sequence','index_loc'])
        .iloc[:-3]
    )
    
    # if you have multiple trip instances, you now need to recalculate a unique 
    # sequence across all cases 
    rawnav_chart_all['sequence'] = (
       	rawnav_chart_all['sequence']
       	.transform(lambda x: x.diff().ne(0).cumsum())
    )
    
    # I'm a hack!
    rawnav_chart_all = (
        rawnav_chart_all 
        .query('high_level_decomp != "you shouldnt see this"')
        )
    
    return(rawnav_chart_all)

def plot_ts_data(ts_data):
    
    fig = px.line(
        ts_data,
        x = 'min_past_st',
        y = 'odom_mi',
        color = 'high_level_decomp',
        custom_data = ['route','pattern','start_date_time','high_level_decomp'],
        line_group = 'sequence', # need to make unique id, but for now just showing one,
        labels={ # replaces default labels by column name
                "high_level_decomp": "High-Level Decomposition",  
                "min_past_st": "Trip Time Elapsed (Minutes)", 
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
        
    fig.update_traces(
        hovertemplate="<br>".join([
            "Time Past Start (Minutes): %{x}",
            "Odometer (Miles): %{y}",
            "Route: %{customdata[0]}",
            "Pattern: %{customdata[1]}",
            "Start Date-Time: %{customdata[2]}",
            "Time Type: %{customdata[3]}"
        ])
    )

    fig.update_traces(
        line = dict(width = 3)
    )
    
    return(fig)