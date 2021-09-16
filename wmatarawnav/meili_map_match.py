# -*- coding: utf-8 -*-
"""
Created on Thu Sep 16 06:27:26 2021

@author: WylieTimmerman
"""

import json
import requests
import pandas as pd
import numpy as np

def mapmatch(rawnav_ti):
    
    #### Pre-process data
    use_shape = (
        rawnav_ti
        .rename(
            {
                'long':'lon',
                'sec_past_st':'time'
            }, 
            axis = 'columns'
        )
        .filter(
            [
                'lat',
                'lon', # trying to drop to see if it sovles weird edge issue
                'time'
            ], 
            axis = "columns"
        )
        .reset_index(drop = True)
    )

    #### Params
    # We hard code these into the function since we don't think we'll 
    # want people to change this for rawnav
    use_costing = "bus"
    use_shape_match = "walk_or_snap"
    use_directions = dict(
        units = "miles",
        directions_type = "none"
    )
    
    # this seemed totally ignored, or else i got it wrong
    # still, we'lll leave in. I think in the version of meili that we're 
    # using htis doesn't work.
    # use_filters = dict(
    #     attributes = [
    #         "edge.names",
    #         "edge.begin_heading",
    #         "edge.end_heading",
    #         "edge.begin_shape_index",
    #         "edge.end_shape_index",
    # 		"edge.id",
    #         "edge.wayid",
    # 		"edge.weighted_grade",
    # 		"edge.speed",
    #         "osm_changeset",
    #         "matched.point",
    #         "matched.type",
    #         "matched.edge_index",
    #         "matched.begin_route_discontinuity",
    #         "matched.end_route_discontinuity",
    #         "matched.distance_along_edge",
    #         "matched.distance_from_trace_point"
    #     ],
    #     action = ["include"]
    # )
    
    #### Assemble request
    data = (
        json.dumps(
            dict(
                shape = use_shape.to_dict(orient = "records"),
                costing = use_costing,
                shape_match = use_shape_match,
                # see comment above, leaving in as placeholder for now.
                # filters = use_filters,
                directions_options = use_directions
            )
        )
    )
    url = "http://localhost:8002/trace_attributes"
    headers = {'Content-type': 'application/json'}
    
    #### Run request
    r = requests.get(url, data=data, headers=headers)
    
    if (r.status_code != 200):
        raise NameError("request failed")
    
    rjson = r.json()
    
    #### Extract
    # Matched points
    rawnav_matched = (
        pd.DataFrame(
            rjson['matched_points']
        )
        .filter(
            [
                'lat',
                'lon',
                'edge_index',
                'type',
                'distance_from_trace_point',
                'distance_along_edge'
            ],
            axis = "columns"
        )
        # TODO: add units to column names
        # TODO: revist name to 'proj'? that confuses me
        .rename(
            {
                # long not lon is intentional to match rawnav
                "lon" : "longmatch",
                "lat" : "latmatch"
            },
            axis = "columns"
        )
    )
    
    # Edges
    rawnav_edges = (
        pd.DataFrame(
            rjson['edges']   
        )
    )
    
    # need to flatten these names
    rawnav_edges['street_names'] = (
        rawnav_edges['names']  
        .replace(np.nan,"")
        .transform(
            lambda x: ",".join(map(str,x))
        )
    )
    
    rawnav_edges = (
        rawnav_edges
        # TODO: there should be a way to only request certain columns come back,
        # but i have been unsuccessful so far on that with use_filters
        .filter(
            [
                'id',
                'way_id',
                'begin_shape_index',
                'end_shape_index',
                'street_names',
                'lane_count',
                'speed',
                'speed_limit',
                'begin_heading',
                'end_heading',
                'max_upward_grade',
                'max_downward_grade',
                'weighted_grade',
                'length',
                'road_class'
            ],        
            axis = "columns")    
    )
    
    #### Format return
    rawnav_return = (
        rawnav_ti
        .merge(
            rawnav_matched,
            how = "left",
            left_index = True,
            right_index = True,
        )
        .merge(
            rawnav_edges,
            how = "left",
            left_on = ['edge_index'],
            right_index = True
        )    
    )
    
    return(rawnav_return)