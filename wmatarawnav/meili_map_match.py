# -*- coding: utf-8 -*-
"""
Created on Thu Sep 16 06:27:26 2021

@author: WylieTimmerman
"""

import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import numpy as np


def mapmatch(rawnav_ti):
    # refernece:
    # https://valhalla.readthedocs.io/en/latest/api/map-matching/api-reference/
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
    use_shape_match = "map_snap" # edge walk never seems to work with rawnav, so skip
    use_directions = dict(
        units = "miles",
        directions_type = "none"
    )
    # borrowed from
    # https://github.com/nick-caros/transit-map-matching/blob/main/valhalla_readme.md
    use_trace_options_search_radius = 100 
    use_trace_options_interpolation_distance = 100
    use_trace_options_turn_penalty_factor = 500
    # these are some of ours
    use_trace_options_gps_accuracy = 50
    
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
            {
                "shape" : use_shape.to_dict(orient = "records"),
                "costing" : use_costing,
                "shape_match" : use_shape_match,
                # see comment above, leaving in as placeholder for now.
                # "filters" : use_filters,
                "directions_options" : use_directions,
                "trace_options.search_radius" : use_trace_options_search_radius,
                "trace_options.interpolation_distance" : use_trace_options_interpolation_distance,
                "trace_options.turn_penalty_factor" : use_trace_options_turn_penalty_factor,
                "trace_options.gps_accuracy" : use_trace_options_gps_accuracy 
            }
        )
    )
    url = "http://localhost:8002/trace_attributes"
    headers = {'Content-type': 'application/json'}
    
    #### Run request
    # NOTE: valhalla/meili didn't seem to like multiple retries on failed matching,
    # so there's no code to do that. May be 
    r = requests.get(url, data=data, headers=headers)
    
    if (r.status_code == 400):
        # this is when matching fails. 
        rjson = r.json()
        # yes yes glue strings i know
        print(
            "Loki error " +
            str(rjson['error_code']) + 
            ": " + 
            rjson['error'] +
            " on " + 
            rawnav_ti['filename'].iloc[0] + 
            "-" +
            str(rawnav_ti['index_run_start'].iloc[0])
        )
        # reference, look under loki
        # https://github.com/valhalla/valhalla/blob/7e80a71c5034037746b02864e62899d1a4ce6292/docs/api/turn-by-turn/api-reference.md
        # this is when map matching fails
        # from some investigation, it can happen when there are only a few poitns 
        # in a trip instance and they're in a weird spot. usually error 154, 123, or 444
        rawnav_return = (
            rawnav_ti
            .reset_index(drop = True)
        )
        
        rawnav_return = (
            rawnav_return
            .reindex(
                columns = 
                    rawnav_return.columns.tolist() + 
                    [
                        "latmatch",                  
                        "longmatch",                
                        "edge_index",               
                        "type",                     
                        "distance_from_trace_point", 
                        "distance_along_edge",      
                        "id",                  
                        "way_id",                   
                        "begin_shape_index",         
                        "end_shape_index",         
                        "street_names",              
                        "lane_count",               
                        "speed",                    
                        "speed_limit",  
                        "begin_heading",             
                        "end_heading",              
                        "max_upward_grade",          
                        "max_downward_grade",       
                        "weighted_grade",            
                        "length",                   
                        "road_class"                
                    ]
            )
        )
        
        return(rawnav_return)

    if (r.status_code != 200):
        breakpoint() # if you get this, need to handle any remaining errors more thoughtfully above
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
    if 'names' in rawnav_edges.columns:
        # need to flatten these names
        rawnav_edges['street_names'] = (
            rawnav_edges['names']  
            .replace(np.nan,"")
            .transform(
                lambda x: ",".join(map(str,x))
            )
        )
    else:
        # TODO: maybe this should be NA instead of zero-length string? don't think it matters yet
        # but should think about later
        rawnav_edges['street_names'] = ""
    
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
    
    # TODO: incorporate OSM changeset id
    
    # TODO: get nodes in somehow to identify intersections
    # TODO: do a bunch of trycatch if we get werid stuff back in map matching
    
    #### Format return
    rawnav_return = (
        rawnav_ti
        .reset_index(drop = True)
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




























def locate(df, latcol = "latmatch", longcol = "longmatch"):
    # TODO: test that df has cols lat and long
    
    # Set params
    use_verbose = True
    # TODO: could do multiple...
    use_locations = (
        df
        .filter([latcol,longcol], axis = "columns")
        .rename(
            columns = 
                {
                latcol : "lat",
                longcol : "lon"
                },
        )
        .to_dict(orient = "records")
    )
        
    use_costing = "bus"
    use_directions = dict(
        units = "miles",
        directions_type = "none"
    )
    
    # load params
    data = (
        json.dumps(
            {
                "locations" : use_locations,
                "verbose" : use_verbose,
                "costing" : use_costing,
                "directions_options" : use_directions,
                "id" : 12345
            }
        )
    )
    url = "http://localhost:8002/locate"
    headers = {'Content-type': 'application/json'}
    
    r = requests.get(url, data=data, headers=headers)
    
    if (r.status_code != 200):
        breakpoint() # if you get this, need to handle any remaining errors more thoughtfully above
        raise NameError("request failed")

    r_df = pd.DataFrame(r.json())
    # this is a little sloppy in that we don't keep track of what lat/long input 
    # generated the ids, but we get teh edge ids in the output so we can keep track of 
    # what's missing or not
    r_edge_melt = pd.json_normalize(r_df.edges).melt()
    r_edge_melt = r_edge_melt.loc[r_edge_melt.value.notna()]

    r_return = (
            r_edge_melt
            .assign(
                edge_shape = 
                    lambda x: 
                        list(
                            map(
                                lambda y: y.get('edge_info.shape'),
                                x.value
                            )
                        ),
                edge_id = 
                    lambda x: 
                        list(
                            map(
                                lambda y: y.get('edge_id.value'),
                                x.value
                            )
                        ),
                edge_forward = 
                    lambda x: 
                        list(
                            map(
                                lambda y: y.get('edge.forward'),
                                x.value
                            )
                        ),
                edge_end_node = 
                    lambda x: 
                        list(
                            map(
                                lambda y: y.get('edge.end_node.value'),
                                x.value
                            )
                        ),
                edge_names = 
                    lambda x: 
                        list(
                            map(
                                lambda y: y.get('edge_info.names'),
                                x.value
                            )
                        ) 
            )
    )
        
    r_return['street_names'] = (
            r_return['edge_names']  
            .replace(np.nan,"")
            .transform(
                lambda x: ", ".join(map(str,x))
            )
        )    
    
    r_return = r_return.drop(['value','variable','edge_names'], axis = "columns")

    return(r_return)    

