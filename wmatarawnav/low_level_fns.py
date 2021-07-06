# -*- coding: utf-8 -*-
"""
Created on Mon Jul  6 22:38:06 2020

@author: WylieTimmerman
"""
import pandas as pd 
import fiona
import geopandas as gpd
from shapely.geometry import Point
from scipy.spatial import cKDTree
import numpy as np


def tribble(columns, *data):
    """
    Parameters
    ----------
    columns: list
        list of column names for dataframe
    data: 
        values to insert into columns 
    Returns
    -------
    df: pd.DataFrame
    """
    # I miss R
    return pd.DataFrame(
        data=list(zip(*[iter(data)]*len(columns))),
        columns=columns
    )

def reorder_first_cols(df,first_cols_list):
    """
    Parameters
    ----------
    df: pd.DataFrame
    first_cols_list: list
        list of columns in df to move to the beginning/left-side of the dataframe, ala
        dplyr::select() with tidyselect::everything()
    Returns
    -------
    df: pd.DataFrame
    """
    assert(isinstance(first_cols_list, list))
    
    new_cols_order = first_cols_list + [col for col in df.columns if col not in first_cols_list]
    
    df = df[new_cols_order]
    
    return df

def check_convert_list(possible_list):
    """
    Parameters
    ----------
    possible_list: 'list' object or 'str'
    Returns
    -------
    possible_list: if possible_list is a string value, it is converted to a list. This helps 
        address cases where iteration over a single value inadvertently leads the iteration to
        proceed over each character in the string.
    """
    if isinstance(possible_list,str):
        return ([possible_list])
    else:
        return (possible_list)
    
def drop_geometry(gdf):
    """
    Parameters
    ----------
    gdf: gpd.GeoDataFrame 
        Requires geometry in column 'geometry'
    Returns
    -------
    df: pd.DataFrame
       Dataframe containing all columns of the GeoDataFrame except for 'geometry', inspired
       by sf::st_drop_geometry and https://github.com/geopandas/geopandas/issues/544
    """
    df = pd.DataFrame(gdf[[col for col in gdf.columns if col != gdf._geometry_column_name]])
    
    return(df)
       
def explode_first_last(gdf):
    """
    Parameters
    ----------
    gdf: gpd.GeoDataFrame 
        with geometry in column 'geometry', currently only linestring supported
    Returns
    -------
    line_first_last: gpd.DataFrame, geodataframe with one row for the first and last vertex 
        of each geometry in the input gdf. The attributes of each original row are carried 
        forward to the output gdf.
    NOTE -- BAM - come back - I think this can be cleaner. See shapely line functions in bus route --> street code.
    """
    assert(all(gdf.geom_type.to_numpy() == "LineString")), print("Currently only LineString segment geometry supported")
    line_first_last_list = []
     
    # Not especially pythonic, but preserves dtypes nicely relative to itertuples and esp. iterrows
    for i in range(0,len(gdf)):

        justone = gdf.iloc[i,:]
        
        # There are issues if only one segment is passed
        if (isinstance(justone,pd.Series)):
            justone = justone.to_frame().transpose()
            justone = gpd.GeoDataFrame(justone, crs = gdf.crs, geometry = "geometry")
    
        first_point = Point(list(justone['geometry'].iloc[0].coords)[0])
        last_point = Point(list(justone['geometry'].iloc[0].coords)[-1])
        
        first_row = gpd.GeoDataFrame(
            drop_geometry(justone).assign(location = 'first'),
            geometry = [first_point],
            crs = justone.crs)
        
        last_row = gpd.GeoDataFrame(
            drop_geometry(justone).assign(location = 'last'),
            geometry = [last_point],
            crs = justone.crs)

        line_first_last_list.append(first_row)
        line_first_last_list.append(last_row)
    
    line_first_last = gpd.GeoDataFrame(pd.concat( line_first_last_list, ignore_index=True, axis = 0),
                                       crs = gdf.crs)
    
    return(line_first_last)

def ckdnearest(gdA, gdB):
    """
    # https://gis.stackexchange.com/questions/222315/geopandas-find-nearest-point-in-other-dataframe
    Parameters
    ----------
    gdA : gpd.GeoDataFrame
        typically wmata schedule data for the correct route and direction.
    gdB : gpd.GeoDataFrame
        rawnav data: only nearest points to gdA are kept in the output.
    Returns
    -------
    gdf : gpd.GeoDataFrame
        wmata schedule data for the correct route and direction with the closest rawnav point.
    """
    
    gdA.reset_index(inplace=True, drop=True);
    gdB.reset_index(inplace=True, drop=True)
    nA = np.array(list(zip(gdA.geometry.x, gdA.geometry.y)))
    nB = np.array(list(zip(gdB.geometry.x, gdB.geometry.y)))
    btree = cKDTree(nB)
    dist, idx = btree.query(nA, k=1)
    gdf = pd.concat(
        [gdA.reset_index(drop=True),
         gdB.loc[idx, ['filename', 'index_run_start', 'index_loc', 'odom_ft', 'sec_past_st', 'lat', 'long']].reset_index(
             drop=True),
         pd.Series(dist, name='dist_to_nearest_point')], axis=1)
    return gdf


def reset_col_names(df):
    """
    # https://gis.stackexchange.com/questions/222315/geopandas-find-nearest-point-in-other-dataframe
    Parameters
    ----------
    df : pd.DataFrame,
        dataframe after a groupby and aggregation with hierarchical column names
    Returns
    -------
    df : pd.DataFrame,
        same dataframe without hierarchy and collapsed column names
    Notes
    -----
    Pandas has a fun way of returning hierarchical column names that don't lend themselves well
    to continuing to do work on a dataframe. This addresses that.
    """
    
    df.columns = ["_".join(x) for x in df.columns.ravel()]
    df = df.reset_index()
    df.columns = df.columns.str.replace(pat = "_$",repl = "", regex = True)
    return(df)

def semi_join(left,right,on):
    """
    # https://gis.stackexchange.com/questions/222315/geopandas-find-nearest-point-in-other-dataframe
    Parameters
    ----------
    left : pd.DataFrame,
        dataframe to keep records from
    right: pd.DataFrame,
        dataframe to use to filter
    on: list,
        list of columns present in both dataframes to filter on
    Returns
    -------
    df : pd.DataFrame,
        all records of left that are found in right
    Notes
    -----
    Pandas has a fun way of returning hierarchical column names that don't lend themselves well
    to continuing to do work on a dataframe. This addresses that.
    """
    
    out = (
        left[left[on].agg(tuple,1).isin(right[on].agg(tuple,1))]
    )

    return(out)