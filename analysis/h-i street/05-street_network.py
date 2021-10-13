# -*- coding: utf-8 -*-
"""
Created on Mon Oct  4 10:28:17 2021

@author: WylieTimmerman

We'll use this to extract and create shapes we can join to.
"""

# % Environment Setup
import os
import sys
import pandas as pd
import numpy as np
import shutil
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import dotenv_values
import geopandas as gpd

if os.getlogin() == "WylieTimmerman":
    # Working Paths
    # the following needed to run in vscode jupyter interpreter
    os.environ["GDAL_DATA"] = os.environ["CONDA_PREFIX"] + "\Library\share\gdal"
    path_working = r"C:\Users\WylieTimmerman\Documents\projects_local\WMATA_AVL_datamart"
    os.chdir(os.path.join(path_working))
    sys.path.append(r"C:\Users\WylieTimmerman\Documents\projects_local\WMATA_AVL_datamart")
    path_sp = r"C:\OD\Foursquare ITP\Projects - WMATA Datamart\Task 3 - Bus Priority"
    path_source_data = os.path.join(path_sp, "data", "00-Raw")
    path_processed_data = os.path.join(path_sp, "Data", "02-Processed")
    # Server credentials
    config = dotenv_values(os.path.join(path_working, '.env'))
    # other things for wylie's dev environment

import wmatarawnav as wr

# Globals
hi_routes = ['37', '39', '42', '43', 'G8', '30N', '30S', '32', '33', '36']
analysis_routes = hi_routes
# EPSG code for WMATA-area work
wmata_crs = 2248


# Make Output Directory
ids_to_loc = pd.DataFrame()

# Iterate over every route pattern's ids, then send to
for analysis_route in analysis_routes:
    print(analysis_route)
    rawnav = (
        pq.read_table(
            source=os.path.join(path_processed_data, "decomp_match_hi.parquet"),
            use_pandas_metadata=True,
            filters=[('route', '=', analysis_route)]
        )
        .to_pandas()
    )

    id_dist = (
        rawnav
        .groupby(['id'], sort=False, as_index=False)
        .agg(
            latmatch=('latmatch', 'first'),
            longmatch=('longmatch', 'first')
        )
    )

    ids_to_loc = pd.concat([ids_to_loc, id_dist])


# now that we have them all, get a distinct list

ids_to_loc_dist = (
    ids_to_loc
    .drop_duplicates(['id'])
)

# locate them using the matched coord.
id_shape = wr.locate(ids_to_loc_dist, latcol='latmatch', longcol='longmatch')

# dedupe again, sometimes we get additional hits near intersections
id_shape_dist = (
    id_shape
    .drop_duplicates(['edge_id', 'edge_shape'])
)

# Convert the shapes from polyline to geopandas
id_shape_dist['edge_decoded'] = (
    id_shape_dist['edge_shape']
    .apply(
        lambda x: wr.run_decode(x)
    )
)

# Convert to geodataframe
id_shape_dist = (
    gpd.GeoDataFrame(
        id_shape_dist,
        geometry="edge_decoded",
        crs="EPSG:4326"
    )
)

# Export to geojson
## Could also export to parquet in newer formats, but i think this is okay for this size of data.
# TODO: move this to processed, not interim
(
    id_shape_dist
    .to_file(
        os.path.join(path_sp, "Data", "01-Interim", "meili_shapes_hi.geojson"),
        driver='GeoJSON'
    )
)
