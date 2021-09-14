# % Environment Setup
import os, sys, pandas as pd, pyarrow.parquet as pq

# For postgresql
# TODO: for now, skipping, as amit says it's a bit slow
from dotenv import dotenv_values
# import pg8000.native # not strictly required to load, but is used by sqlalchemy below
# import sqlalchemy
import pyarrow as pa

if os.getlogin() == "WylieTimmerman":
    # Working Paths
    # the following needed to run in vscode jupyter interpreter
    os.environ["GDAL_DATA"] = os.environ["CONDA_PREFIX"] + "\Library\share\gdal"
    path_working = r"C:\Users\WylieTimmerman\Documents\projects_local\WMATA_AVL_datamart"
    os.chdir(os.path.join(path_working))
    sys.path.append(r"C:\Users\WylieTimmerman\Documents\projects_local\WMATA_AVL_datamart")
    path_sp = r"C:\OD\Foursquare ITP\Projects - WMATA Datamart\Task 3 - Bus Priority"
    path_source_data = os.path.join(path_sp,"data","00-Raw")
    path_processed_data = os.path.join(path_sp, "data","02-Processed")
    # Server credentials
    config = dotenv_values(os.path.join(path_working, '.env'))
    # other things for wylie's dev environment        
    # from IPython import get_ipython  
    
    # ipython.magic("reset -f")
    # ipython = get_ipython()
    # ipython.magic("load_ext autoreload")
    # ipython.magic("autoreload 2")
    

# Globals
tsp_route_list = ['30N','30S','33','31']
analysis_routes = '30N'
analysis_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
# EPSG code for WMATA-area work
wmata_crs = 2248

import wmatarawnav as wr

# Load the summary and export

stop_index = (
    pq.read_table(source=os.path.join(path_processed_data,"stop_index.parquet"),
                    use_pandas_metadata = True
    )
    .to_pandas()
    # As a bit of proofing, we confirm this is int32 and not string, may remove later
    .assign(pattern = lambda x: x.pattern.astype('int32')) 
    .rename(columns = {'odom_ft' : 'odom_ft_stop'})
    .reset_index()
)

stop_index_out = (
    stop_index 
    .filter(
        ['route','direction','pattern','pattern_destination',
        'stop_id','order','stop_sort_order','geo_description',
        'stop_lon','stop_lat','stop_sequence'
        ]
    )
    .drop_duplicates()
)

stop_index_out.to_csv("stop_index_out.csv")