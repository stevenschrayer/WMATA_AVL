# -*- coding: utf-8 -*-
"""
Create by: abibeka, wytimmerman
Purpose: Process rawnav data and output summary and processed dataset.
Created on: Thu Apr  2 12:35:10 2020
"""

from datetime import datetime
import pandas as pd, os, sys, shutil
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
else:
    raise FileNotFoundError("Define the path_working, path_source_data, gtfs_dir, \
                            ZippedFilesloc, and path_processed_data in a new elif block")

import wmatarawnav as wr
# analysis_routes = ['30N','30S','32','33','36']
# later = ['37','39','42','43','G8']
analysis_routes = ['G8']

#### Load Raw RawNav data
# reload inventory
rawnav_inventory = (
    pd.read_parquet(path=os.path.join(path_processed_data,"rawnav_inventory_mult_repart.parquet"))
    .assign(
        #returned as categorical
        filename = lambda x: x.filename.astype(str),
        line_num = lambda x: pd.to_numeric(x.line_num, errors='coerce').convert_dtypes(),
        year = lambda x: x.tag_date.dt.year.astype('Int64')
    ) 
)

rawnav_inventory_filtered = (
    rawnav_inventory
    .groupby('filename',sort = False)
    .filter(lambda x: (x.route.isin(analysis_routes).any() & x.year.ne(2021).any()))
)

rawnav_inventory_filtered = (
    rawnav_inventory_filtered
    .loc[rawnav_inventory_filtered.file_id.isin(['02143171014','02172171020','02212171018'])]    
)

if len(rawnav_inventory_filtered) == 0:
    raise Exception("No Analysis Routes found in file_universe")

# Iterate over each file, skipping to the first row where data in our filtered inventory is found
# Rather than read run-by-run, we read the rest of the file, then filter to relevant routes later
rawnav_inv_filt_first = rawnav_inventory_filtered.groupby(['fullpath', 'filename']).line_num.min().reset_index()
rawnav_inventory_filtered_valid = rawnav_inventory_filtered.copy(deep = True)

# data is loaded into a dictionary named by the ID
route_rawnav_tag_dict = {}

for index, row in rawnav_inv_filt_first.iterrows():
    tag_info_line_no = rawnav_inventory_filtered[rawnav_inventory_filtered['filename'] == row['filename']]
    reference = min(tag_info_line_no.line_num)
    # -1 refers to the fact that the tag line identifying the start of a run will be removed, such
    # that the second row associated with a run will become the first row of data. This helps to 
    # ensure that indices of the processed data will line up with values in the rawnav inventory
    tag_info_line_no.loc[:, "new_line_no"] = tag_info_line_no.line_num - reference - 1
    temp = wr.load_rawnav_data(
        zip_folder_path=row['fullpath'],
        skiprows=row['line_num'])

    if type(temp) != type(None):
        route_rawnav_tag_dict[row['filename']] = dict(RawData=temp, tagLineInfo=tag_info_line_no)
    else:
        remove_file = row['filename']  
        rawnav_inventory_filtered_valid  = rawnav_inventory_filtered_valid.query('filename!= @remove_file')


#### Clean RawNav data
rawnav_data_dict = {}

for key, datadict in route_rawnav_tag_dict.items():
    print(key)
    temp_dat = wr.clean_rawnav_data_alt(
        data_dict = datadict,
        filename = key,
        analysis_routes = analysis_routes
    )

    rawnav_data_dict[key] = temp_dat['rawnavdata']

out_rawnav_dat = pd.concat(rawnav_data_dict)

#### Output
# Path Setup
path_rawnav_data = os.path.join(path_processed_data, "rawnav_data_mult.parquet")

if os.path.isdir(path_rawnav_data):
    shutil.rmtree(os.path.join(path_rawnav_data), ignore_errors=True) 
if not os.path.isdir(path_rawnav_data):
    os.mkdir(path_rawnav_data)
  
(
 pq.write_to_dataset(
     pa.Table.from_pandas(
         out_rawnav_dat,
         schema = wr.rawnav_data_schema()
     ), 
     root_path=os.path.join(path_rawnav_data),
     partition_cols=['route'])
)    
 