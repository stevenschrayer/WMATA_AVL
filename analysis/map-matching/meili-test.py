# -*- coding: utf-8 -*-
"""
Created on Thu Sep 16 01:56:53 2021

@author: WylieTimmerman
"""
# NOTE: requires running valhalla
# Runs test example at 
# https://gis-ops.com/valhalla-how-to-run-with-docker-on-ubuntu/
# except with python requests instead of curl/cli

#### Libs
import requests
import pandas as pd
import json

#### Test

# Coords
latlongs = pd.DataFrame({
   "lat" : [41.318818, 41.321001],
   "lon" : [19.461336, 19.459598]
   }
)

use_latlon = latlongs.to_json(orient = "records")

# Params
use_costing = "auto"
use_directions = dict(units = "miles")


# Assemble request
# NO


# v2
data_dict = (
        dict(
            locations = latlongs.to_dict(orient = "records"),
            costing = use_costing,
            directions_options = use_directions
        )
)

data = json.dumps(data_dict)

url = "http://localhost:8002/route"
headers = {'Content-type': 'application/json'}

r = requests.get(url, data=data, headers=headers)
# so far, got failed to parse request
