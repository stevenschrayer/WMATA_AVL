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

#### Coords and Params
latlongs = pd.DataFrame({
   "lat" : [41.318818, 41.321001],
   "lon" : [19.461336, 19.459598]
   }
)

# Params
use_costing = "auto"
use_directions = dict(units = "miles")

# Assemble request
data = (
    json.dumps(
        dict(
            locations = latlongs.to_dict(orient = "records"),
            costing = use_costing,
            directions_options = use_directions
        )
    )
)
url = "http://localhost:8002/route"
headers = {'Content-type': 'application/json'}

#### Run request
r = requests.get(url, data=data, headers=headers)

if (r.status_code != 200):
    raise NameError("request failed")

rjson = r.json()

rjsonloads = json.loads(rjson)
# stopping here, from here we probably just need to start parsing the response for 
# a DC case
