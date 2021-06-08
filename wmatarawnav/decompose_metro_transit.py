# -*- coding: utf-8 -*-
"""
Created on Mon June 7 03:48 2021

@author: WylieTimmerman
"""
import pandas as pd
import numpy as np
from . import low_level_fns as ll

def decompose_basic_mt(
    rawnav,
    max_fps = 73.3,
    stop_zone_upstream_ft = 150,
    stop_zone_downstream_ft = 150):

    # identify stop zone based on +
    rawnav_w_zone = (
        rawnav
        .assign(
            stop_zone = 
        )

    )
                        