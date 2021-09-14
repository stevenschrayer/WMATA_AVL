# -*- coding: utf-8 -*-
"""
Created on Thu Aug 12 11:07:41 2021

@author: WylieTimmerman
"""
# %% standard example
from pykalman import KalmanFilter
import numpy as np
kf = KalmanFilter(transition_matrices = [[1, 1], [0, 1]], observation_matrices = [[0.1, 0.5], [-0.3, 0.0]])
measurements = np.asarray([[1,0], [0,0], [0,1]])  # 3 observations
kf = kf.em(measurements, n_iter=5)
(filtered_state_means, filtered_state_covariances) = kf.filter(measurements)
(smoothed_state_means, smoothed_state_covariances) = kf.smooth(measurements)


# %% my example
import pandas as pd
import os, sys
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
    # other things for wylie's dev environment            

rawnav = pd.read_csv(os.path.join(path_sp,"Data","01-Interim","test_decomp_mov10.csv"))

rawnav_fil = (
    rawnav
    .filter(
        ['odom_ft','sec_past_st'],
        axis = "columns"
    )
    .to_numpy()
)

kf2 = KalmanFilter(
    transition_matrices = [[1, 1], [0, 1]], 
    observation_matrices = [[0.1, 0.5], [-0.3, 0.0]]
)

measurements2 = rawnav_fil

kf2 = kf2.em(measurements2, n_iter=5)

(filtered_state_means2, filtered_state_covariances2) = kf2.filter(measurements2)
(smoothed_state_means2, smoothed_state_covariances2) = kf2.smooth(measurements2)

# WT: wow, this worked really poorly.