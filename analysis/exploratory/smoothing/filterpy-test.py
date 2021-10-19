# -*- coding: utf-8 -*-
"""
Created on Fri Aug 13 02:12:09 2021

@author: WylieTimmerman
"""

from filterpy.kalman import KalmanFilter
import numpy as np

# % Environment Setup
import os, sys, pandas as pd, numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import dotenv_values
import scipy.interpolate as interpolate
import matplotlib.pyplot as plt
import plotly.express as px 
import plotly.io as pio
import plotly.graph_objects as go


pio.renderers.default='browser'

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

# %% read in the data

rawnav = pd.read_csv(os.path.join(path_sp,"Data","01-Interim","test_decomp_mov10.csv"))

rawnav_fil = rawnav[['sec_past_st','odom_ft']]

# %% from the docs
zs = [t + np.random.randn()*4 for t in range (40)]
# TODO: seems like I need a kalman filter already setup and kalman is the object 
# that we call batch_filter on, with zs as the positional argument

KalmanFilter.batch_filter(zs)

(mu, cov, _, _) = kalman.batch_filter(zs)

#        Xs : numpy.array
#                array of the means (state variable x) of the output of a Kalman
#                filter.

#         Ps : numpy.array
#             array of the covariances of the output of a kalman filter.

#         Fs : list-like collection of numpy.array, optional
#             State transition matrix of the Kalman filter at each time step.
#             Optional, if not provided the filter's self.F will be used

#         Qs : list-like collection of numpy.array, optional
#             Process noise of the Kalman filter at each time step. Optional,
#             if not provided the filter's self.Q will be used

#         inv : function, default numpy.linalg.inv
#             If you prefer another inverse function, such as the Moore-Penrose
#             pseudo inverse, set it to that instead: kf.inv = np.linalg.pinv


(x, P, K, Pp) = (
    rts_smoother(
        mu, 
        cov, 
        kf.F, 
        kf.Q
    )
)

# %% from issues page
# from this

# Can't get this to work :( 
def kalman_filter_smooth(data, r_noise=5.):
    cols = data.shape[1]
    f1 = KalmanFilter(dim_x=cols*2, dim_z=cols)
    f2 = KalmanFilter(dim_x=cols*2, dim_z=cols)

    
    initData = []
    for i in range(data.shape[1]):
        initData.append(data[0,i])
        initData.append(data[1,i]-data[0,i])
    f1.x=np.array(initData)
    
    # for matrix H
    Hmat = np.zeros((cols, cols*2))
    for r in range(Hmat.shape[0]):
        c = r*2
        Hmat[r,c] = 1.

    # for matrix F
    Fmat = np.eye(cols*2)
    for r in range(Fmat.shape[1]):
        if r % 2 == 0:
            Fmat[r,r+1] = 1
    f1.F = Fmat
    f1.H = Hmat
    f1.P *= 50.
    f1.R *= r_noise
    
    xs0,ps0,_,_ = f1.batch_filter(data)
    xs1, ps1, ks, _  = f1.rts_smoother(xs0, ps0)
    
    return xs1, ps1, ks

xs1, ps2, ks = kalman_filter_smooth(rawnav_fil, r_noise = 5.)
