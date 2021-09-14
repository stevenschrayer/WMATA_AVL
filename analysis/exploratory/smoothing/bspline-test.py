# -*- coding: utf-8 -*-
"""
Created on Thu Aug 12 11:44:39 2021

@author: WylieTimmerman
"""

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

x = rawnav.sec_past_st.to_numpy()
y = rawnav.odom_ft.to_numpy()

# %% Try 1
# %%% interpolate
t, c, k = (
    interpolate.splrep(
        x,
        y,
        s=.5,
        k=3
    )
)

# 
spline = interpolate.BSpline(t, c, k, extrapolate=False)

rawnav_pred = (
    pd.DataFrame(
        {'sec_past_st' : x,
         'odom_ft_pred' : spline(x)}    
    )
    .assign(
        test = 1
    )
)

# %%% plot
# in plotly

fig = px.line(
    rawnav_pred, 
    x='sec_past_st', 
    y='odom_ft_pred'
)

fig.add_scatter(
    x = rawnav['sec_past_st'], 
    y = rawnav['odom_ft'],
    mode = "markers"
)

fig.show()

# %%% check diffs

rawnav_compare = (
    rawnav
    .filter([
        'sec_past_st',
        'odom_ft'
        ],
        axis = "columns"
    )
    .merge(
        rawnav_pred,
        on = "sec_past_st",
        how = "left"
    )
)

# %% Try 2
# %% interpolate

# somehow there is no y argument listed in instructions, but it's definitely required..?
# oh, prep vs rep
# https://docs.scipy.org/doc/scipy/reference/generated/scipy.interpolate.splrep.html

# TODO: kind of gave up here: kind of seemed like the knots are basiclaly the points we need 
# to model, and we end up providing the same list

# looks like could set wieghts as  np.ones(len(x))
# then could modify s to different things
# default of s is then len(x) - sqrt(2* len(x))
t, c, k = (
    interpolate.splrep(
        x,
        y,
        w = np.ones(len(x)),
        s=1,
        k=3
    )
)

# 
spline = interpolate.BSpline(t, c, k, extrapolate=False)

rawnav_pred = (
    pd.DataFrame(
        {'sec_past_st' : x,
         'odom_ft_pred' : spline(x)}    
    )
)

# %% plot
# in plotly

fig = px.line(
    rawnav_pred, 
    x='sec_past_st', 
    y='odom_ft_pred'
)

fig.add_scatter(
    x = rawnav['sec_past_st'], 
    y = rawnav['odom_ft'],
    mode = "markers"
)

fig.show()

# %% other items
# This came from the vizviewer thing
# https://towardsdatascience.com/autonomous-driving-dataset-visualization-with-python-and-vizviewer-24ce3d3d11a0?gi=d0cda85b9d64
# TODO: i don't understand the two calls to splprep below with different args and outputs,
# nor how i call these deriv fucntions
"""
   yaws - numpy array to yaw values for each frame
   t - timestamps for each frame
"""

# weights = np.ones(len(t))
# smooth = len(self.weights)
# # higher order polynomial of odd degree for good fit
# degree = 5
# tck, u = splprep(yaws, weights, t, k=degree, s=smooth)
# yaws_spline = splprep(yaws, t, weights=weights, degree=5, smooth=smooth)
# yaw_rate = yaws_spline(deriv=1)
# # calculate 2nd order derivatives
# yaw_accel = yaws_spline(deriv=2)
# # calculate 3rd order derivatives
# yaw_jerk = yaws_spline(deriv=3)