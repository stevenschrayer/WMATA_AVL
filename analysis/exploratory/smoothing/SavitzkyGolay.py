# -*- coding: utf-8 -*-
"""
Created on Fri Aug 13 04:24:09 2021

@author: WylieTimmerman
"""

import pandas as pd
import os, sys
from dotenv import dotenv_values
import numpy as np
from math import factorial

import plotly.express as px 
import plotly.io as pio
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

rawnav = pd.read_csv(os.path.join(path_sp,"Data","01-Interim","test_decomp_mov10.csv"))

# and inspired by https://stackoverflow.com/questions/20618804/how-to-smooth-a-curve-in-the-right-way
def savitzky_golay(y, window_size, order, deriv=0, rate=1):
    r"""Smooth (and optionally differentiate) data with a Savitzky-Golay filter.
    The Savitzky-Golay filter removes high frequency noise from data.
    It has the advantage of preserving the original shape and
    features of the signal better than other types of filtering
    approaches, such as moving averages techniques.
    Parameters
    ----------
    y : array_like, shape (N,)
        the values of the time history of the signal.
    window_size : int
        the length of the window. Must be an odd integer number.
    order : int
        the order of the polynomial used in the filtering.
        Must be less then `window_size` - 1.
    deriv: int
        the order of the derivative to compute (default = 0 means only smoothing)
    Returns
    -------
    ys : ndarray, shape (N)
        the smoothed signal (or it's n-th derivative).
    Notes
    -----
    The Savitzky-Golay is a type of low-pass filter, particularly
    suited for smoothing noisy data. The main idea behind this
    approach is to make for each point a least-square fit with a
    polynomial of high order over a odd-sized window centered at
    the point.
    Examples
    --------
    t = np.linspace(-4, 4, 500)
    y = np.exp( -t**2 ) + np.random.normal(0, 0.05, t.shape)
    ysg = savitzky_golay(y, window_size=31, order=4)
    import matplotlib.pyplot as plt
    plt.plot(t, y, label='Noisy signal')
    plt.plot(t, np.exp(-t**2), 'k', lw=1.5, label='Original signal')
    plt.plot(t, ysg, 'r', label='Filtered signal')
    plt.legend()
    plt.show()
    References
    ----------
    .. [1] A. Savitzky, M. J. E. Golay, Smoothing and Differentiation of
       Data by Simplified Least Squares Procedures. Analytical
       Chemistry, 1964, 36 (8), pp 1627-1639.
    .. [2] Numerical Recipes 3rd Edition: The Art of Scientific Computing
       W.H. Press, S.A. Teukolsky, W.T. Vetterling, B.P. Flannery
       Cambridge University Press ISBN-13: 9780521880688
    """

    # breakpoint()
    try:
        window_size = np.abs(int(window_size))
        order = np.abs(int(order))
    except ValueError: # minor modification in this line
        raise ValueError("window_size and order have to be of type int")
    if window_size % 2 != 1 or window_size < 1:
        raise TypeError("window_size size must be a positive odd number")
    if window_size < order + 2:
        raise TypeError("window_size is too small for the polynomials order")
    order_range = range(order+1)
    half_window = (window_size -1) // 2
    # precompute coefficients
    b = np.mat([[k**i for i in order_range] for k in range(-half_window, half_window+1)])
    m = np.linalg.pinv(b).A[deriv] * rate**deriv * factorial(deriv)
    # pad the signal at the extremes with
    # values taken from the signal itself
    firstvals = y[0] - np.abs( y[1:half_window+1][::-1] - y[0] )
    lastvals = y[-1] + np.abs(y[-half_window-1:-1][::-1] - y[-1]) # get error here
    y = np.concatenate((firstvals, y, lastvals))
    return np.convolve( m[::-1], y, mode='valid')

# interpolate intermediate points

rawnav_expand = (
    pd.DataFrame(
        {'sec_past_st': np.arange(rawnav.sec_past_st.min(), rawnav.sec_past_st.max(),1 )} 
    )
    .merge(
         rawnav[['sec_past_st','fps_next','odom_ft']],
         on = 'sec_past_st',
         how = 'left'
    )
    .assign(
        fps_next = lambda x: x.fps_next.ffill(),
        odom_ft = lambda x: x.odom_ft.interpolate(method = "index")
    )
)

# this looks okay
rawnav_expand['fps_next_sm'] = savitzky_golay(rawnav_expand['fps_next'].to_numpy(), 21, 3)

# this also looks okay
# 3rd degree polynomial means nothing too crazy happens, 30 second window is not too big
# probably a little smaller is better though
# rawnav_expand['fps_next_sm'] = savitzky_golay(rawnav_expand['fps_next'].to_numpy(), 31, 3)

rawnav_expand = (
    rawnav_expand
    .assign(
        fps_next_sm = lambda x: np.where(
            x.fps_next_sm.le(0),
            0,
            x.fps_next_sm
        )    
    )    
)

# remaining issue is that some predictions are negative. i think we could just set these to 0, i guess.
# %% plot
fig = px.line(
    rawnav_expand, 
    x='sec_past_st', 
    y='fps_next_sm'
)

fig.add_scatter(
    x = rawnav['sec_past_st'], 
    y = rawnav['fps_next'],
    mode = "markers"
)

fig.show()

# %% applying to just odom ft
# this actually looks okay too
rawnav_expand['odom_ft_sm'] = savitzky_golay(rawnav_expand['odom_ft'].to_numpy(), 31, 3)
# this actually looks okay too
rawnav_expand['odom_ft_sm'] = savitzky_golay(rawnav_expand['odom_ft'].to_numpy(), 31, 5)
# anotehr options
rawnav_expand['odom_ft_sm'] = savitzky_golay(rawnav_expand['odom_ft'].to_numpy(), 21, 5)

fig = px.line(
    rawnav_expand, 
    x='sec_past_st', 
    y='odom_ft_sm'
)

fig.add_scatter(
    x = rawnav['sec_past_st'], 
    y = rawnav['odom_ft'],
    mode = "markers"
)

fig.show()

# what is the difference
rawnav_expand = (
    rawnav_expand
    .assign(
        odom_diff = lambda x: x.odom_ft_sm - x.odom_ft,
        fps_next_diff = lambda x: x.fps_next_sm - x.fps_next,
    )
)

# %% what if we calc speed off these smoothed values
rawnav_expand['odom_ft_sm_next'] = (
    rawnav_expand['odom_ft_sm'].shift(-1)
)

rawnav_expand = (
    rawnav_expand
    .assign(
        # assuming one sec per observation
        fps_next_on_odom_sm = lambda x: x.odom_ft_sm_next - x.odom_ft_sm
    )    
)

fig = px.line(
    rawnav_expand, 
    x='sec_past_st', 
    y='fps_next_on_odom_sm'
)

fig.add_scatter(
    x = rawnav['sec_past_st'], 
    y = rawnav['fps_next'],
    mode = "markers"
)

fig.show()

# Hmm, the only weird thing is that your odometer becomes non-monotonic, which
# i think is a little weird. probably better to just infer speed, fix negs to 0, and then
# rejoin