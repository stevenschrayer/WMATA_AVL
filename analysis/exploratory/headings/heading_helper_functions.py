# -*- coding: utf-8 -*-
"""
Created on Tue Aug 24 23:29:24 2021

@author: JackMcDowell
"""


# Entirely copied from Wylie's movement decomposition, replacing odometer 
# calculations with headings

import pandas as pd
import numpy as np
import wmatarawnav as wr
from wmatarawnav import low_level_fns as ll
from wmatarawnav import decompose_rawnav as dr
from math import factorial


#### interpolation functions
# despite the name, this is called by interp_over_sec
def interp_column(rawnav_ti, col_name, threshold = 1, fix_interp = True, interp_method = "index"):
    # threshold is how far outside the bands of observed values we would allow. a little
    # wiggle room probbaly okay given how we understand these integer issues appearing
    # breakpoint()
    if ('sec_past_st' not in rawnav_ti.columns):
        print('missing sec_past_st in columns')
#        breakpoint()
    
    rawnav_ti.set_index(['sec_past_st'], inplace = True)
    
    if (rawnav_ti.index.duplicated().any()):
        rawnav_ti.reset_index(inplace = True)
        raise ValueError("sec_past_st shouldn't be duplicated at this point")
    else:
        # interpolate
        rawnav_ti[col_name] = rawnav_ti[col_name].interpolate(method = interp_method)
        
        # The _min and _max columns should already exist from calling agg_sec()
        assign_statement1 = {col_name+'_low' : lambda x, thr = threshold : ((x[col_name] < (x[col_name+'_min'] - thr))),
                             col_name+'_hi' : lambda x, thr = threshold : ((x[col_name] > (x[col_name+'_max'] + thr)))}        
        
        assign_statement2 = {col_name : lambda x, thr = threshold: np.select(
                    [
                        #I think we avoid evaluating other conditions 
                        # if the first case is true
                        x[col_name+'_low'].eq(False) & x[col_name+'_hi'].eq(False),
                        x[col_name+'_low'],
                        x[col_name+'_hi'],
                    ],
                    [
                        x[col_name],
                        x[col_name+'_min'] - thr,
                        x[col_name+'_max'] + thr
                    ],
                    default = x[col_name] # this is probably overkill
                    )   
        }
        
        # test
        # Could probably fix some of this, but oh well
        if (fix_interp == True):
            rawnav_ti = (
                rawnav_ti
                .assign(
                    **assign_statement1
                )
                .assign(
                    **assign_statement2 
                )
                
            )
        
        # this is a recalculation after fixes above
        rawnav_ti = (
            rawnav_ti
            .assign(
                interp_fail = lambda x, thr = threshold : (
                    (x[col_name] < (x[col_name+'_min'] - thr)) |
                    (x[col_name] > (x[col_name+'_max'] + thr))
                )
            )
        )
        
        # output
        rawnav_ti.reset_index(inplace = True)

        return(rawnav_ti)

def agg_sec(rawnav):
    """
    Parameters
    ----------
    rawnav: pd.DataFrame, rawnav data. Expect cols sec_past_st and odom_ft
    Returns
    -------
    rawnav: pd.DataFrame, rawnav data with additional fields.
    Notes
    -----
    """
    
    # Grouping everything is pretty slow, so we'll split out the repeated second cases, fix
    # the seconds/odom ft issues, then recombine
    # we'll also split out the stop window items, as they make the agg business a bit trickier
    rawnav = (
        rawnav
        .assign(
            dupes = lambda x:
                x.duplicated(
                    subset = ['filename','index_run_start','sec_past_st'],
                    keep = False
            ),
            stop_window_x = lambda x:
                np.where(
                    x.stop_window.eq('X-1'),
                    'X-1',
                    None # None might be better, but later we'll end up creating blanks anyway.
                ),
            stop_window_e = lambda x:
                np.where(
                    x.stop_window.str.contains('E'),
                    x.stop_window,
                    None
                ),
            # placeholder we'll update later
            collapsed_rows = 1
        )
    )
     
    # Separate out the non-duplicated ones to save processing time
    rawnav_nodupe = (
        rawnav.loc[rawnav.dupes == False]
    )
    # Even though we could try to pull out only the ones that have repeated values and perform
    # the agg on them, seems a little safer as we get going to do for all and then optimize
    # later.
    rawnav_dupe = (
        rawnav
        .loc[rawnav.dupes == True]
        # trick here is i'm not sure what columns we'll keep
        .groupby(
            [
                # these are the parts that don't vary by instance
                # not sure if it's slower to do the big group or to
                # just grab 'first' for each of these.
                'filename',
                'index_run_start',
                # this is the one that we actually care about
                'sec_past_st'
            ],
            as_index = False
        )
        .agg(
            # some of these are just to avoid a bigger groupby call
            route_pattern = ('route_pattern',"first"),
            pattern = ('pattern',"first"),
#            index_run_end = ('index_run_end',"first"),
            route = ('route',"first"),
#            wday = ('wday',"first"),
#            start_date_time = ('start_date_time',"first"),
            # in this sense, we're starting to lose data and have to make judgment calls
            index_loc = ('index_loc','max'),
            lat = ('lat','last'),
            long = ('long','last'),
            heading = ('heading','last'),
            heading_min = ('heading','min'),
            heading_max = ('heading', 'max'),
            # i'm hoping it's never the case that door changes on the same second
            # if it does, will be in a world of pain.
            # this join works better when we expect every row to be filled
            veh_state = ('veh_state', lambda x: ','.join(x.unique().astype(str))),
            # we'll impute this later, but for now, we just fill
            odom_ft = ('odom_ft','last'),
            odom_ft_min = ('odom_ft','min'),
            odom_ft_max = ('odom_ft','max'),
#            sat_cnt = ('sat_cnt','last'),
            # TODO: taking the last might hide a door open that lasts <1 second, but that seems
            # unlikly and we aren't likely to care.
            door_state = ('door_state', 'last'),
            door_state_all = ('door_state', lambda x: ','.join(x.unique().astype(str))),
            # for stop_window, we are more likely to have blanks, so this works
            # we also are likely to run into instances where the stop window close tags 
            # show up in the same second as the stop window, so this just sorts them into a separate
            # column
            stop_window_e = ('stop_window_e', lambda x: x.str.cat(sep=",",na_rep = None)),
            stop_window_x = ('stop_window_x', lambda x: x.str.cat(sep=",",na_rep = None)),
#            blank = ('blank', lambda x: ','.join(x.unique().astype(int).astype(str))),
#            lat_raw = ('lat_raw','last'),
#            long_raw = ('long_raw','last'),
            # TODO: possible we don't want to collapse this...
            row_before_apc = ('row_before_apc', lambda x: ','.join(x.unique().astype(int).astype(str))),
            collapsed_rows = ('index_loc','count')
        )
        .assign(
            stop_window_e = lambda x: 
                np.where(
                    x.stop_window_e.eq(''),
                    None,
                    x.stop_window_e
                ),
            stop_window_x = lambda x: 
                np.where(
                    x.stop_window_x.eq(''),
                    None,
                    x.stop_window_x
                )
        )
    )
            
    # recombine duplicated and non-duplicated
    rawnav = (
        pd.concat([
            rawnav_dupe,
            rawnav_nodupe
        ])
        .sort_values(
            by = ['filename','index_run_start','index_loc']    
        )
        .drop(
            ['dupes','stop_window'],
            axis = "columns"
        )
    )
    
    # Next, we think some of the aggregated values are a little bit screwy, so we 
    # set to NA and interpolate
    rawnav = (
        rawnav
        .pipe(
            # Should have done this sooner, but definitely necessary after these aggregations
            ll.reorder_first_cols,
            ['filename',
             'index_run_start',
             'route_pattern',
             'pattern',
#             'index_run_end',
             'route',
#             'wday',
#             'start_date_time',
             'index_loc',
             'odom_ft',
             'sec_past_st',
             'heading',
             'door_state',
             'veh_state',
#             'row_before_apc',
             'lat',
             'long',
#             'lat_raw',
#             'long_raw',
#             'sat_cnt',
             'collapsed_rows',
             'heading_min',
             'heading_max',
             'odom_ft_min',
             'odom_ft_max',
             'door_state_all',
             'stop_window_e',
             'stop_window_x',
             'row_before_apc'
             ]
        )            
    )
    
    return(rawnav)

def interp_column_over_sec(rawnav, col_name, interp_method = "index"):
    
    #### first, set as NA the values that we plan to interpolate over.
    # TODO: In the future, we probably will not want to be dropping data,
    # it's a bit sloppy. For now there are some known 'bad' values and it's a little
    # bit easier to just get rid of them and linearly interpolate since we're only looking
    # at a few cases.

    # first, we look for ones that have one missing second afterwards but have one second before
    # for various reasons, these odometers tend to come out high
    rawnav['sec_past_st_next'] = (
        rawnav
        .groupby(['filename','index_run_start'], sort = False)['sec_past_st']
        .shift(-1)
    )
    
    rawnav['sec_past_st_lag'] = (
        rawnav
        .groupby(['filename','index_run_start'], sort = False)['sec_past_st']
        .shift(1)
    )
    
    assign_statement1 = {col_name : lambda x: np.where(x.secs_next.eq(2) & x.secs_last.eq(1),
                                                      np.nan,
                                                      x[col_name]
                                                      )
            }
    
    rawnav = (
        rawnav
        .assign(
            secs_next = lambda x: x.sec_past_st_next - x.sec_past_st,
            secs_last = lambda x: x.sec_past_st - x.sec_past_st_lag
        )
        .assign(
            **assign_statement1
        )
    )
    
    assign_statement2 = {col_name : lambda x: np.where(x.collapsed_rows.eq(1),
                                                       x[col_name],
                                                       np.nan
                                                       ),
                        'interp_fail' : lambda x: np.nan
    }
    
     # where we collapsed, let's NA these out for interpolate
     # if the repeated values are the same, we should probably not NA these out
    rawnav = (
        rawnav
        .assign(
            **assign_statement2
        )
    )

    #### interpolate the heading values 
    rawnav = (
        rawnav
        .reset_index(drop=True) # getting weird indices out of order
        .groupby(['filename','index_run_start'], sort = False)
        # TODO: we should also probably interpolate heading
        .apply(lambda x: interp_column(x, col_name, interp_method = interp_method))
        .reset_index(drop = True)
    ) 
        
    
    #### Clean up
    
    rawnav = (
        rawnav
        .drop(
            [
            'sec_past_st_next',
            'sec_past_st_lag',
            'secs_next',
            'secs_last',
            'interp_fail'
            ],
            axis = "columns"
        )
    )
    
    return(rawnav)

def calc_rawnav_speed(rawnav, col_name):
    
    #### lag values
    rawnav[[col_name+'_next','sec_past_st_next']] = (
        rawnav
        .groupby(['filename','index_run_start'], sort = False)[[col_name,'sec_past_st']]
        .transform(lambda x: x.shift(-1))
    )
    
    
    assign_statement1 = {'secs_marg' : lambda x: x.sec_past_st_next - x.sec_past_st,
                        col_name+'_marg' : lambda x: x[col_name+'_next'] - x[col_name],
                        col_name+'_speed_next' : lambda x: ((x[col_name+'_next'] - x[col_name]) / 
                                (x.sec_past_st_next - x.sec_past_st))
                        }
    
    assign_statement2 = {col_name+'_speed_next' : lambda x: x[col_name+'_speed_next'].replace([np.nan],0)}
    
    #### calculate degrees per second
    rawnav = (
        rawnav
        .assign(
            **assign_statement1
        )
        # if you get nan's, it's usually zero travel distance and zero time around 
        # doors. the exception is at the end of the trip.
        .assign(
            **assign_statement2
        )
    )
        
    # if you're the last row , we reset you back to np.nan
    rawnav.loc[rawnav.groupby(['filename','index_run_start']).tail(1).index, col_name+'_speed_next'] = np.nan

    return(rawnav)
    
def calc_rawnav_accel(rawnav, speed_col, groupvars = ['filename','index_run_start']):
    # a little inefficient to recalculate this, but we're tryign to call this within the exapnded
    # data as well.
    rawnav['sec_past_st_next'] = (
        rawnav
        .groupby(groupvars, sort = False)['sec_past_st']
        .shift(-1)
    )
    
    speed_lag_col = speed_col + "_lag"
    
    #### Calculate acceleration
    rawnav[[speed_lag_col]] = (
        rawnav
        .groupby(groupvars, sort = False)[[speed_col]]
        .transform(lambda x: x.shift(1))
    )

    assign_statement1 = {speed_col+'_accel' : lambda x: (x[speed_col]- x[speed_lag_col]) / (x.sec_past_st_next - x.sec_past_st)}
    assign_statement2 = {}


    # Note, not replicating the 3rd ping lag, as it's a little dicey i htink
    rawnav = (
        rawnav 
        .assign(
            # this may seem a bit screwy, but it lines up well with intuitions when visualized
            # can share some notebooks (99-movement-explore-*.Rmd) 
            # that illustrate differences between approaches here if desired
            # accel_next is also a bit of a misnomer; more like accel_at_point, 
            # because some downstream/notebook code depends on accel_next, sticking to that
            # nomenclature for now
            deg_accel_next = lambda x: (x[speed_col]- x[speed_lag_col]) / (x.sec_past_st_next - x.sec_past_st)
        )
        # as before, we'll set these cases to nan and then fill
         .assign(
            deg_accel_next = lambda x: x.deg_accel_next.replace([np.Inf,-np.Inf],np.nan),
        )
    )
    
    # but now, if you're the last row, we reset you back to np.nan
    rawnav.loc[rawnav.groupby(groupvars).tail(1).index, 'deg_accel_next'] = np.nan
         
    #### Cleanup
    # drop some leftover cols
    rawnav = (
        rawnav
        .drop([
            speed_lag_col,
            'sec_past_st_next'
            ],
            axis = "columns"
        )
    )
    
    return(rawnav)
    
    
#### Smoothing

# borrowed from : https://scipy-cookbook.readthedocs.io/items/SavitzkyGolay.html
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
    lastvals = y[-1] + np.abs(y[-half_window-1:-1][::-1] - y[-1])
    y = np.concatenate((firstvals, y, lastvals))
    return np.convolve( m[::-1], y, mode='valid')

def expand_rawnav_column(rawnav_ti, expand_col):
    # for use in the .assign() call below. This allows us to name the column using a function input
    assign_statement = {expand_col : lambda x: x[expand_col].ffill()}
    
    rawnav_ti_expand = (
        pd.DataFrame(
            {'sec_past_st': np.arange(rawnav_ti.sec_past_st.min(), rawnav_ti.sec_past_st.max(),1 )} 
        )
        .merge(
             rawnav_ti[['sec_past_st',expand_col]],
             on = 'sec_past_st',
             how = 'left'
        )
        .assign(
            **assign_statement
        )
    )
    
    return(rawnav_ti_expand)

# TODO: consider an approach that will let us smooth other things too
# for each trip instance, this expands the data, applies the savitzy golay method, and 
# recalculates accel and jerk
def apply_smooth_rawnav(rawnav_ti, smooth_col, window_size):
    
    rawnav_ex = expand_rawnav_column(rawnav_ti, smooth_col)
    rawnav_ex[smooth_col+"_sm"] = savitzky_golay(rawnav_ex[smooth_col].to_numpy(), window_size, 3)    
    
    # Heading and angular speed values need to be able to be negative. May need
    # to add this back in as a conditional statement
#    rawnav_ex = (
#        rawnav_ex
#        .assign(
#            deg_sec_next_sm = lambda x: np.where(
#                x.deg_sec_next_sm.le(0),
#                0,
#                x.deg_sec_next_sm
#            )    
#        )    
#    )
    
    # Not bothering with accel right now
#    rawnav_ex = (
#        rawnav_ex
#        # a little hack to shortcut the need to group, since we're doing all this by trip 
#        # instance
#        .assign(grp = 1)
#        .pipe(
#            calc_angular_accel,
#            groupvars = ['grp'], 
#            speed_col = 'deg_sec_next_sm'
#        )
#        .drop(['grp'], axis = 'columns')
#    )
        
    rawnav_ti = pd.merge(
        rawnav_ti
        # drop the placeholder columns
        .drop(
            [smooth_col+"_sm"
#             'deg_accel_next'
             ],
            axis = 'columns'
        ),
        rawnav_ex
        .drop(
            [smooth_col],
            axis = "columns"
        ),
        on = ['sec_past_st'],
        how = 'left'
    )

    return(rawnav_ti)

# this is a parent function that handles some placeholder column creation, gropuing, and applying
# the smoothing function
def smooth_rawnav_column(rawnav, smooth_col, window_size):
    
    # this requires interpolated data, according to internet
    # https://gis.stackexchange.com/questions/173721/reconstructing-modis-time-series-applying-savitzky-golay-filter-with-python-nump
    
    assign_statement = {smooth_col+"_sm" : np.nan}
#                        deg_accel_next : np.nan}
    
    rawnav = (
        rawnav
        .assign(
            # i'm not sure if these placeholders are necessary or not
            **assign_statement
        )
        .groupby(['filename','index_run_start'],sort = False)
        .apply(
            lambda x: apply_smooth_rawnav(x, smooth_col, window_size)
        )
        # i'm not sure how the index changes, but oh well
        .reset_index(drop = True)
    )
    
    return(rawnav)