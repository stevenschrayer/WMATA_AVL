# -*- coding: utf-8 -*-
"""
Created on Mon June 7 03:48 2021

@author: WylieTimmerman
"""
import pandas as pd
import numpy as np
from . import low_level_fns as ll
from . import decompose_rawnav as dr

# we'll use this to just identify what the heck the bus is doing at any particular
# point in time.

def reset_odom(
    rawnav,
    groupvars = ['filename','index_run_start']    
    ):
    
    rawnav['odom_ft'] = (
        rawnav
        .groupby(['filename','index_run_start'])['odom_ft']
        .transform(
            lambda x: x - min(x)    
        )
    )

    rawnav['sec_past_st'] = (
        rawnav
        .groupby(['filename','index_run_start'])['sec_past_st']
        .transform(
            lambda x: x - min(x)    
        )
    )
    
    return(rawnav)

def decompose_mov2(
    rawnav,
    speed_thresh_fps = 7.333,
    max_fps = 130,# this is about the highest i ever saw when expressing on freeway, so yeah.
    stopped_fps = 3, #upped from 2
    slow_fps = 14.67, # upped default to 10mph
    steady_accel_thresh = 3, #based on some casual observations
    steady_low_thresh = .10): 
    # our goal here is to get to accel/decel/steady state 
    
    # Categorize stopped movement
    # for now, not distinguishing slow
    rawnav = (
        rawnav
        .assign(
            # need a True/False that's easily coercible to numeric
            is_stopped = lambda x, stop = stopped_fps: x.fps_next.le(stop)
        )
    )
    
    # categorize groups
    rawnav['stopped_changes'] = (
    	rawnav
    	.groupby(['filename','index_run_start'])['is_stopped']
    	.transform(lambda x: x.diff().ne(0).cumsum())
    )
    
    rawnav = (
        rawnav
        .assign(
            is_steady = lambda x, thresh = steady_accel_thresh, slow = slow_fps: 
                # seems like the low percentile on steady could catch part of accel phase,
                 # may want to add more conditions here later
                (x.fps_next.ge(slow)) & 
                (x.is_stopped.eq(False)) & 
                # new accel condition
                ((x.accel_next > -thresh) & (x.accel_next < thresh))
        )
    )
    
    # assign accel vs. decel based on where you're at between stopped and steady
    # TODO: will need case where you never get to steady state
    rawnav_seg_steady_lims = (
    	rawnav
        .query('(is_stopped == False) & (is_steady == True)')
    	.groupby(['filename','index_run_start','stopped_changes'])
    	.agg(
            steady_fps_sec_start = ('sec_past_st', 'min'),
            steady_fps_sec_end = ('sec_past_st', 'max')
        )
        .reset_index()
    )

    # rejoin the lims
    rawnav = (
        rawnav
        # .drop(['steady_fps_sec_start','steady_fps_sec_end'], axis = "columns")
        .merge(
            rawnav_seg_steady_lims,
            on = ['filename','index_run_start','stopped_changes'],
            how = "left"
        )
        .assign(
            accel_decel = lambda x: 
                np.select(
                    [
                    (x.sec_past_st < x.steady_fps_sec_start) & x.is_stopped.eq(False), 
                    (x.sec_past_st > x.steady_fps_sec_end) & x.is_stopped.eq(False),
                    x.steady_fps_sec_start.isna()
                    ],
                    [
                     "accel",
                     "decel",
                     "other_delay"
                    ],
                    default = np.nan
                    )
        )
    )
        
    # assign the decomp
    rawnav = (
        rawnav
        .assign(
            basic_decomp = lambda x: np.select(
                [
                x.is_stopped.eq(True),
                x.is_steady.eq(True),
                x.accel_decel.ne("nan"), #TODO: i screwed something up
                x.is_steady.eq(False) & x.is_stopped.eq(False) & x.accel_decel.eq("nan")
                ],
                [
                "stopped",
                "steady",
                x.accel_decel,
                "other_delay"
                ]
            )
        )
    )
    
    # once no longer debugging, drop these cols
    # rawnav = (
    #     rawnav
    #     .drop([
    #         "is_stopped",
    #         "stopped_changes",
    #         "steady_fps",
    #         "steady_fps",
    #         "steady_fps_sec_start",
    #         "steady_fps_sec_end",
    #         "accel_decel"
    #         ],
    #         axis = "columns"
    #     )
    # )
    
    return(rawnav)
        

def decompose_mov(
    rawnav,
    speed_thresh_fps = 7.333,
    max_fps = 130,# this is about the highest i ever saw when expressing on freeway, so yeah.
    stopped_fps = 2, #seems like this intuitively matches 'stopped' on the charts
    slow_fps = 7.34, #this is 5mph; when we see vehicles do this on chart, they are creeping usually
    steady_accel_thresh = 2, #based on some casual observations
    steady_low_thresh = .10): 
    # our goal here is to get to accel/decel/steady state 
    
    # Categorize stopped movement
    # for now, not distinguishing slow
    rawnav = (
        rawnav
        .assign(
            # need a True/False that's easily coercible to numeric
            is_stopped = lambda x, stop = stopped_fps: x.fps_next.le(stop)
        )
    )
    
    # categorize groups
    rawnav['stopped_changes'] = (
    	rawnav
    	.groupby(['filename','index_run_start'])['is_stopped']
    	.transform(lambda x: x.diff().ne(0).cumsum())
    )
    
    # in these groups, find a steady state speed where going fast than slow speed
    # but don't have much acceleration
    # breakpoint()
    rawnav_seg_steady = (
    	rawnav
        .query('(is_stopped == False) & (fps_next >= @slow_fps)')
        .query('(accel_next > -(@steady_accel_thresh)) & (accel_next < @steady_accel_thresh)')
    	.groupby(['filename','index_run_start','stopped_changes'])
    	.agg(
            steady_fps = ('fps_next', lambda x: x.quantile([steady_low_thresh]))
        )
        .reset_index()
    )
    
    # assign the steady state back to data
    rawnav = (
        rawnav
        # mostly for interactive issues with name joins
        # .drop(['steady_fps'], axis = "columns")
        .merge(
            rawnav_seg_steady,
            on = ['filename','index_run_start','stopped_changes'],
            how = "left"
        )
        .assign(
            is_steady = lambda x, thresh = steady_accel_thresh: 
                # seems like the low percentile on steady could catch part of accel phase,
                 # may want to add more conditions here later
                (x.fps_next.ge(x.steady_fps)) & 
                (x.is_stopped.eq(False)) & 
                # new accel condition
                ((x.accel_next > -thresh) & (x.accel_next < thresh))
        )
    )
    
    # assign accel vs. decel based on where you're at between stopped and steady
    rawnav_seg_steady_lims = (
    	rawnav
        .query('(is_stopped == False) & (is_steady == True)')
    	.groupby(['filename','index_run_start','stopped_changes'])
    	.agg(
            steady_fps_sec_start = ('sec_past_st', 'min'),
            steady_fps_sec_end = ('sec_past_st', 'max')
        )
        .reset_index()
    )

    # rejoin the lims
    rawnav = (
        rawnav
        # .drop(['steady_fps_sec_start','steady_fps_sec_end'], axis = "columns")
        .merge(
            rawnav_seg_steady_lims,
            on = ['filename','index_run_start','stopped_changes'],
            how = "left"
        )
        .assign(
            accel_decel = lambda x: 
                np.select(
                    [
                        # these commented parts are overcoding due to earlier type=o
                    (x.sec_past_st < x.steady_fps_sec_start) & x.is_stopped.eq(False), #& x.basic_decomp.ne('steady'),
                    (x.sec_past_st > x.steady_fps_sec_end) & x.is_stopped.eq(False) #& x.basic_decomp.ne('steady')
                    ],
                    [
                     "accel",
                     "decel"
                    ],
                    default = np.nan
                    )
        )
    )
        
    # assign the decomp
    rawnav = (
        rawnav
        .assign(
            basic_decomp = lambda x: np.select(
                [
                x.is_steady.eq(True),
                x.accel_decel.ne("nan"), #TODO: i screwed something up
                x.is_stopped.eq(True),
                x.is_steady.eq(False) & x.is_stopped.eq(False) & x.accel_decel.eq("nan")
                ],
                [
                "steady",
                x.accel_decel,
                "stopped",
                "other_delay"
                ]
            )
        )
    )
    
    # once no longer debugging, drop these cols
    # rawnav = (
    #     rawnav
    #     .drop([
    #         "is_stopped",
    #         "stopped_changes",
    #         "steady_fps",
    #         "steady_fps",
    #         "steady_fps_sec_start",
    #         "steady_fps_sec_end",
    #         "accel_decel"
    #         ],
    #         axis = "columns"
    #     )
    # )
    
    return(rawnav)
        
def interp_odom(x, ft_threshold = 1, fix_interp = True):
    # ft_threshold is how far outside the bands of observed odom_ft values we would allow. a little
    # wiggle room probbaly okay given how we understand these integer issues appearing
    # 
    
    x.set_index(['sec_past_st'], inplace = True)
    
    if (x.index.duplicated().any()):
        raise ValueError("sec_past_st shouldn't be duplicated at this point")
    else:
        # interpolate
        # TODO: add options for different interpolation options
        x.odom_ft = x.odom_ft.interpolate(method = "index")
        
        # test
        # Could probably fix some of this, but oh well
        if (fix_interp == True):
            x = (
                x
                .assign(
                    odom_low = lambda x, ft = ft_threshold : (
                        (x.odom_ft < (x.odom_ft_min - ft))
                    ),
                    odom_hi = lambda x, ft = ft_threshold : (
                        (x.odom_ft > (x.odom_ft_max + ft))    
                    )
                )
                .assign(
                    odom_ft = lambda x, ft = ft_threshold: np.select(
                    [
                        #I think we avoid evaluating other conditions 
                        # if the first case is true
                        x.odom_low.eq(False) & x.odom_hi.eq(False),
                        x.odom_low,
                        x.odom_hi,
                    ],
                    [
                        x.odom_ft,
                        x.odom_ft_min - ft,
                        x.odom_ft_max + ft
                    ],
                    default = x.odom_ft # this is probably overkill
                    )    
                )
                
            )
        
        # this is a recalculation after fixes above
        x = (
            x
            .assign(
                odom_interp_fail = lambda x, ft = ft_threshold : (
                    (x.odom_ft < (x.odom_ft_min - ft)) |
                    (x.odom_ft > (x.odom_ft_max + ft))
                )
            )
        )
        
        # output
        x.reset_index(inplace = True)
        return(x)

def smooth_vals(rawnav):
    """
    Parameters
    ----------
    rawnav: pd.DataFrame, rawnav data. Expect cols sec_past_st and odom_ft
    groupvars: list of column names. 
    Returns
    -------
    rawnav_add: pd.DataFrame, rawnav data with additional fields.
    Notes
    -----
    Because wmatarawnav functions generally leave source rawnav data untouched except 
    just prior to the point of analysis, these calculations may be performed several times
    on smaller chunks of data. We group by stop_id in addition to run in case a segment has 
    multiple stops in it (e.g., Georgia & Irving)
    
    By default calculations are grouped by run, but in certain phases of data processing, it
    can be appropriate to group by run and stop.
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
                )
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
            index_run_end = ('index_run_end',"first"),
            route = ('route',"first"),
            wday = ('wday',"first"),
            start_date_time = ('start_date_time',"first"),
            # in this sense, we're starting to lose data and have to make judgment calls
            index_loc = ('index_loc','max'),
            lat = ('lat','last'),
            long = ('long','last'),
            heading = ('heading','last'),
            # i'm hoping it's never the case that door changes on the same second
            # if it does, will be in a world of pain.
            # this join works better when we expect every row to be filled
            veh_state = ('veh_state', lambda x: ','.join(x.unique().astype(str))),
            odom_ft_min = ('odom_ft','min'),
            odom_ft_max = ('odom_ft','max'),
            sat_cnt = ('sat_cnt','last'),
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
            blank = ('blank', lambda x: ','.join(x.unique().astype(int).astype(str))),
            lat_raw = ('lat_raw','last'),
            long_raw = ('long_raw','last'),
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
        .assign(
            odom_ft = lambda x: np.where(
                x.collapsed_rows.isna(),
                x.odom_ft,
                np.nan
            ),
            odom_interp_fail = lambda x: np.nan
        )
        .groupby(['filename','index_run_start'])
        .apply(lambda x: interp_odom(x))
        .reset_index(drop = True)
        .pipe(
            # Should have done this sooner, but definitely necessary after these aggregations
            ll.reorder_first_cols,
            ['filename',
             'index_run_start',
             'route_pattern',
             'pattern',
             'index_run_end',
             'route',
             'wday',
             'start_date_time',
             'index_loc',
             'odom_ft',
             'sec_past_st',
             'heading',
             'door_state',
             'veh_state',
             'row_before_apc',
             'lat',
             'long',
             'lat_raw',
             'long_raw',
             'sat_cnt',
             'collapsed_rows',
             'odom_ft_min',
             'odom_ft_max',
             'odom_interp_fail',
             'door_state_all',
             'stop_window_e',
             'stop_window_x',
             'row_before_apc'
             ]
        )            
    )

    # TODO: should probably break the second half of this function into a separate function
    # sometimes the above returns that .loc view/copy warning? i'm not sure
    #%% lag values
    rawnav[['odom_ft_next','sec_past_st_next']] = (
        rawnav
        .groupby(['filename','index_run_start'], sort = False)[['odom_ft','sec_past_st']]
        .transform(lambda x: x.shift(-1))
    )


    # We'll use a bigger lag for more stable values for free flow speed
    # later, we decided not to use these
    rawnav[['odom_ft_next3','sec_past_st_next3']] = (
        rawnav
        .groupby(['filename','index_run_start'], sort = False)[['odom_ft','sec_past_st']]
        .transform(lambda x: x.shift(-3))
    )
    
    #%% Calculate FPS
    rawnav_add = (
        rawnav
        .assign(
            secs_marg = lambda x: x.sec_past_st_next - x.sec_past_st,
            odom_ft_marg = lambda x: x.odom_ft_next - x.odom_ft,
            fps_next = lambda x: ((x.odom_ft_next - x.odom_ft) / 
                                (x.sec_past_st_next - x.sec_past_st)),
            fps_next3 = lambda x: ((x.odom_ft_next3 - x.odom_ft) / 
                                 (x.sec_past_st_next3 - x.sec_past_st))
        )
    )
        
    # if you get nan's, it's usually zero travel distance and zero time around 
    # doors. the exception is at the end of the trip.
    rawnav_add = (
        rawnav_add
        .assign(
            fps_next = lambda x: x.fps_next.replace([np.nan],0),
            fps_next3 = lambda x: x.fps_next3.replace([np.nan],0)
        )
        # we'll set np.Inf to np.nan so we can fill in the following step
        .assign(
            fps_next = lambda x: x.fps_next.replace([np.Inf],np.nan),
            fps_next3 = lambda x: x.fps_next3.replace([np.Inf],np.nan),
        )
    )
    
    # if you get infinite on speed, it's because your odometer increments but seconds
    # don't. as discussed above, rather than dropping these values or interpolating on 
    # sec_past_st, for now we'll just fill the previous speed value forward 
    rawnav_add[['fps_next','fps_next3']] = (
        rawnav_add
        .groupby(['filename','index_run_start'])[['fps_next','fps_next3']]
        .transform(lambda x: x.ffill())
    )
    
    # but now, if you're the last row or last three rows, we reset you back to 
    # np.nan
    rawnav_add.loc[rawnav_add.groupby(['filename','index_run_start']).tail(1).index, 'fps_next'] = np.nan
    rawnav_add.loc[rawnav_add.groupby(['filename','index_run_start']).tail(3).index, 'fps_next3'] = np.nan
    
    #%% Calculate acceleration
    rawnav_add[['fps_next_lag']] = (
        rawnav_add
        .groupby(['filename','index_run_start'], sort = False)[['fps_next']]
        .transform(lambda x: x.shift(1))
    )

    # Note, not replicating the 3rd ping lag, as it's a little dicey i htink
    rawnav_add = (
        rawnav_add 
        .assign(
            # this may seem a bit screwy, but it lines up well with intuitions when visualized
            # can share some notebooks (99-movement-explore-*.Rmd) 
            # that illustrate differences between approaches here if desired
            # accel_next is also a bit of a misnomer; more like accel_at_point, 
            # because some downstream/notebook code depends on accel_next, sticking to that
            # nomenclature for now
            accel_next = lambda x: (x.fps_next - x.fps_next_lag) / (x.sec_past_st_next - x.sec_past_st),
        )
        # as before, we'll set these cases to nan and then fill
         .assign(
            accel_next = lambda x: x.accel_next.replace([np.Inf,-np.Inf],np.nan),
        )
    )
    
    # this is the point where I should've written another function to do these things
    rawnav_add[['accel_next']] = (
        rawnav_add
        .groupby(['filename','index_run_start'])[['accel_next']]
        .transform(lambda x: x.ffill())
    )
    
    # but now, if you're the last row, we reset you back to np.nan
    rawnav_add.loc[rawnav_add.groupby(['filename','index_run_start']).tail(1).index, 'accel_next'] = np.nan
    
    #%% Calculate Jerk
    # TODO: Look into derivative of acceleration (jerk)? might address some smoothing issues
    rawnav_add[['accel_next_lag']] = (
        rawnav_add
        .groupby(['filename','index_run_start'], sort = False)[['accel_next']]
        .transform(lambda x: x.shift(1))
    )
    
    rawnav_add = (
        rawnav_add 
        .assign(
            # this may seem a bit screwy, but it lines up well with intuitions when visualized
            # can share some notebooks (99-movement-explore-*.Rmd) 
            # that illustrate differences between approaches here if desired
            # accel_next is also a bit of a misnomer; more like accel_at_point, 
            # because some downstream/notebook code depends on accel_next, sticking to that
            # nomenclature for now
            jerk_next = lambda x: (x.accel_next - x.accel_next_lag) / (x.sec_past_st_next - x.sec_past_st),
        )
        # as before, we'll set these cases to nan and then fill
         .assign(
            jerk_next = lambda x: x.jerk_next.replace([np.Inf,-np.Inf],np.nan),
        )
    )
    
    rawnav_add[['jerk_next']] = (
        rawnav_add
        .groupby(['filename','index_run_start'])[['jerk_next']]
        .transform(lambda x: x.ffill())
    )
    
    # but now, if you're the last row, we reset you back to np.nan
    rawnav_add.loc[rawnav_add.groupby(['filename','index_run_start']).tail(1).index, 'jerk_next'] = np.nan
        
    #%% Cleanup
    # drop some leftover cols
    
    rawnav_add = (
        rawnav_add
        .drop([
            'odom_ft_next',
            'sec_past_st_next',
            'odom_ft_next3',
            'sec_past_st_next3',
            'accel_next_lag'
            ],
            axis = "columns")
        )
    
    return(rawnav_add)
    