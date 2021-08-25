# -*- coding: utf-8 -*-
"""
Created on Tue Aug 24 23:29:24 2021

@author: JackMcDowell
"""


# Entirely copied from Wylie's movement decomposition, replacing odometer 
# calculations with headings

#### interpolation functions
# despite the name, this is called by interp_over_sec
def interp_odom(x, ft_threshold = 1, fix_interp = True, interp_method = "index"):
    # ft_threshold is how far outside the bands of observed odom_ft values we would allow. a little
    # wiggle room probbaly okay given how we understand these integer issues appearing
    
    x.set_index(['sec_past_st'], inplace = True)
    
    if (x.index.duplicated().any()):
        raise ValueError("sec_past_st shouldn't be duplicated at this point")
    else:
        # interpolate
        x.odom_ft = x.odom_ft.interpolate(method = interp_method)
        
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
            # we'll impute this later, but for now, we just fill
            odom_ft = ('odom_ft','last'),
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
             'door_state_all',
             'stop_window_e',
             'stop_window_x',
             'row_before_apc'
             ]
        )            
    )
    
    return(rawnav)

def interp_over_sec(rawnav, interp_method = "index"):
    
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
    
    rawnav = (
        rawnav
        .assign(
            secs_next = lambda x: x.sec_past_st_next - x.sec_past_st,
            secs_last = lambda x: x.sec_past_st - x.sec_past_st_lag
        )
        .assign(
            odom_ft = lambda x: np.where(
                x.secs_next.eq(2) & x.secs_last.eq(1),
                np.nan,
                x.odom_ft
            )    
        )
    )
    
     # where we collapsed, let's NA these out for interpolate
     # if the repeated values are the same, we should probably not NA these out
    rawnav = (
        rawnav
        .assign(
            odom_ft = lambda x: np.where(
                x.collapsed_rows.eq(1),
                x.odom_ft,
                np.nan
            ),
            odom_interp_fail = lambda x: np.nan
        )
    )
        
    #### interpolate the odom values 
    rawnav = (
        rawnav
        .groupby(['filename','index_run_start'])
        # TODO: we should also probably interpolate heading
        .apply(lambda x: interp_odom(x, interp_method = interp_method))
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
            'odom_interp_fail'
            ],
            axis = "columns"
        )
    )
    
    return(rawnav)

def calc_speed(rawnav):
    
    #### lag values
    rawnav[['odom_ft_next','sec_past_st_next']] = (
        rawnav
        .groupby(['filename','index_run_start'], sort = False)[['odom_ft','sec_past_st']]
        .transform(lambda x: x.shift(-1))
    )
    
    #### calculate FPS
    rawnav = (
        rawnav
        .assign(
            secs_marg = lambda x: x.sec_past_st_next - x.sec_past_st,
            odom_ft_marg = lambda x: x.odom_ft_next - x.odom_ft,
            fps_next = lambda x: ((x.odom_ft_next - x.odom_ft) / 
                                (x.sec_past_st_next - x.sec_past_st))
        )
        # if you get nan's, it's usually zero travel distance and zero time around 
        # doors. the exception is at the end of the trip.
        .assign(
            fps_next = lambda x: x.fps_next.replace([np.nan],0)
        )
    )
        
    # if you're the last row , we reset you back to np.nan
    rawnav.loc[rawnav.groupby(['filename','index_run_start']).tail(1).index, 'fps_next'] = np.nan

    return(rawnav)
    
def calc_accel_jerk(rawnav, groupvars = ['filename','index_run_start'], fps_col = 'fps_next'):
    # a little inefficient to recalculate this, but we're tryign to call this within the exapnded
    # data as well.
    rawnav['sec_past_st_next'] = (
        rawnav
        .groupby(groupvars, sort = False)['sec_past_st']
        .shift(-1)
    )
    
    fps_lag_col = fps_col + "_lag"
    
    #### Calculate acceleration
    rawnav[[fps_lag_col]] = (
        rawnav
        .groupby(groupvars, sort = False)[[fps_col]]
        .transform(lambda x: x.shift(1))
    )

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
            accel_next = lambda x: (x[fps_col]- x[fps_lag_col]) / (x.sec_past_st_next - x.sec_past_st),
        )
        # as before, we'll set these cases to nan and then fill
         .assign(
            accel_next = lambda x: x.accel_next.replace([np.Inf,-np.Inf],np.nan),
        )
    )
    
    # but now, if you're the last row, we reset you back to np.nan
    rawnav.loc[rawnav.groupby(groupvars).tail(1).index, 'accel_next'] = np.nan
    
    #### Calculate Jerk
    rawnav[['accel_next_lag']] = (
        rawnav
        .groupby(groupvars, sort = False)[['accel_next']]
        .transform(lambda x: x.shift(1))
    )
    
    rawnav = (
        rawnav 
        .assign(
            jerk_next = lambda x: (x.accel_next - x.accel_next_lag) / (x.sec_past_st_next - x.sec_past_st),
        )
        # as before, we'll set these cases to nan and then fill
         .assign(
            jerk_next = lambda x: x.jerk_next.replace([np.Inf,-np.Inf],np.nan),
        )
    )
    
    # but now, if you're the last row, we reset you back to np.nan
    rawnav.loc[rawnav.groupby(groupvars).tail(1).index, 'jerk_next'] = np.nan
         
    #### Cleanup
    # drop some leftover cols
    rawnav = (
        rawnav
        .drop([
            fps_lag_col,
            'sec_past_st_next',
            'accel_next_lag'
            ],
            axis = "columns"
        )
    )
    
    return(rawnav)