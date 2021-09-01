# -*- coding: utf-8 -*-
"""
Created on Mon June 7 03:48 2021

@author: WylieTimmerman
"""
import pandas as pd
import numpy as np
from . import low_level_fns as ll
from . import decompose_rawnav as dr
from math import factorial
import warnings

# we'll use this to just identify what the heck the bus is doing at any particular
# point in time.

#### Stop Alignment functions

def reset_odom(
    rawnav,
    indicator_var = "stop_id_loc",
    indicator_val = None,
    reset_vars = ['odom_ft','sec_past_st']
    ):
    
    rawnav = rawnav.set_index('index_loc')
    
    if indicator_val == None:
        reset_idx = rawnav[indicator_var].first_valid_index() 
    else:
        reset_idx = rawnav.loc[rawnav[indicator_var] == indicator_val].first_valid_index()
        
    if reset_idx == None:
        # TODO: oddly, there are a lot of pings that should've been matched to 
        # a nearby stop but weren't. For now, i'm just going to skip these,
        # but we need to look at the processing code more.
        print("ditching: " + rawnav.filename.iloc[1] + "_" + str(rawnav.index_run_start.iloc[1]))
        return(None)
    
    for var in reset_vars:
        # keep a copy of original...
        rawnav[(var + "_og")] = rawnav[var]
        # ...but overwrite the original for convenience
        rawnav[(var)] = rawnav[var] - rawnav.loc[reset_idx,var]
    
    rawnav = rawnav.reset_index()
    
    return(rawnav)

def match_stops(
    rawnav,
    stop_index
    ):
    
    # TODO: need to consider cases where it's not the first stop that matches
    # maybe there's a default odometer reading at each stop in a pattern, and you 
    # start at that if you didn't match sooner.
    
    #### Find the nearest point to any stop
    # NOTE: somehow all of the trips we tested on didn't have stops here, so we 
    # address later.
    # This adds a column 'stop_id_loc'
    # TODO: somehow this isn't joining quite right on index_loc, and i'm not
    # sure why. Will need to revist processing code, but for now, may not rely on
    # stop_id_loc much
    rawnav = (
        rawnav
        .merge(
            stop_index
            .filter(items = ['filename','index_run_start','index_loc','stop_id'])
            .rename(columns = {'stop_id' : 'stop_id_loc'}),
            left_on = ['filename','index_run_start','index_loc'],
            right_on = ['filename','index_run_start','index_loc'],
            how = "left"
        )
    )
    
    # TODO: in enterprise land, we should make sure this isn't possible
    # if a trip has no matched stops, we drop the trip at this point, i guess.
    rawnav = (
        rawnav
        .groupby(['filename','index_run_start'])
        .filter(
            lambda x: any(x.stop_id_loc.notna())    
        )    
    )
    
    #### Associate stop_ids to groups
    # Our overall goal is to build a crosswalk between filename, index_run_start, 
    #stopped_changes_collapsed and the stop_id 
    # that we'll rejoin to the main table.
    
    # Some of this is a little overdone because there's a 
    # possibility that stopped_changes_collapsed falls close-ish to two different stop points,
    # and given the wide variety of cases we'll find, i'm trying to be more careful. That's 
    # where this kind of iteration falls in.
    
    # Setup Iteration
    stop_index_forgrp = (
        stop_index
        .rename({'stop_id': 'stop_id_group'}, axis = 'columns')
        # we need index_loc at the end, but we will drop it from some interim joins
        .filter(['filename','index_run_start','stop_id_group','odom_ft_stop','index_loc'])
        .sort_values(['odom_ft_stop'])
    )
    
    rawnav_fil = rawnav.copy()
    
    query_list = [
        'stop_decomp == "doors_on_O_S"',
        'door_case == "doors"',
        'door_case == "nodoors"'
    ]
    
    # create a blank dictionary for DFs we'll concatenate later
    stopid_stopped_changes_xwalk_dict = {}  
    # Run Iteration
    for thequery in query_list:
        rawnav_stopareas = (
            rawnav_fil.query(thequery)
        )

        rawnav_stopareas_idd = (
            rawnav_stopareas
            .pipe(
                pd.merge_asof,
                    right = stop_index_forgrp.drop(columns = ['index_loc']),
                    by = ['filename','index_run_start'],
                    left_on = ['odom_ft'],
                    right_on = ['odom_ft_stop'],
                    direction = "nearest"
            )
        )
    
        # chuck cases where the nearest stop is not within 200 ft. this is arbitrary
        # if there are multiple possible matches, pick the one that has the minimum distance
        # and if there are still multiple matches, pick the first.
        
        rawnav_stopareas_idd_fil = (
            rawnav_stopareas_idd
            .assign(odom_ft_stop_diff = lambda x: abs(x.odom_ft_stop - x.odom_ft))
            .groupby(['filename','index_run_start','stopped_changes_collapse'])
            .apply(lambda x: x.loc[x.odom_ft_stop_diff.le(200) & (x.odom_ft_stop_diff == min(x.odom_ft_stop_diff))])
            .drop_duplicates(
                subset = ['filename','index_run_start','stopped_changes_collapse','odom_ft'],
                keep = 'first'
            )
            .reset_index(drop = True)
            # if you happened to have two sets of stopped_changes_collapse join to same
            # stop_id, now you need to pick one.
            # this happened on rawnav04475210210.txt-4279 at 32089
            .sort_values(['filename','index_run_start','stop_id_group','odom_ft_stop_diff'])
            .drop_duplicates(
                subset = ['filename','index_run_start','stop_id_group'],
                keep = 'first'
            )
            .filter(
                ['filename','index_run_start','stopped_changes_collapse','stop_id_group'],
                axis = "columns"
            )
        )
        # append to the dictionary based on the query name
        stopid_stopped_changes_xwalk_dict[thequery] = rawnav_stopareas_idd_fil

        # remove items we've already matched
        stop_index_forgrp = (
            stop_index_forgrp
            .pipe(
                ll.anti_join,
                rawnav_stopareas_idd_fil,
                on = ['filename','index_run_start','stop_id_group']
            )
        )
        
        rawnav_fil = (
            rawnav_fil
            .pipe(
                ll.anti_join,
                rawnav_stopareas_idd_fil,
                on = ['filename','index_run_start','stopped_changes_collapse']
            )
        )
    
    # Combine results
    stopid_stopped_changes_xwalk = pd.concat(stopid_stopped_changes_xwalk_dict.values())    
    # need to de-duplicate one more time i think...
    
    # Join
    rawnav = (
        rawnav
        .merge(
            stopid_stopped_changes_xwalk,
            on = ['filename','index_run_start','stopped_changes_collapse'],
            how = "left",
            suffixes = ('','_xwalk')
        )
        # this seems duplicative, but we're only joining on the ones that
        # didn't join to a stop in order to indicate passups
        .merge(
            stop_index_forgrp
            .filter(items = ['filename','index_run_start','index_loc','stop_id_group']),
            left_on = ['filename','index_run_start','index_loc'],
            right_on = ['filename','index_run_start','index_loc'],
            how = "left",
            suffixes = ('','_pu')
        )
        .assign(
            stop_id_group = lambda x: 
                np.where(
                   x.door_case.isna(),
                   x.stop_id_group_pu,
                   x.stop_id_group
                ),
            stop_case = lambda x: np.select(
                [
                    x.stop_id_group.notna() & x.door_case.eq('doors'),
                    x.stop_id_group.isna() & x.door_case.eq('nodoors'),
                    x.stop_id_group.notna() & x.door_case.isna()
                ],
                [
                    'atstop',
                    'notstop',
                    'passstop'
                ],
                default = pd.NA
            )    
        )
        .assign(
            stop_decomp_ext = lambda x:
                np.where(
                    x.stop_case.isin(['atstop','notstop']),
                    x.stop_case + "_" + x.stop_decomp,
                    x.stop_case
                )
        )
        .drop(columns = ['stop_id_group_pu'])
        .sort_values(['filename','index_run_start','index_loc'])
    )
    
    
    # TODO: add tests or checks that if we matched 15 stops to a file in stop_index,
    # then we have 15 unique stop groups that got associated in some way. shoudl be guaranteed
    # by our methods but should check.
    # TODO: check that no two sets of stopped pings in our data were joined to the same 
    # stop_id. That would be weird and bad. should be prevented by our methods.
    # TODO: if it's even possible for a stopped_changes_collapse thing to come close enough to
    # two pings, we should look into that. Our methods are probably addressing this but should 
    # check.
        
    return(rawnav)

def create_stop_segs(
        rawnav
    ):
    #### Create Stop Segments
    rawnav['stop_id_forw'] = (
        rawnav
        .groupby(['filename','index_run_start'])['stop_id_group']
        .transform(lambda x: x.ffill())
        .astype('Int64')
        .astype('string')
        .fillna(value = "tripstart")
    )
    
    rawnav['stop_id_back'] = (
        rawnav
        .groupby(['filename','index_run_start'])['stop_id_group']
        .transform(lambda x: x.bfill())
        .astype('Int64')
        .astype('string')
        .fillna(value = "tripend")
    )
    
    rawnav = (
        rawnav
        .assign(
            stop_seg = lambda x:
                np.where(
                    x.stop_id_group.notna(),
                    pd.NA,
                    (x.stop_id_forw.astype(str) + "_" + x.stop_id_back.astype(str))
                )
        )
        .assign(
            trip_seg = lambda x:
                np.where(
                    x.stop_id_group.notna(),
                    x.stop_id_group.astype('Int64').astype(str),
                    x.stop_seg
                )
        )
        .drop(['stop_id_forw','stop_id_back'], axis = "columns")
    )
        
    #### Associate accel/decel with stop changes
    
    rawnav_stopped_changes_grps = (
        rawnav
        .groupby(['filename','index_run_start','stopped_changes_collapse'])
        .agg(
            # for now, just associating this to accel/decel
            door_case = ('door_case','first')
        )
        .reset_index()
    )
        
    rawnav = (
        rawnav
        .assign(
            stopped_changes_collapse_prev = lambda x:
                x.stopped_changes_collapse - 1,
            stopped_changes_collapse_next = lambda x:
                x.stopped_changes_collapse + 1
        )
    )

    rawnav = (
        rawnav
        .merge(
            rawnav_stopped_changes_grps
            .rename(columns = {'stopped_changes_collapse' : 'stopped_changes_collapse_prev'}),
            on = ['filename','index_run_start','stopped_changes_collapse_prev'],
            how = "left",
            suffixes = ('','_prev')            
        )
        .merge(
            rawnav_stopped_changes_grps
            .rename(columns = {'stopped_changes_collapse' : 'stopped_changes_collapse_next'}),
            on = ['filename','index_run_start','stopped_changes_collapse_next'],
            how = "left",
            suffixes = ('','_next')            
        )
        # these fills aren't strictly speaking accurate, but we want to cover the 
        # case where the look ahead and behind is not NA for first and last case
        .assign(
            door_case_prev = lambda x: x.door_case_prev.fillna(value = 'nodoors'),
            door_case_next = lambda x: x.door_case_next.fillna(value = 'nodoors')
        )
    )
    
    rawnav = (
        rawnav
        .assign(
            basic_decomp_ext = lambda x: np.select(
                [
                    x.basic_decomp.eq('accel'),
                    x.basic_decomp.eq('decel'),
                    x.basic_decomp.eq('stopped')
                ],
                [
                    x.basic_decomp + "_" + x.door_case_prev,
                    x.basic_decomp + "_" + x.door_case_next,
                    x.basic_decomp + "_" + x.door_case
                ],
                default = x.basic_decomp
            )
        )
        .drop(
            ['stop_seg',
             'stopped_changes_collapse_next',
             'stopped_changes_collapse_prev',
             'door_case_prev',
             'door_case_next'],
            axis = 'columns'
        )
    )
    
    return(rawnav)

#### Decomposition
def decompose_mov(
    rawnav,
    slow_fps = 14.67, # upped default to 10mph
    steady_accel_thresh = 2,
    stopped_fps = 3): 
    # our goal here is to get to accel/decel/steady state 
    
    #### Categorize stopped movement
    # JACK: you may want to steal next few chunks
    # for now, not distinguishing slow
    rawnav = (
        rawnav
        .assign(
            # need a True/False that's easily coercible to numeric
            # TODO: consider overriding is_stopped if the door opens. These are probably cases where the
            # vehicle did stop for a moment 
            # or at the very least, test that we don't have these cases in the decomposition.
            is_stopped = lambda x, stop = stopped_fps: (x.fps_next.le(stop) | x.door_state.eq("O"))
        )
        .assign(
            veh_state_calc = lambda x: 
                np.where(
                    x.is_stopped.eq(True),
                    "S",
                    "M"
                )
        )
    )
    

    #### Categorize stop groups
    rawnav['stopped_changes'] = (
    	rawnav
    	.groupby(['filename','index_run_start'])['is_stopped']
    	.transform(lambda x: x.diff().ne(0).cumsum())
    )
    
    #### Calc rolling vals only within stop segments
   
    rawnav = (
        rawnav
        .assign(
            is_steady = lambda x, thresh = steady_accel_thresh, slow = slow_fps: 
                (x.fps3.ge(slow)) & 
                (x.is_stopped.eq(False)) & 
                ((x.accel9 > -thresh) & (x.accel9 < thresh))
        )
    )
    
   
    #### Calculate where steady state begins/ends, accel and decel phases
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

    #### Identify Accel, Decel, and other delays
    # rejoin the lims
    # anything else becomes accel, decel, or other
    rawnav = (
        rawnav
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
                    (x.sec_past_st > x.steady_fps_sec_end) & x.is_stopped.eq(False)
                    ],
                    [
                     "accel",
                     "decel"
                    ],
                    default = pd.NA
                )
        )
    )
    
    #### Assign the basic decomp
    # Note that we will later override some of this based on consecutive portions of other_delay 
    rawnav = (
        rawnav
        .assign(
            basic_decomp = lambda x: np.select(
                [
                x.is_stopped.eq(True),
                x.is_steady.eq(True),
                x.accel_decel.notna(),
                x.is_steady.eq(False) & x.is_stopped.eq(False) & x.accel_decel.isna()
                ],
                [
                "stopped",
                "steady",
                x.accel_decel,
                "other_delay"
                ],
                default = "error"
            )
        )
    )

    #### Combine stop areas 
    # When we see short bits of movement around the stop (such as traffic delaying entry to
    # stop area or creeping from a near-side stop to the signal) we want to combine those into
    # one chunk. We first look for the earliest point where the bus stopped in a chunk of 
    # stopped time.   
    rawnav_stopped_lims = (
        rawnav
        .loc[rawnav.is_stopped]
        .groupby(['filename','index_run_start','stopped_changes'], sort = False)
        .agg(
            min_odom = ('odom_ft','min')
        )
    )

    # find out when you're near a stop
    rawnav = (
        rawnav
        .sort_values(by = ["odom_ft",'index_loc'])
        .pipe(
            pd.merge_asof,
            right = rawnav_stopped_lims.sort_values(['min_odom']),
            by = ['filename','index_run_start'],
            left_on = 'odom_ft',
            right_on = 'min_odom',
            direction = 'forward'
        )
        .assign(
            # TODO: we should probably handle this in a more spaitally sensitive fashion,
            # but will probably need to use threhsolds like this in any case. The idea here
            # is that we want to collapse stop activity wihtin 150 feet to one case on the notion
            # that hte bus is probably just pulling forward from bus stop to the intersection 
            # stop bar/crosswalk
            near_stop = lambda x: x.min_odom - x.odom_ft <= 150
        )
    )

    # we only want to collapse cases if the points within a stopped_changes group are 
    # all in this murky other delay category and they're all near a stop (i.e. those 
    # purple dots between orange or red dots)
    # TODO: also cases where it's all stopped might need to apply, 
    # see rawnav04475210210.txt 4279 at start of trip
    rawnav['all_other_delay'] = (
        rawnav
        .groupby(['filename','index_run_start','stopped_changes'], sort = False)['basic_decomp']
        .transform(
            lambda x: all(x == 'other_delay')
        )   
    )
       
    rawnav['all_near_stop'] = (
        rawnav
        .groupby(['filename','index_run_start','stopped_changes'], sort = False)['near_stop']
        .transform(
            lambda x: all(x.eq(True))
        )   
    )
    
    rawnav = rawnav.assign(reset_group = lambda x: (x.all_other_delay & x.all_near_stop) | x.is_stopped)
        
    # Reset to be based on changes in those values
    rawnav['stopped_changes_collapse'] = (
    	rawnav
    	.groupby(['filename','index_run_start'])['reset_group']
    	.transform(lambda x: x.diff().ne(0).cumsum())
    )

    #### Identify if stopped for pax
    rawnav = (
        rawnav
        .assign(
            door_state_closed = lambda x: (x.door_state == "C") 
        )
    )
        
    rawnav['any_door_open'] = (
        rawnav
        .groupby(['filename','index_run_start','stopped_changes_collapse'])['door_state_closed']
        .transform(lambda x: any(~x))       
    )
    
    rawnav['any_veh_stopped'] = (
        rawnav
        .groupby(['filename','index_run_start','stopped_changes_collapse'])['is_stopped']
        .transform(lambda x: any(x))  
    )

    ####  distinguish dwell types
    # we want to see where the first door open event happens, since we'll think of that as the
    # 'real' door open case. Anything after that kind of becomes delay (e.g., operatior
    # is stuck at light and reopens door because someone comes running up)
    rawnav['door_changes'] = (
        rawnav
        .groupby(['filename','index_run_start','stopped_changes_collapse'])['door_state_closed']
        .transform(
            lambda x: x.diff().ne(0).cumsum()
        )
    )
        
    # next, we find where the min and max observation of a door change is, similar to what 
    # we'll do for steady state in a bit. In R we could just stick to grouped operations,
    # but here it's easier to summarize and then rejoin.
    # Unfortunately, we can't just assume that based on the index of door_changes that you're
    # open or closed, as this bit us during QJ study. Weird things can happen on the margin 
    # of intervals, basically, so this is safer.
    door_open_lims = (
        rawnav
        .loc[
            rawnav.any_veh_stopped & ~rawnav.door_state_closed
        ]
        .groupby(['filename','index_run_start','stopped_changes_collapse'])
        .agg(
            door_open_min = ('door_changes','min'),
            door_open_max = ('door_changes','max')
        )
    )
    
    #### Add stop-area door state items
    # TODO: maybe i should use the APC tags here too? Maybe we should go from first to last 
    # APC tag?
    # TODO: maybe consider adding a bus stop identifier before this point
    rawnav = (
        rawnav       
        .merge(
            door_open_lims,
            on = ['filename','index_run_start','stopped_changes_collapse'],
            how = 'left'
        )
        .assign(
            relative_to_firstdoor = lambda x: np.select(
                [
                    x.door_state.eq('C') & x.door_changes.le(x.door_open_min),
                    x.door_state.eq('O') & x.door_changes.eq(x.door_open_min),
                    x.door_changes.gt(x.door_open_min),
                    x.any_door_open.eq(False) & x.any_veh_stopped.eq(True) 
                ],
                [
                    "pre",
                    "at",
                    "post",
                    # we use a text NA here to distinguish from cases where 
                    # vehicle is not at a stop, period
                    "na" 
                ],
                default = pd.NA          
            )    
        )
        .assign(
            # cle
            door_changes = lambda x: 
                np.where(
                    x.any_veh_stopped.eq(False),
                    pd.NA,
                    x.door_changes
                )
        )
    )
        
    rawnav = (
        rawnav
        .assign(
            basic_decomp = lambda x:
                np.where(
                    x.any_veh_stopped.eq(True),
                    "stopped",
                    x.basic_decomp
                )
        )
        .assign(
            door_case = lambda x: 
                np.select(
                    [
                    x.basic_decomp.eq('stopped') & x.any_door_open.eq(True),
                    x.basic_decomp.eq('stopped') & x.any_door_open.eq(False)
                    ],
                    [
                    "doors",
                    "nodoors"
                    ],
                    default = pd.NA
                ),
            # Not strictly needed but i wanted this var
            pax_activity = lambda x:
                np.select(
                    [
                        x.door_state.eq("O") & x.row_before_apc.eq(1),
                        x.door_state.eq("O") & x.row_before_apc.eq(0)
                    ],
                    [
                        "pax",
                        "nopax"
                    ],
                    default = pd.NA
                )
        )
        .assign(
            stop_decomp = lambda x: 
                np.where(
                    x.basic_decomp.eq('stopped'),
                    x.door_case + "_" + x.relative_to_firstdoor + "_" + x.door_state + "_" + x.veh_state_calc,
                    pd.NA                    
                )
        )
    )

    ####  cleanup
    # TODO:  once no longer debugging, drop these cols or whatever else that were problematic
    # TODO: should we drop veh_state? it's not really correct, we don't use it
    rawnav = (
        rawnav
        .drop([
            "is_stopped",
            "is_steady",
            "stopped_changes",
            "steady_fps_sec_start",
            "steady_fps_sec_end",
            "accel_decel",
            "min_odom",
            "near_stop",
            "all_other_delay",
            "all_near_stop",
            "reset_group",
            "door_state_closed",
            "door_open_min",
            "door_open_max"
            ],
            axis = "columns"
        )
    )
    
    # to clarify, the key stop 
    
    return(rawnav)
        
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
    rawnav['row_number'] = (
        rawnav
        .groupby(['filename','index_run_start'], sort = False)
        .cumcount()
    )
    
    rawnav = (
        rawnav
        .assign(
            
        )
        .assign(
            odom_ft = lambda x: 
                np.where(
                    x.collapsed_rows.eq(1) | (x.collapsed_rows.gt(1) & x.row_number.eq(0)),
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
            'odom_low',
            'odom_hi',
            'secs_next',
            'secs_last',
            'odom_interp_fail',
            'row_number'
            ],
            axis = "columns"
        )
    )
    
    return(rawnav)


#### Speed Calcs

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

#### Smoothing Functions
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

def expand_rawnav(rawnav_ti):
    
    rawnav_ti_expand = (
        pd.DataFrame(
            {'sec_past_st': np.arange(rawnav_ti.sec_past_st.min(), rawnav_ti.sec_past_st.max(),1 )} 
        )
        .merge(
             rawnav_ti[['sec_past_st','fps_next']],
             on = 'sec_past_st',
             how = 'left'
        )
        .assign(
            fps_next = lambda x: x.fps_next.ffill(),
        )
    )
    
    return(rawnav_ti_expand)

# TODO: consider an approach that will let us smooth other things too
# for each trip instance, this expands the data, applies the savitzy golay method, and 
# recalculates accel and jerk
def apply_smooth(rawnav_ti):
    rawnav_ex = expand_rawnav(rawnav_ti)
    
    rawnav_ex['fps_next_sm'] = savitzky_golay(rawnav_ex.fps_next.to_numpy(), 21, 3)    
    
    rawnav_ex = (
        rawnav_ex
        .assign(
            fps_next_sm = lambda x: np.where(
                x.fps_next_sm.le(0),
                0,
                x.fps_next_sm
            )    
        )    
    )
    
    rawnav_ex = (
        rawnav_ex
        # a little hack to shortcut the need to group, since we're doing all this by trip 
        # instance
        .assign(grp = 1)
        .pipe(
            calc_accel_jerk,
            groupvars = ['grp'], 
            fps_col = 'fps_next_sm'
        )
        .drop(['grp'], axis = 'columns')
    )
        
    rawnav_ti = pd.merge(
        rawnav_ti
        # drop the placeholder columns
        .drop(
            ['fps_next_sm',
             'accel_next',
             'jerk_next'
             ],
            axis = 'columns'
        ),
        rawnav_ex
        .drop(
            ['fps_next'],
            axis = "columns"
        ),
        on = ['sec_past_st'],
        how = 'left'
    )

    return(rawnav_ti)

# this is a parent function that handles some placeholder column creation, gropuing, and applying
# the smoothing function
def smooth_speed(rawnav):
    
    # this requires interpolated data, according to internet
    # https://gis.stackexchange.com/questions/173721/reconstructing-modis-time-series-applying-savitzky-golay-filter-with-python-nump
    
    rawnav = (
        rawnav
        .assign(
            # i'm not sure if these placeholders are necessary or not
            fps_next_sm = np.nan,
            accel_next = np.nan,
            jerk_next = np.nan
        )
        .groupby(['filename','index_run_start'],sort = False)
        .apply(
            lambda x: apply_smooth(x)
        )
        # i'm not sure how the index changes, but oh well
        .reset_index(drop = True)
    )
    
    return(rawnav)
    

def calc_rolling(
        rawnav,
        groupvars = ['filename','index_run_start']
    ):
    rawnav = (
        rawnav
        .assign(timest = lambda x: pd.to_datetime(x.start_date_time)+ pd.to_timedelta(x.sec_past_st, unit = "s"))
        .set_index('timest')
    )
    
    # this works
    rawnav[['fps3','accel3','jerk3']] = (
        rawnav
        .groupby(groupvars,sort = False)[['fps_next_sm','accel_next','jerk_next']]
        .transform(
            lambda x:
                x
                .sort_index() # not sure how this got unsorted, but...
                .rolling(
                    window = '3s', 
                    min_periods = 1, 
                    center = True, 
                    win_type = None
                )
                .mean()
        )
    )
        
    rawnav[['accel9']] = (
        rawnav
        .groupby(groupvars,sort = False)[['accel_next']]
        .transform(
            lambda x:
                x
                .sort_index()
                .rolling(
                    window = '9s', 
                    min_periods = 1, 
                    center = True, 
                    win_type = None
                )
                .mean()
        )
    )
        
    rawnav.reset_index(inplace = True, drop = True)
    
    return(rawnav)