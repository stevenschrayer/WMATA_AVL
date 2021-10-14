# -*- coding: utf-8 -*-
"""
Created on Mon June 7 03:48 2021

@author: WylieTimmerman
"""
import pandas as pd
import numpy as np
from . import low_level_fns as ll
from math import factorial

# we'll use this to just identify what the heck the bus is doing at any particular
# point in time.

#### Stop Alignment functions

def reset_odom(
    rawnav,
    indicator_var = "stop_id_loc",
    indicator_val = None,
    reset_vars = ['odom_ft','sec_past_st']
    ):
    """
    Reset odometer values.
    
    Reset values to zero based on criteria. Typically, we look for, say, the third stop in every
    pattern and reset the odometer based on that point, since at that point most trip instances
    are past the first or second stop where there can be weirdness with whether the trip instance
    has started.
    
    TODO: This function is likely to become moot once more map matching is incorporating into our
    code, as that approach will be a more reliable means of resetting odometers. For now, we aren't
    using it because we didn't have enough time to debug fully.
    
    Parameters
    ----------
    rawnav : pd.DataFrame
        Rawnav dataframe
    indicator_var : str, optional
        The variable/column name to use to search for indicator_val below. 
        The default is "stop_id_loc".
    indicator_val : str or int, optional
        The value of indicator_var to look for such that at this value, the odom_ft
        and sec_past_st values are reset to 0. The default is None.
    reset_vars : list, optional
        List of variables to reset to 0. The default is ['odom_ft','sec_past_st'].

    Returns
    -------
    rawnav: pd.DataFrame
        rawnav dataframe with odom_ft and sec_past_st values reset, with original values now
        stored in odom_ft_og and sec_past_st_og. The new odom_ft and sec_past_st values may be 
        negative in places before the reset value.

    """
    rawnav = rawnav.set_index('index_loc')
    
    if indicator_val == None:
        reset_idx = rawnav[indicator_var].first_valid_index() 
    else:
        reset_idx = rawnav.loc[rawnav[indicator_var] == indicator_val].first_valid_index()
        
    if reset_idx == None:
        # TODO: oddly, there are a lot of pings that should've been matched to 
        # a nearby stop but weren't. For now, i'm just going to skip these,
        # but we need to look at the processing code more.
        print(
            "no matching indicator var/val, ditching: " + 
            rawnav.filename.iloc[0] + 
            "_" + 
            str(rawnav.index_run_start.iloc[0])
        )
        return(None)
    
    for var in reset_vars:
        # keep a copy of original...
        rawnav[(var + "_og")] = rawnav[var]
        # ...but overwrite the original for convenience
        try:
            rawnav[(var)] = rawnav[var] - rawnav.loc[reset_idx,var]
        except:
            breakpoint()
    
    rawnav = rawnav.reset_index()
    
    return(rawnav)

def match_stops(
    rawnav,
    stop_index
    ):
    """
    Match known stop locations to rawnav data.
    
    Because buses may stop at slightly different locations than known stop locations, we use 
    some logic to associate clusters of stop-related activity (door open, door close time, that 
    fall together, etc.) with nearby stop_ids. There are a few rounds of iteration because if 
    some stop activity somehow falls close to multiple known stop locations (or multiple clusters
    of stop activity lie near the same stop), we want to handle these
    cases in a thoughtful manner.

    First, we identify the ping nearest to each known stop location, indicated in stop_id_loc 
    (along with its stop order in the pattern with stop_sequence_loc). Next, we look for the 
    following in order:
        1. door open cases within 100 feet of this stop.
        2. stop groups that had a door open within 100 feet of this stop
            (a slightly broader definition of the above that also looks for other related
             pings around the door open, such as when the bus serves a stop and then edges towards
             the intersection and stops again).
        3. door closed cases within 100 feet of this stop.
    In each case above, the distance from a stop location is identified using the odometer reading 
    at the nearby stop_id_loc field. Additional checks are performed to ensure that no stop_id is 
    matched twice, and that no stop_id is matched to more than one cluster of stop activity.
    
    TODO: 
        - We will also likely want to revist this is in light of further map matching work to be
        done.

    Parameters
    ----------
    rawnav : pd.DataFrame
        rawnav data.
    stop_index : pd.DataFrame
        stop_index table previously created with merge_rawnav_target() and related schedule merge
        code.

    Returns
    -------
    rawnav: pd.DataFrame
        rawnav data with stop identifiers added.
         - stop_id_loc: identifies the rawnav ping nearest to each known stop location. It takes a 
           value of the stop_id at that ping.
         - stop_sequence_loc: identifies the scheduled stop order of that ping.
         - stop_id_group: for groups of stopped  (stopped_changes_collapse), identifies the 
             corresponding stop_id. See description for how this is done.
         - stop_case: for stop groups, either "atstop" or "notatstop" for all pings in the group.
             If the vehicle passes the stop without stopping, at the ping nearest to the stop,
             stop_case is 'passstop'.
         - stop_decomp_ext: Combines stop_case with stop_decomp to create values that are
             easier to make use of.

    """   
    #### Find the nearest point to any stop
    # This adds a column 'stop_id_loc' and 'stop_sequence_loc'
    # because we could have aggregated a timestamp value that is the index_loc
    # join value, we 
    new_nearest = (
        pd.merge_asof(
            stop_index
            .filter(items = ['filename','index_run_start','index_loc','stop_id','stop_sequence'])
            .sort_values(['index_loc'])
            .rename(
                columns = {
                    'stop_id' : 'stop_id_loc',
                    'stop_sequence' : 'stop_sequence_loc',
                    'index_loc' : 'stop_index_loc'
                }
            ),
            rawnav
            .filter(['filename','index_run_start','index_loc'], axis = "columns")
            .rename(
                columns = {
                    'index_loc' : 'rawnav_index_loc'
                }
            )
            .sort_values(['rawnav_index_loc'])
            ,
            by = ['filename','index_run_start'],
            left_on = ['stop_index_loc'],
            right_on = ['rawnav_index_loc'],
            # in some cases we collapse right at the index_loc we need to join on, so this
            # is our quick solution
            direction = "nearest",
            tolerance = 10
        )
        .drop(
            ['stop_index_loc'],
            axis = "columns"
        )
        .rename(
            columns = {"rawnav_index_loc" : 'index_loc'}
        )
    )
    
    rawnav = (
        rawnav
        .merge(
            new_nearest,
            on = ['filename', 'index_run_start', 'index_loc'],
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
    # stopped_changes_collapsed and the stop_id 
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
        .sort_values(['odom_ft_stop','index_loc'])
    )
    
    # we make a copy of the data and will remove bits of the data as we successfully complete
    # rounds of the iteration.
    rawnav_fil = (
        rawnav
        .sort_values(by = ['odom_ft'])
    )
    
    query_list = [
        'stop_decomp == "doors_at_O_S"',
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
    
        # chuck cases where the nearest stop is not within 100 ft. this is arbitrary
        # if there are multiple possible matches, pick the one that has the minimum distance
        # and if there are still multiple matches, pick the first.
        # Changed from 200 ft to 100 ft after some intersections were associated with farside
        # stops when the farside stop was passed up
        
        rawnav_stopareas_idd_fil = (
            rawnav_stopareas_idd
            .assign(odom_ft_stop_diff = lambda x: abs(x.odom_ft_stop - x.odom_ft))
            .groupby(['filename','index_run_start','stopped_changes_collapse'])
            .apply(
                lambda x: 
                    x.loc[x.odom_ft_stop_diff.le(100) & 
                          (x.odom_ft_stop_diff == min(x.odom_ft_stop_diff))]
            )
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
    # TODO: do we need to de-duplicate one more time at this point? I believe not, but more tests
    # needed.
    
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
        # didn't join to a stop in order to indicate passups.
        # this is why the index_loc column is kept along.
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
                    x.stop_id_group.notna() & x.door_case.notna(),
                    x.stop_id_group.notna() & x.door_case.isna(),
                    x.stop_id_group.isna() & x.door_case.notna()
                ],
                [
                    'atstop',
                    'passstop',
                    'notatstop'
                ],
                default = pd.NA
            )    
        )
        .assign(
            stop_decomp_ext = lambda x:
                np.where(
                    x.stop_case.isin(['atstop','notatstop']),
                    x.stop_case + "_" + x.stop_decomp,
                    x.stop_case
                )
        )
        .drop(columns = ['stop_id_group_pu'])
        .sort_values(by = ['filename', 'index_run_start', 'index_loc'])
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
    """
    Create stop segments.
	
	Creates stop segments using the matched stop information. Stop segments are based on the groups
    of stop-related activity, not on the actual stop locations. This has the benefit of better 
    disaggregating time at a stop from time in motion when evaluating stop-to-stop segment times.
	
	At each stop, the column 'trip_seg' takes only the stop_id_group value. Between stop activity, 
    trip_seg is a string that collapses the previous and next stop, as in "25698_23697". Travel
    between the start of the trip instance and the first stop is identified as 
    "tripstart_[first matched stop ID]", while travel after the last stop and to the end of the
    trip instance is identified as "[last matched stop ID]_tripend". 
	
	See details below on other columns created.
	
	TODO: Some of these steps may best belong in other parts of the decomposition code, namely the
    match_stops function.

    Parameters
    ----------
    rawnav : pd.DataFrame
        rawnav data that has undergone previous processing steps.

    Returns
    -------
    rawnav data with additional columns:
	    - trip_seg: See description. 
		- basic_decomp_ext: identifies whether deceleration, acceleration, and stopped phases are 
        associated with a door open event/stop, or whether they are not (i.e. related to stopping
        at an intersection). 
        - stop_id_group_ext: associated a stop_id_group identifier not only with the stop activity,
        but also with the acceleration and deceleration phases around the stop. This is intended to
        support comparisons of accel/decel at a stop across trip instances.		
		
    """
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
            door_case = ('door_case','first'),
            stop_id_group = ('stop_id_group','first')
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
    )
    
    rawnav = (
        rawnav        
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
            ),
            stop_id_group_ext = lambda x: np.select(
                [
                    x.basic_decomp.eq('accel'),
                    x.basic_decomp.eq('decel')
                ],
                [
                    x.stop_id_group_prev,
                    x.stop_id_group_next
                ],
                default = x.stop_id_group
            )
        )
        .drop(
            ['stop_seg',
             'stopped_changes_collapse_next',
             'stopped_changes_collapse_prev',
             'door_case_prev',
             'door_case_next',
             'stop_id_group_prev',
             'stop_id_group_next'],
            axis = 'columns'
        )
    )
    
    return(rawnav)


def trim_ends(
        rawnav_ti
    ):
    """
    Remove pings from trips before the first stop and after the last stop.
	
	This is done based on the earliest ping with non-null stop_id_group_ext values
    (i.e., the area around a stop including accel/decel time) or non-null stop_id_loc value 
    until the last ping with any of these non-null characteristics. This has the consequence that
    some trip instances may be longer or shorter depending on whether they stopped at the first 
    matched stop. As a result, odometer resets are still required in a subsequent step. The use of
    this trimming is done to improve visualization and correct for trip instances that do not have
    trip end tags. 

    Parameters
    ----------
    rawnav_ti : pd.DataFrame
        rawnav dataframe that has undergone previous processing steps.

    Returns
    -------
    rawnav_ti : pd.DataFrame
		rawnav dataframe with parts of trip instance ends removed.

    """    
    rawnav_ti = (
         rawnav_ti
         .assign(index_loc_int = lambda x: x.index_loc.astype("int64"))
         .set_index('index_loc_int')
    )
    
    first_idx = (
        rawnav_ti
        .loc[rawnav_ti.stop_id_group_ext.notna() | rawnav_ti.stop_id_loc.notna()]
        .first_valid_index()
    )
    
    last_idx = (
        rawnav_ti
        .loc[rawnav_ti.stop_id_group_ext.notna() | rawnav_ti.stop_id_loc.notna()]
        .last_valid_index()
    ) + 1
    
    rawnav_ti = (
        rawnav_ti
        .loc[first_idx:last_idx]    
        .reset_index(drop = True)
    )    
    
    return(rawnav_ti)

#### Decomposition
def decompose_mov(
    rawnav,
    slow_fps = 14.67, # upped default to 10mph
    steady_accel_thresh = 2,
    stopped_fps = 3): 
    """
    Add movement decomposition variables to rawnav data.

    Parameters
    ----------
    rawnav : pd.DataFrame
        rawnav data with requisite input columns
    slow_fps : float or int, optional
        The speed at which to consider vehicle movement low. The default is 14.67.
	steady_accel_thresh : float or int, optional
        The absolute value of acceleration below which we can consider a vehicle to be in a steady state, conditional on being above slow_fps. The default is 2.
    stopped_fps : int or float, optional
        The speed in fps under which to consider a vehicle stopped. The default is 3.

    Returns
    -------
    rawnav: pd.DataFrame
        rawnav data with decomposition columns added:
        - veh_state_calc: A recalculation of veh_state based on the stopped_fps parameter.
        - basic_decomp: the decomposition of values into accel, decel, stopped, steady state, 
            and other_delay.
        - stopped_changes_collapse: an integer value that changes every time the trip instance 
            reaches a stop or leaves a stop. The 'collapse' refers to the fact that these changes 
            are collapsed in cases where the vehicle makes small movements around a stop area, such
            as being stopped to serve passengers and then pulling up to the stop bar. 
        - any_door_open: within a stopped_changes_collapse group, whether the door opened
            at any time.
        - any_veh_stopped: within a stopped_changes_collapse group, whether the vehicle was stopped 
            at any time based on veh_state_calc.
        - door_changes: within a stopped_changes_collapse group, a column that increments every
            time the door state changes from open to closed. Is NA outside of groups where the 
            vehicle did not stop.
        - relative_to_firstdoor: within a stopped_changes_collapse group where the vehicle stopped, 
            identifies where the ping falls relative to the first door open. Takes values 'pre' 
            (before first door open), 'post' (after first door open), 'at' (is the first door 
            open), or 'na' (the vehicle stopped but the door did not open). Is NA outside of groups
            where the vehicle did not stop. 
        - door_case: if the basic_decomp is 'stopped', were there any doors open ('doors') or no 
            doors open ('nodoors')? Based on any_door_open and basic_decomp. Is NA outside of groups
            where the vehicle did not stop. 
        - pax_activity: If the door opened, was there passenger activity? Based on row_before_apc.
            Is NA outside of groups where the vehicle did not stop.
        - stop_decomp: Combines door_case, relative_to_firstdoor, door_state, and veh_state_calc 
            into a single field for ease of use. Is NA outside of groups where the vehicle did
            not stop.
    """
    #### Categorize stopped movement
    rawnav = (
        rawnav
        .assign(
            # need a True/False that's easily coercible to numeric
            # TODO: if it's the last ping and there's no speed, should probably fill forward 
            # from the last ping.
            is_stopped = lambda x, stop = stopped_fps, slow = slow_fps : 
                (
                    # in case the instantaneous speed is off, we also check that the smoothed
                    # speed is below the slow rate too
                    # WT: since i simplified some of the odometer aggregation and removed that
                    # interpolation, there are a few things that are a bit off again that need 
                    # more TLC
                    (x.fps_next.le(stop) & x.fps_next_sm.le(slow)) | 
                    x.door_state.eq("O")
                )
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
       
    rawnav = (
        rawnav
        .assign(
            is_steady = lambda x, thresh = steady_accel_thresh, slow = slow_fps: 
                (x.fps_next_sm.ge(slow)) & 
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
            # is that we want to collapse stop activity wihtin 100 feet to one case on the notion
            # that hte bus is probably just pulling forward from bus stop to the intersection 
            # stop bar/crosswalk. 150 feet proved a little long, as some farside
            # stops would catch the previous stop at an intersection.
            near_stop = lambda x: x.min_odom - x.odom_ft <= 100
        )
        .sort_values(by = ['filename','index_run_start','index_loc'])
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
            # even though this is more of an integer, for conversion to parquet later
            # we just make it float64 now.
            door_changes = lambda x: 
                np.where(
                    x.any_veh_stopped.eq(False),
                    np.nan,
                    x.door_changes.astype('float64')
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
    """
    Interpolate odometer reading where odom_ft is missing. 
    
    These missing values are typically inserted by other functions so that interpolation can 
    replace these values.
    
    The interpolation can have limits, however. If fix_interp is True, interpolated values 
    are constrained to between odom_ft_min and odom_ft_max values that are created in agg_sec.
    The ft_threshold parameter controls how far beyond these two values the interpolated value can 
    go--we found that in many cases, the interpolated value may be just a half a foot outside the
    range, which is likely a consequence of the odom_ft readings being integers only. If those 
    limits are exceeded, the interpolation is reset to the nearest value of odom_ft_min or 
    odom_ft_max. The range of those values may be a few feet or many dozen, but doing this 
    correction avoids spikes or drops in speed values.    
    
    interp_method = "index" provides room to change the interpolation method selected
    for use in np.interpolate, and is left here as a bit of future proofing.

    Parameters
    ----------
    x : pd.DataFrame
        A trip instance.
    ft_threshold : int, optional
        Threshold in feet over . The default is 1.
    fix_interp : bool, optional
        Whether to fix interpolated values that fall outside the odom_ft_min and 
        odom_ft_max range. The default is True.
    interp_method : str, optional
        interpolation method passed to np.interpolate argument. The default is "index".

    Raises
    ------
    ValueError
        If sec_past_st is still duplicated at this point, the function will raise an error. Using
        agg_sec before calling interp_over_sec should address this.

    Returns
    -------
    x : pd.DataFrame
        rawnav trip instance with the odom_ft values interpolated.
    """
    # this is done because many interpolation functions are based on the index.
    x.set_index(['sec_past_st'], inplace = True)
    
    if (x.index.duplicated().any()):
        raise ValueError("sec_past_st shouldn't be duplicated at this point")
    else:
        # interpolate
        x.odom_ft = x.odom_ft.interpolate(method = interp_method)
        
        # Could probably make some of this tidier, but oh well.
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
    Aggregate observations with repeated timestamp values.
    
    Rawnav data has integer timestamp values. These values can sometimes repeat, which wreaks havoc
    on speed calculations, doubly so when odometer readings associated with these timestamps may
    be different. To address the first part of this challenge--repeated seconds readings--we
    aggregate values where the timestamp is repeated, typically using the last observation's value.
    
    TODO: in the datamart environment, there are a few columns that we're not likely going to have
    (e.g., row_before_apc) or that will be named differently. Will have to make updates here,
    since we refer to these things more explicitly.
    
    Parameters
    ----------
    rawnav : pd.DataFrame
        rawnav file that may have duplicated seconds values

    Returns
    -------
    rawnav: pd.DataFrame
        rawnav file that no longer has duplicated seconds values. Columns are also slightly 
        reordered. In cases where duplicated seconds 
        readings were found, the last value is taken. Exceptions include the following below. These
        columns are not particularly useful with the exception of odom_ft_min and odom_ft_max,
        but we were wary of 'throwing away data' even at this stage. 
            - stop_window_e : column that takes the E-** value from the stop_window field. This is
            done to 
            prevent conflicts where the E-** and X-1 indicators appear in the same second. This may
            not strictly be necessary, but is a bit of future proofing in case the stop_window
            indicators become relevant again. If there were hypothetically multiple E-** indicators
            in the same second, they are joined by a comma in this field.
            - stop_window_x : column that takes the X-1 value from the stop_window field. This is
            done to 
            prevent conflicts where the E-** and X-1 indicators appear in the same second. This may
            not strictly be necessary, but is a bit of future proofing in case the stop_window
            indicators become relevant again. If there were hypothetically multiple E-** indicators
            in the same second, they are joined by a comma in this field.
            - blank : any non-blank values in this column (typically blank) are joined with commas.
            - row_before_apc : the row_before_apc indicators are joined with a , .
            - collapsed_rows : the count of the number of collapsed rows. 
            - odom_ft_min : the minimum odometer reading where there were two observations at the
            same second. Used for data cleaning in interp_over_sec()
            - odom_ft_max : the maximum odometer reading where there were two observations at the
            same second. Used for data cleaning in interp_over_sec().
            - veh_state_all : Unique values of vehicle state where there were two observations at 
            the same second. Vehicle state isn't really used at all here, but we are trying to avoid 
            throwing out data.

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

    # because the number/names of columns that come into this function are somewhat variable,
    # we will specify lists of columns to join together, but will otherwise just take the last()
    # observation of the remaining ones
    rawnav_dupe = (
        rawnav
        .loc[rawnav.dupes == True]
        # trick here is i'm not sure what columns we'll keep
    )
     
    # these are ones we don't want to have aggregated with 'last' later. We otherwise don't
    # use this list
    cols_custom = [
        'stop_window_e',
        'stop_window_x',
        'blank',
        'row_before_apc',
        'collapsed_rows'
    ]    
    
    # including sec_past_st because we're collapsing on that
    cols_idx = ['filename','index_run_start','sec_past_st']
    
    cols_last = [col for col in rawnav_dupe.columns if col not in (cols_custom + cols_idx)]
    
    rawnav_dupe_custom = (
        rawnav_dupe
        .groupby(cols_idx)
        .agg(
            stop_window_e = ('stop_window_e', lambda x: x.str.cat(sep=",",na_rep = None)),
            stop_window_x = ('stop_window_x', lambda x: x.str.cat(sep=",",na_rep = None)),
            # TODO: this still comes out weird, because sometimes blank is originally read in from
            # parquet as float. Doesn't really matter, but anyway.
            blank = ('blank', lambda x: ','.join(x.unique().astype(int).astype(str))),
            # is 1 or 0, so if apc activity occurred, note that it did.
            row_before_apc = ('row_before_apc', 'max'),
            collapsed_rows = ('index_loc','count'),
            odom_ft_min = ('odom_ft','min'),
            odom_ft_max = ('odom_ft','max'),
            odom_ft_mean = ('odom_ft', 'mean'),
            veh_state_all = ('veh_state', lambda x: ','.join(x.unique().astype(str)))
        )
        # this seems to work more consistently than replace()
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
    
    rawnav_dupe_last = (
        rawnav_dupe
        [cols_last + ['filename','index_run_start','sec_past_st']]
        .groupby(cols_idx)
        .last()
    )
    
    rawnav_dupe_out = (
        rawnav_dupe_custom
        .merge(
            rawnav_dupe_last,
            how = "outer",
            left_index = True,
            right_index = True
        )
        .assign(
            odom_ft = lambda x: np.where(x.odom_ft_mean.notna(),x.odom_ft_mean,x.odom_ft)
        )
        .reset_index()
    )    
            
    # recombine duplicated and non-duplicated
    rawnav = (
        pd.concat([
            rawnav_dupe_out,
            rawnav_nodupe
        ])
        .sort_values(
            by = ['filename','index_run_start','index_loc']    
        )
        .drop(
            ['dupes','stop_window'],
            axis = "columns"
        )
        # we have issues with some columns after the concat in that they don't always
        .assign(
            row_before_apc = lambda x: x.row_before_apc.astype(str),
            blank = lambda x: x.blank.astype(str)
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
             'odom_ft_mean',
             'stop_window_e',
             'stop_window_x',
             'row_before_apc'
             ]
        )            
    )
    
    return(rawnav)

def interp_over_sec(rawnav, interp_method = "index"):
    """
    Interpolate odom_ft over seconds.
    
    This is a separate step from agg_sec, though you'd think the two would be related. 
    In fact, there are some places where we'll want to interpolate beyond just the places where we 
    aggregated seconds. In particular, through exploratory analysis, we find that there are some 
    situations where the rawnav odometer reading is likely to be wrong, so we NA those values
    out so they can be interpolated over.
    
    TODO: In general, we'll probably want to revisit the aggregation/interpolation process, so
    we've hit pause on this for now.

    Parameters
    ----------
    rawnav : pd.DataFrame
        rawnav dataframe that has been pre-processed by the agg_sec() function        
    interp_method : str, optional
        interpolation method passed to np.interpolate. The default is "index".

    Returns
    -------
    rawnav: pd.DataFrame
        rawnav dataframe that has had its odometer feet values interpolated in cases where 

    """
    #### first, set as NA the values that we plan to interpolate over.
    # TODO: In the future, we probably will not want to be dropping data,
    # it's a bit sloppy. For now there are some known 'bad' values and it's a little
    # bit easier to just get rid of them and linearly interpolate since we're only looking
    # at a few cases.
    # first, we look for ones that have one missing second afterwards but have one second before
    # for various reasons, these odometers tend to come out high
    # Note that we calculate the marginal seconds values in a few places, but because we've just
    # done various kinds of aggregation, we want to recalculate here.
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
    # TODO: if the repeated values are the same (e.g., odom_ft_min and odom_ft_max are same), 
    # we should probably not NA these out
    rawnav['row_number'] = (
        rawnav
        .groupby(['filename','index_run_start'], sort = False)
        .cumcount()
    )
    
    rawnav = (
        rawnav
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
        .apply(lambda x: interp_odom(x, interp_method = interp_method))
        .reset_index(drop = True)
    ) 
        
    
    #### Clean up
    # Remove columns we created
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
    """
    Calculate speed based on odom_ft and sec_past_st.
	
	While this function can be run on rawnav data that has not been processed
	in any other way, we run this after the agg_sec and interp_over_sec
	functions. In this way, the calculated speeds are not affected by known
	issues with the odometer and sec_past_st fields. Because the odom_ft and
	sec_past st fields are integers, the resulting speed values will
	nevertheless be somewhat noisy. These speed values are smoothed in the
	function smooth_speed that follows.
    
    Note that we don't bother to calculate things like the accleration or jerk here; it has seemed
    better to do so off of the smoothed values. 

    Parameters
    ----------
    rawnav : pd.DataFrame
        rawnav dataframe.

    Returns
    -------
    rawnav : pd.DataFrame
		rawnav dataframe with additional columns:
		- odom_ft_next : Within each trip instance, the next odometer reading for a given ping.
		- sec_past_st_next: Within each trip instance, the next sec_past_st reading for a given 
            ping.
		- secs_marg: Within each trip instance, the difference in seconds between the current and 
            the next sec_past_st reading for a given ping.
		- odom_ft_marg : Within each trip instance, the difference in feet between the current and
            the next odomometer reading for a given ping.
		- fps_next :  Within each trip instance, the speed in feet per seconds between the current
            and next ping .

    """
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
    """
    Calculate the acceleration and jerk values at each ping.
    
    A few historical reasons for the jank in this function. Previously we calculated other rolling
    values here too, and at some points we experimented with only calculating rolling variables 
    within each stop segment. 'fps_col' is also an argument because we also experimented with 
    calculating acceleration and jerk off of both the smoothed value and the original value.

    Parameters
    ----------
    rawnav : pd.DataFrame
        rawnav data with a speed column identified by 'fps_col.'
    groupvars : list, optional
        variables that define groups. The default is ['filename','index_run_start'].
    fps_col : str, optional
        String . The default is 'fps_next'.

    Returns
    -------
    rawnav : pd.DataFrame
        rawnav data with additional variables.

    """
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
    # TODO: some of these names are quite unforuntate...
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

def savitzky_golay(y, window_size, order, deriv=0, rate=1):
    r"""Smooth (and optionally differentiate) data with a Savitzky-Golay filter.
    
    borrowed from : https://scipy-cookbook.readthedocs.io/items/SavitzkyGolay.html
    and inspired by https://stackoverflow.com/questions/20618804/how-to-smooth-a-curve-in-the-right-way
    
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
    """
    Expand rawnav timestamp data to cover full extent of trip instance.
    
    Expands rawnav trip instance data such that there is one observation for every second.
    This 'expanded' data will later be discarded, but is required for the use of the interpolation
    function we currently use. In the expanded dataset, speed values are filled forward, rather 
    than interpolated between. This was done to better handle cases where the vehicle is stopped 
    for a long period of time before finally reaccelerating. For those intermediate seconds, we
    don't want to assume that the vehicle is slowly edging forward.
    
    Parameters
    ----------
    rawnav_ti : pd.DataFrame
        rawnav trip instance with fps_next variable.
    
    Returns
    -------
    rawnav_ti_expand : pd.DataFrame
    		rawnav trip instance additional rows for every second between the first and last 
            sec_past_st value. The fps_next speed values are filled for all rows where the value 
            was not previously calculated. No other variables are present. 
    
    """
    rawnav_ti_expand = (
        pd.DataFrame(
            {'sec_past_st' : np.arange(rawnav_ti.sec_past_st.min(), rawnav_ti.sec_past_st.max(),1 )} 
        )
    )
    
    # this will happen when you have only one row of input data.
    # see for instance rawnav06424171024.txt at 4258
    # likely better to just ditch these trip instances earlier, but trying to 
    # make functions a bit more resilient.
    # Usually if you have this problem we still have to do other error handling later, 
    # however.
    if (rawnav_ti_expand.shape == (0,1)):
        rawnav_ti_expand = rawnav_ti[['sec_past_st','fps_next']]
    else:
        rawnav_ti_expand = (
            rawnav_ti_expand
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

def apply_smooth(rawnav_ti):
    """
    Apply smoothing functions.
    
    Applies the savitzky_golay smoothing function and applies additional modifications to smoothed 
    results. Also calls function to calculate acceleration and jerk values.
    
    The arguments used for the savitzy_golay function are somewhat arbitrary.

    Parameters
    ----------
    rawnav_ti : pd.DataFrame
        A single trip instance of rawnav data

    Returns
    -------
    rawnav_ti : pd.DataFrame
		A single trip instance of rawnav data with additional columns (see description in 
        'smooth_speed')

    """
    rawnav_ex = expand_rawnav(rawnav_ti)
    
    try:
        # this can error on very short data frames where interpolation can't take place
        rawnav_ex['fps_next_sm'] = savitzky_golay(rawnav_ex.fps_next.to_numpy(), 21, 3)    
    except:
        # where interpolation fails, we just return the original values
        rawnav_ex['fps_next_sm'] = rawnav_ex['fps_next']
    
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
    
    # I'm not sure this is really the best place to do this, but we could pull out into a separate
    # function call at a higher level.
    rawnav_ex = (
        rawnav_ex
        # a little hack to shortcut the need to group, since we're doing all this by trip 
        # instance. This is probably another reminder 
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

def smooth_speed(rawnav):
    """
    Smooths speed values and create accel and 'jerk' values.
	
	This is a function that calls apply_smooth on each trip instance. 
    For details, see that function.

    Parameters
    ----------
    rawnav : pd.DataFrame
        rawnav data with calculated speed values fps_next

    Returns
    -------
    rawnav : pd.DataFrame
		rawnav data with additional columns:
		- fps_next_sm : smoothed feet per second speed
		- accel_next : acceleration between the current and next ping
		- jerk_next : jerk (second derivative of speed, first derivative of acceleration) between 
            the current and next ping.

    """
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
    """
    Calculate rolling averages.
    
    Averages speed and acceleration values across certain intervals
    of time: 3 seconds, 9 seconds, etc. These values enter into the decomposition calculation.    
    
    Note that the rolling nature of these functions is in terms of seconds, not observations. So if
    a ping has no other pings within 9 seconds of it (centered on that observation), the average
    will not include other values.
    
    Again, groupvars parameter is there for historical reasons: we experimented with apply rolling
    values only within stop segments to avoid cases where values after a stop might be incorporated
    into average speeds. This proved to be time intensive and not particularly any more accurate,
    so it was dropped.
    
    Also note that because the smoothed speed variable is currently used to calculate accel_next
    and jerk_next, even though these rolling variables aren't flagged as smoothed ones, they
    in fact incorporate those smoothed values.

    Parameters
    ----------
    rawnav : pd.DataFrame
        rawnav dataframe with fps_next, accel_next, and jerk_next values, having gone through
        agg_sec and interp_over_sec processes as well.
    groupvars : list, optional
        List of grouping variables. The default is ['filename','index_run_start'].

    Returns
    -------
    rawnav : pd.DataFrame
        rawnav dataframe with additional columns:
           - fps3 : speed in fps averaged over three observations, centered on the ping in question
           - accel3 : accel averaged over three observations, centered on the ping in question
           - jerk3 : jerk averaged over three observations, centered on the ping in question
           - accel9 : accel averaged over nine observations, centered on the ping in question

    """
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