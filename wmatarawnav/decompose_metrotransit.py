# -*- coding: utf-8 -*-
"""
Created on Mon June 7 03:48 2021

@author: WylieTimmerman
"""
import pandas as pd
import numpy as np
from . import low_level_fns as ll
from . import decompose_rawnav as dr

def assign_stop_area(
    rawnav,
    stop_field = "stop_window",
    upstream_ft = 150,
    downstream_ft = 150
):
    # TODO: consider adding a sequential numbering for the parts that aren't in a stop window.
    # TODO: assert that stop_field exists

    # TODO: break this down into more subfunctions so we can show the intermediate results more
    # clealry, test, etc.
    # %% IDENTIFY STOP WINDOWS
    # Identify the start and end of stop windows
    # WE'll then use merge_asof to identify pings within the stop window.
    # This method seems to work well enough. Considered using cut functions as an alternative to 
    # merge_asof, but this works well enough. 
    # TODO: door open can actually happen somewhat away from the stop area indicator, should confirm
    if (stop_field == "stop_window"):
        rawnav_stop = rawnav[rawnav.stop_window.str.contains("^E")]
    else:
        rawnav_stop = rawnav[rawnav[stop_field].notnull()]

    rawnav_stop_window_ind = (
        rawnav_stop
        .filter(['filename','index_run_start',stop_field,'odom_ft'])
        .assign(
            stop_window_start = lambda x, upft = upstream_ft: x.odom_ft - upft,
            stop_window_end = lambda x, dnft = downstream_ft: x.odom_ft + dnft
        )
    )
    
    # TODO: is there a way to keep this in the method chain above? 
    rawnav_stop_window_ind.loc[rawnav_stop_window_ind['stop_window_start'] < 0, 'stop_window_start'] = 0
    
    rawnav_stop_window_max = (
        rawnav
        .groupby(['filename','index_run_start'])
        .agg({"odom_ft" : ['max']})
        .pipe(ll.reset_col_names)
        )
    
    rawnav_stop_window_ind = (
        rawnav_stop_window_ind
        .merge(
            rawnav_stop_window_max,
            on = ['filename','index_run_start'],
            how = 'left'
            )
        # NOTE: this isn't actually strictly necessary given how we merge, but maybe good to 
        # run this and check for issues anyhow
        .assign(
            stop_window_end = lambda x: 
                np.where(
                    x.stop_window_end > x.odom_ft_max,
                    x.odom_ft_max,
                    x.stop_window_end
                )
        )
        .drop(columns = ['odom_ft_max','odom_ft'])
    )

    # note: i think the biggest to-do is to drop cases with repeated stops or 
    # overlapping categories 
    # TODO: remove items where the stop sequence is out of order
    # TODO: address case where stop areas are overlapping
    # TODO: check that sequence of stop_window_start and stop_window_end is monotinic
    # TODO: perhaps check that stop sequences are complete
    
    # MERGE TOGETHER
    # TODO: should set index sooner
    # NB: I was receiving memory error warnings in the code, which i took to mean
    # i had too many intermediate objects, so i went back to overwriting the same object
    # each time. It ended up not being the kind of memory error i thought, so this
    # ends up being unneccessary. Anyhow, apologies in advance if you're debugging.
    
    rawnav = (
        rawnav
        .sort_values(by = ["odom_ft",'index_loc'])
        .pipe(
            pd.merge_asof,
            right = (
                rawnav_stop_window_ind
                .sort_values(by = "stop_window_start")
                .drop(columns = ['stop_window_end'])
            ),
            by = ['filename','index_run_start'],
            left_on = 'odom_ft',
            right_on = 'stop_window_start',
            # this seems the opposite of both my intuition and the docs, but oh well.
            direction = 'backward',
            suffixes = ['','_area_start']
        )
        .pipe(
            pd.merge_asof,
            right = (
                rawnav_stop_window_ind
                .sort_values(by = "stop_window_end")
                .drop(columns = ['stop_window_start'])
            ),
            by = ['filename','index_run_start'],
            left_on = 'odom_ft',
            right_on = 'stop_window_end',
            direction = 'forward',
            suffixes = ['','_area_end']
            )
        .sort_values(by = ['filename','index_run_start','index_loc'])
    )
    
    rawnav = (
        rawnav
        .assign(
            stop_window_area = lambda x: 
                np.where(
                    # if the two windows looking forward and backward are the same, 
                    # that means you're in the stop window
                    x.stop_window_area_start == x.stop_window_area_end, 
                    x.stop_window_area_start ,
                    np.nan
                    )
        )
        .drop(
            columns = [
                'stop_window_area_start',
                'stop_window_start',
                'stop_window_area_end',
                'stop_window_end'
            ]
        )
    )

    return(rawnav)

def decompose_basic_mt(
    rawnav,
    speed_thresh_fps = 7.333):

    # TODO: assert that stop_window_area exists

    # %% IDENTIFY PAX VS. NON-PAX STOP AREA
    
    # Here we yank some code from decompose_rawnav.py; it's typically only applied to a filtered set
    # of stop data, but for now, we'll just tack it on.
    
    # add rolling values
    rawnav = (
        dr.calc_rolling_vals(rawnav)
    )

    # Add binary variables
    rawnav_stop_area = (
            rawnav
            .dropna(subset = ['stop_window_area'])
        )
    
    rawnav_stop_area = (
    	rawnav_stop_area
    	.assign(
    		door_state_closed=lambda x: x.door_state == "C",
    		# note this returns False when fps_next is undefined. We'll have to handle this 
    		# carefully in later stages, as we don't want to inadvertently signal this as a change
    		# in status.
    		veh_state_moving=lambda x: x.fps_next > 0                                                                                               
    	)
    )
    
    # Add a sequential numbering that increments each time door changes in a run/segment combination   
    rawnav_stop_area['door_state_changes'] = (
    	rawnav_stop_area
    	.groupby(['filename','index_run_start','stop_window_area'])['door_state_closed']
    	.transform(lambda x: x.diff().ne(0).cumsum())
    )
    
    # We have to be more careful for vehicle state changes. At times, we'll get undefined speeds
    # (e.g., two pings have the same distance and time values) and given how such values are 
    # handled in python, this could be categorized  as a change in state if we use the 
    # same approach as above. Instead, we'll create a separate
    # table without 'bad' speed records, run the calc on state changes,
    # join back to the original dataset,
    # and then fill the missing values based on nearby ones.
    # Filling based on surrounding values is itself imperfect, but likely to be sufficient 
    # in many cases. This still isn't the end of the story -- in cases where we see no vehicle
    # state changes but see the door open at some point, we'll assume the vehicle actually did
    # stop, however, briefly.
    veh_state = (
    	rawnav_stop_area
    	.filter(items = ['filename','index_run_start','stop_window_area','index_loc','veh_state_moving'])
    	.loc[~rawnav_stop_area.fps_next.isnull()]
    )
    
    veh_state['veh_state_changes'] = (
    		veh_state
    		.groupby(['filename','index_run_start','stop_window_area'])['veh_state_moving']
    		.transform(lambda x: x.diff().ne(0).cumsum())
    )
    
    rawnav_stop_area = (
    	rawnav_stop_area
    	.merge(
    		veh_state
    		.drop(columns = ['veh_state_moving']),
    		on = ['filename','index_run_start','stop_window_area','index_loc'],
    		how = 'left'
    	)
    )
    
    # Note that this could miss cases of transition where the null value for speed occurs
    # at a stop where passengers board/alight. However, if that's the case, we don't use 
    # these values anyhow.
    rawnav_stop_area['veh_state_changes'] = (
    	rawnav_stop_area
    	.groupby(['filename','index_run_start','stop_window_area'])['veh_state_changes']
    	.transform(lambda x: x.ffill())
    	.transform(lambda x: x.bfill())
    )
    	   
    # To identify the cases of the first door opening and last at each stop (needed for decomposition),
    # we'll summarize to a different dataframe and rejoin. 
    # The 'min' is almost always 2, but we're extra careful here in case the door is open at the 
    # start of the segment.
    # 'max' will be interesting - we'll add anything after the first door closing to the last reclosing
    # as 't_l_addl' (which under some circumstances would be signal delay)
    door_open_cases = (
    	rawnav_stop_area
    	.loc[rawnav_stop_area.door_state == "O"]
    	.groupby(['filename','index_run_start','stop_window_area','door_state'])
    	.agg({"door_state_changes" : ['min','max']})
    	.pipe(ll.reset_col_names)
    	.drop(columns = ['door_state'])
    )
       
    # Before we make use of the door_open_cases min and max files, we'll do a similar check on where 
    # the bus came to be not moving. The object namign is a little fuzzy here -- we'll call this
    # 'veh_stop' to distinguish that we're talking about the bus literally not moving, 
    # rather than something to do with a 'bus stop'. This helps with runs where the bus does not
    # stop at all.
    veh_stop_cases = (
    	rawnav_stop_area
    	.loc[(~rawnav_stop_area.veh_state_moving & rawnav_stop_area.fps_next.notnull())]
    	.groupby(['filename','index_run_start','stop_window_area','veh_state_moving'])
    	.agg({"veh_state_changes" : ['min','max']})
    	.pipe(ll.reset_col_names)
    	.rename(columns = {"filename_" : "filename",
    					  "index_run_start_": "index_run_start",
    					  "stop_window_area_":"stop_window_area",
    					  "veh_state_changes_min": "veh_stopped_min",
    					  "veh_state_changes_max": "veh_stopped_max"})
    	.drop(columns = ['veh_state_moving'])
    )
    
    # There will be nans remaining here from cases where bus did not stop or did not pick up 
    # passengers. This is okay, we'll handle these in a bit.
    
    rawnav_stop_area = (
    	rawnav_stop_area
    	.merge(
    		door_open_cases,
    		on = ['filename','index_run_start','stop_window_area'],
    		how = 'left'
    	)
    	.merge(
    		veh_stop_cases,
    		on = ['filename','index_run_start','stop_window_area'],
    		how = 'left'
    	)
    )
    
    # For convience in other downstream calcs, we'll add flags to help with certain cases 
    # where vehicle didn't stop at all or doors didn't open.
    # Similar approach for door open but directly applied
    rawnav_stop_area['any_door_open'] = (
    	rawnav_stop_area
    	.groupby(['filename','index_run_start','stop_window_area'])['door_state_closed']
    	.transform(lambda x: any(~x))       
    )
    
    # These will be decomposed a little bit differently.
    # We'll reuse a table we made earlier since we've dealt with fps_next nulls here.
    veh_state_any_move = (
    	veh_state 
    	.drop(columns = ['veh_state_changes'])
    )
    
    veh_state_any_move['any_veh_stopped'] = (
    	veh_state_any_move
    	.groupby(['filename','index_run_start','stop_window_area'])['veh_state_moving']
    	.transform(lambda x: any(~x))
    )
    
    rawnav_stop_area = (
    	rawnav_stop_area 
    	.merge(
    		veh_state_any_move
    		.drop(columns = ['veh_state_moving']),
    		on = ['filename','index_run_start','stop_window_area','index_loc'],
    		how = "left"
    	)
    	# some NA's may appear after join. We'll usually fill to address these (see below),
    )
    
    rawnav_stop_area['any_veh_stopped'] = (
    	rawnav_stop_area
    	.groupby(['filename','index_run_start','stop_window_area'])['any_veh_stopped']
    	.transform(lambda x: x.ffill())
    	.transform(lambda x: x.bfill())
    )
    
    # in the case where we know doors opened, we'll override and say the vehicle
    # stopped at some point.
    rawnav_stop_area = (
    	rawnav_stop_area
    	.assign(any_veh_stopped = lambda x: 
    			np.where(
    				x.any_door_open,  
    				True,
    				x.any_veh_stopped
    			)
    	)
    )
    		
    # We start to sort row records into phase based on vars we've created. This is just a first cut.
    rawnav_stop_area['rough_phase_by_door'] = np.select(
    	[
    		(rawnav_stop_area.door_state_changes < rawnav_stop_area.door_state_changes_min), 
    		((rawnav_stop_area.door_state == "O") 
    		  & (rawnav_stop_area.door_state_changes == rawnav_stop_area.door_state_changes_min)),
    		(rawnav_stop_area.door_state_changes > rawnav_stop_area.door_state_changes_min),
    		(rawnav_stop_area.door_state_changes_min.isnull()),
    	], 
    	[
    		"t_decel_phase", #we'll cut this up a bit further later
    		"t_stop1",
    		"t_accel_phase", #we'll cut this up a bit further later
    		"t_nopax", #we'll apply different criteria to this later        
    	], 
    	default="doh" #NOTE does this stand for anything? 
    )
    
    # Some buses will stop but not take passengers, so we can't use door openings to cue what 
    # phase the bus is in. in these cases, we'll take the first time the bus hits a full stop 
    # to end the decel phase. We could do this method for all runs (and the case could be made), but
    # leaving this as different (i.e. a bus that opens its doors at t_10 may have come to a full
    # stop earlier at t_5 and again at t_10; thus, the phase as calculated by veh state and 
    # by door state can be inconsistent). In practice, we won't use the values in this column except
    # in some special cases where bus is not serving pax.
    rawnav_stop_area['rough_phase_by_veh_state'] = np.select(
       [
    	(rawnav_stop_area.veh_state_changes < rawnav_stop_area.veh_stopped_min),
    	(rawnav_stop_area.veh_state_changes == rawnav_stop_area.veh_stopped_min),
    	((rawnav_stop_area.veh_state_changes > rawnav_stop_area.veh_stopped_min)
    	 & (rawnav_stop_area.veh_state_changes <= rawnav_stop_area.veh_stopped_max)),
    	(rawnav_stop_area.veh_state_changes > rawnav_stop_area.veh_stopped_max),
    	(rawnav_stop_area.veh_stopped_min.isnull())
       ],     
       [
    	"t_decel_phase",
    	"t_stop", #not tstop1, note - essentially just that the vehicle is stopped
    	"t_l_addl",
    	"t_accel_phase",
    	"t_nostopnopax" # this will be updated again in cases where bus opens doors but doesn't appear to stop
       ],        
       default = 'not relevant'
    )
    
    #NOTE - what's going on here??
    ##NOTE - BAM - I broke the .transform into 2 steps because it was breaking.. not sure why
    # the 2-step version is essentially what .transform is supposed to do
    #it might have something to do with the index...
    
    # In cases where bus is stopped around door open, we do special things.
    # First, we flag rows where bus is literally stopped to pick up passengers.
    # Note that based on t_stop1 definition, this only happens first time bus opens doors
    # This will be off in cases where the bus has door open time (t_stop1) but 
    # the vehicle never appears to stop, but our logic downstream will be unaffected.
    rawnav_stop_group = (
    	rawnav_stop_area
    	.groupby(['filename','index_run_start','stop_window_area','veh_state_changes'])['rough_phase_by_door'].agg(lambda var: var.isin(['t_stop1']).any())
    	.reset_index()
    	.rename(columns={'rough_phase_by_door':'at_stop'})
    	)
    
    rawnav_stop_area_merge = rawnav_stop_area.merge(rawnav_stop_group,
    						how='left',
    						on=['filename','index_run_start','stop_window_area','veh_state_changes'])
    
    # Though not strictly necessary, we'll fix teh cases where the vehicle never really stops
    # but we see door open time. Just in case anyone goes looking, don't want incorrect values
    rawnav_stop_area_merge = (
    	rawnav_stop_area_merge 
    	.assign(
    		at_stop = lambda x: 
    			np.where(
    				x.veh_stopped_min.isna().to_numpy() 
    				& (x.rough_phase_by_door.to_numpy() != "t_stop1"),
    				False,
    				x.at_stop
    			)
    	)
    )
    
    rawnav_stop_area_merge['at_stop_phase'] = np.select(
    	[
    		((rawnav_stop_area_merge.at_stop) 
    		 # One might consider condition that is less sensitive. Maybe speed under 2 mph?
    		 # Note that we don't use a test on fps_next because 0 dist and 0 second ping could
    		 # lead to NA value
    			 & (rawnav_stop_area_merge.odom_ft_marg == 0)
    			 & (rawnav_stop_area_merge.rough_phase_by_door == "t_decel_phase")),
    		((rawnav_stop_area_merge.at_stop) 
    			& (rawnav_stop_area_merge.odom_ft_marg == 0)
    			& (rawnav_stop_area_merge.rough_phase_by_door == "t_accel_phase"))
    	],
    	[
    		"t_l_initial",
    		"t_l_addl"
    	],
    	default = "NA" # NA values aren't problematic here, to be clear
    )
    
    # Finally, we combine the door state columns for the decomposition
    rawnav_stop_area = (
    	rawnav_stop_area_merge
    	# Assign the at_stop_phase corrections
    	.assign(stop_area_phase = lambda x: np.where(x.at_stop_phase != "NA",
    												 x.at_stop_phase,
    												 x.rough_phase_by_door))
    	# Assign the additional records between the first door closing to last door closing to
    	# t_l_addl as well
    	.assign(stop_area_phase = lambda x: np.where(
    		(x.stop_area_phase == "t_accel_phase")
    		& (x.door_state_changes <= x.door_state_changes_max),
    		"t_l_addl",
    		x.stop_area_phase
    		)
    	)
    )
    # And we do a final pass cleaning up the runs that don't serve passengers or don't stop at all
    stop_area_decomp = (  
    	rawnav_stop_area
    	# runs that don't stop
    	.assign(stop_area_phase = lambda x: np.where(x.any_veh_stopped == False,
    												 "t_nostopnopax",
    												 x.stop_area_phase))
    	.assign(stop_area_phase = lambda x: np.where(((x.any_door_open == False) 
    												  & (x.any_veh_stopped == True)),
    												 x.rough_phase_by_veh_state,
    												 x.stop_area_phase)
    	)
    )
    
    # Note: Columns maintained in output are likely excessive for most needs, but are left in 
    # for any debugging necessary.
    
    stop_area_decomp_tojoin = (
        stop_area_decomp
        .filter(items = ['filename','index_run_start','index_loc','stop_area_phase'])
    )
    
    # %% CREATE NON-STOP AREA DECOMP
    # TODO: do i still have NaN fps_next3 values? if so this will probably bust up
    # also feels like this 999999 max thing is dodgy
    rawnav = (
        # this was our last table that had all records, so we come back here.
        rawnav
        .assign(
            in_motion_decomp = lambda x, thresh = speed_thresh_fps: 
               pd.cut(
                   x.fps_next3, 
                   bins = 
                       pd.IntervalIndex.from_tuples(
                           data = [(0,thresh),(thresh,999999999)],
                           closed = 'left'
                       )
               )
        )
        .assign(
            in_motion_decomp = lambda x:
                x.in_motion_decomp.cat.rename_categories(['<5 mph','>= 5mph'])
        )
        # a little superfluous, but i'd rather NA these out
        .assign(
            in_motion_decomp = lambda x:
                np.where(
                    pd.isna(x.stop_window_area),
                    x.in_motion_decomp,
                    np.nan
                )
        )
    )
    
    # %% CREATE OVERALL DECOMPOSITION
    rawnav = (
        rawnav
        .merge(
            stop_area_decomp_tojoin,
            how = "left",
            on = ['filename','index_run_start','index_loc']
        )
    )
    
    rawnav = (
        rawnav
        .assign(
            # this is kinda inefficient
            high_level_decomp = lambda x:
                np.select(
                    [
                        pd.notna(x.in_motion_decomp),
                        pd.notna(x.stop_area_phase) & (x.stop_area_phase == "t_stop1"),
                        pd.notna(x.stop_area_phase) & (x.stop_area_phase != "t_stop1")
                    ],
                    [
                        x.in_motion_decomp,
                        "Passenger",
                        "Non-Passenger"
                    ],
                    default = "you shouldnt see this"
                )
        )   
    )
        
    return(rawnav)

def get_stop_ff(
    rawnav,
    method = "mt",
    use_ntile = 0.95
):
        # assert method.isin(['mt','ntile','all'])
        # if method = ntile, then make sure use_ntile is non-nul
        # TODO: assert on possible values of metro transit decomposition method
        # TODO: assert somehow that we have stop windows already assinged

    run_stop_area_speed = (
        rawnav
        .groupby(['filename','index_run_start','stop_window_area'])
        .agg({
            "odom_ft" : ['min','max'],
            "sec_past_st" : ['min','max']
        })
        .pipe(ll.reset_col_names)
        .assign(
            stop_window_fps = lambda x: 
                (x.odom_ft_max - x.odom_ft_min) / 
                    (x.sec_past_st_max - x.sec_past_st_min)
        )
    )

    run_stop_area_all = (
        run_stop_area_speed
        .assign(
            mph = lambda x: x.stop_window_fps / 1.467
        )
        .groupby(['stop_window_area'])
        .agg({
            'mph' : 
                [
                    'min',
                    lambda x: x.quantile(.5),
                    lambda x: x.quantile(.90),
                    lambda x: x.quantile(.95),
                    lambda x: x.quantile(.99),
                    'max'
                ]
        })
        .pipe(ll.reset_col_names)
        .rename(
            columns = {
                "mph_<lambda_0>" : "mph_med",
                "mph_<lambda_1>" : "mph_p90",
                "mph_<lambda_2>" : "mph_p95",
                "mph_<lambda_3>" : "mph_p99"
        })
        .melt(
            id_vars = ['stop_window_area'],
            value_vars = ['mph_min','mph_med','mph_p90','mph_p95','mph_p99','mph_max'],
            var_name = 'ntile',
            value_name = 'mph'
        )
    )   

    if (method == "mt"):
        # note that here we don't separate out passenger delay when calculating the freeflow times
        # per their paper
        # i think we'll also just have to discard first and last stop delay, given the issues we've
        # identified there. That will also help sort out the directionality issue for now
        # TODO: return to address issue of directionality at a stop that might be served by 
        # buses operating in different directions (maybe certain transit center bays)
        run_stop_area_return = (
            run_stop_area_all
            .loc[run_stop_area_all['ntile'] == 'mph_max',]
        )
    elif (method == 'ntile'):
        run_stop_area_return = (
            run_stop_area_all
            .loc[run_stop_area_all['ntile'] == use_ntile,]
        )
    elif (method == "all"):
        run_stop_area_return = run_stop_area_all

    return run_stop_area_return

def decompose_full_mt(
    rawnav,
    stop_ff
):

    # TODO:
    # set the assertions

    rawnav_decompose = (
        rawnav
        .group_by(['filename','index_run_start','stop_window_area','high_level_decomp'])
        .agg(
            {
                'secs_past_st' : [lambda x: max(x) - min(x)],
                'odom_ft' : [lambda x: max(x) - min(x)]
            }
        )
        .pipe(ll.reset_col_names)
        .rename(
            columns = 
                {
                    'odom_ft_<lambda>': 'odom_ft_total',
                    'sec_past_st_<lambda>':'t_segment_total'
                }
        )
    )

    rawnav_decompose_ff = (
        rawnav_decompose
        # join in the freeflow speeds
        .merge(
            stop_ff,
            left_on = ['stop_window_area'],
            right_on = ['stop_window_area']
        )
    )