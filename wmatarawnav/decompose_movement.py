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

def decompose_mov(
    rawnav,
    speed_thresh_fps = 7.333,
    max_fps = 130): # this is about the highest i ever saw when expressing on freeway, so yeah.

    # TODO: assert that stop_window_area exists

    #### IDENTIFY PAX VS. NON-PAX STOP AREA
    
    # Here we yank some code from decompose_rawnav.py; it's typically only applied to a filtered set
    # of stop data, but for now, we'll just tack it on.
    # add rolling values
    rawnav = (
        calc_rolling_vals2(rawnav)
    )

    # reset infinite values to NA, i guess. will probably also reset values above max to na
    rawnav = (
        rawnav
        .assign(
            fps_next = lambda x: x.fps_next.replace([np.inf,-np.inf],np.nan),
            fps_next3 = lambda x: x.fps_next3.replace([np.inf,-np.inf],np.nan)
        )
    )
    
    breakpoint()
    
    
    return(rawnav)
        

def decompose_movement(
    rawnav,
    speed_thresh_fps = 7.333,
    max_fps = 130): # this is about the highest i ever saw when expressing on freeway, so yeah.

    # TODO: assert that stop_window_area exists

    #### IDENTIFY PAX VS. NON-PAX STOP AREA
    
    # Here we yank some code from decompose_rawnav.py; it's typically only applied to a filtered set
    # of stop data, but for now, we'll just tack it on.
    
    # add rolling values
    rawnav = (
        dr.calc_rolling_vals(rawnav)
    )
    
    # reset infinite values to NA, i guess. will probably also reset values above max to na
    rawnav = (
        rawnav
        .assign(
            fps_next = lambda x: x.fps_next.replace([np.inf,-np.inf],np.nan),
            fps_next3 = lambda x: x.fps_next3.replace([np.inf,-np.inf],np.nan)
        )
    )
        
    breakpoint()
    
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
    
    #### CREATE NON-STOP AREA DECOMP
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
    
    #### CREATE OVERALL DECOMPOSITION
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
            basic_decomp = lambda x:
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
                    # because we don't have 3 pings to look ahead over,
                    # we get NA speed values at the end. The vehicle could be in motion,
                    # could be stopped, or whatever. We might think about just filling forward
                    # the preceeding values, but this might extent stop windows longer than we want
                    # or otherwise be wrong. I think we just filter this out before analysis
                    # rather than trying to do something more careful here.
                    default = "End of Trip Pings"
                )
        )   
    )
        
    return(rawnav)
    
def calc_rolling_vals2(rawnav,
                      groupvars = ['filename','index_run_start']):
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
    
    # this will cause us to lose some stop windows, but those don't tend to be that useful
    # so we'll just live with it. to avoid losing door_state, which is useful, we'll
    # keep situations where sec_past_st is repeated there. Some of the cleanup code 
    # below should still be useful in such situations
    rawnav = (
        rawnav
        .drop_duplicates(subset = (groupvars + ['sec_past_st','door_state']), keep = 'last')
    )
       
    # so this does a lil error
    # rawnav[['odom_ft_next','sec_past_st_next']] = (
    #     rawnav
    #     .groupby(groupvars, sort = False)[['odom_ft','sec_past_st']]
    #     .shift(-1)
    # )

    # sometimes the above returns that .loc view/copy warning? i'm not sure
    rawnav[['odom_ft_next','sec_past_st_next']] = (
        rawnav
        .groupby(groupvars, sort = False)[['odom_ft','sec_past_st']]
        .transform(lambda x: x.shift(-1))
    )


    # We'll use a bigger lag for more stable values for free flow speed
    rawnav[['odom_ft_next3','sec_past_st_next3']] = (
        rawnav
        .groupby(groupvars, sort = False)[['odom_ft','sec_past_st']]
        .transform(lambda x: x.shift(-3))
    )
    
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
        .groupby(groupvars)[['fps_next','fps_next3']]
        .transform(lambda x: x.ffill())
    )
    
    # but now, if you're the last row or last three rows, we reset you back to 
    # np.nan
    rawnav_add.loc[rawnav_add.groupby(groupvars).tail(1).index, 'fps_next'] = np.nan
    rawnav_add.loc[rawnav_add.groupby(groupvars).tail(3).index, 'fps_next3'] = np.nan
    
    # calculate acceleration
    rawnav_add[['fps_next_lead','fps_next3_lead']] = (
        rawnav_add
        .groupby(groupvars, sort = False)[['fps_next', 'fps_next3']]
        .transform(lambda x: x.shift(-1))

    )
    
    rawnav_add = (
        rawnav_add 
        .assign(
            accel_next = lambda x: (x.fps_next_lead - x.fps_next) / (x.sec_past_st_next - x.sec_past_st),
            accel_next3 = lambda x: (x.fps_next3_lead - x.fps_next3) / (x.sec_past_st_next3 - x.sec_past_st)
        )
        # as before, we'll set these cases to nan and then fill
         .assign(
            accel_next = lambda x: x.accel_next.replace([np.Inf,-np.Inf],np.nan),
            accel_next3 = lambda x: x.accel_next3.replace([np.Inf,-np.Inf],np.nan),
        )
    )
    
    # this is the point where I should've written another function
    rawnav_add[['accel_next','accel_next3']] = (
        rawnav_add
        .groupby(groupvars)[['accel_next','accel_next3']]
        .transform(lambda x: x.ffill())
    )
    
    # but now, if you're the last row or last three rows, we reset you back to 
    # np.nan
    rawnav_add.loc[rawnav_add.groupby(groupvars).tail(1).index, 'accel_next'] = np.nan
    rawnav_add.loc[rawnav_add.groupby(groupvars).tail(3).index, 'accel_next3'] = np.nan
    
    
    return(rawnav_add)
    