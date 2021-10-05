# -*- coding: utf-8 -*-
"""
Created on Tue Oct  5 01:15:23 2021

@author: WylieTimmerman
"""

rawnav_nofirstdoor = (
    rawnav
    .head(100000)
    # remove first door open time for now, will leave in other stopped time around things
    .loc[~ rawnav.stop_decomp.eq("doors_at_O_S")]
)
# stupid pandas can't handle filters upstream in pipes when you need to reference dataframe name
# later in a pipe

# stupid pandas doesn't do agg lambdas using multiple columns cleanly
def wavg(y, df):
    return np.average(y, weights = df.loc[y.index,'secs_marg'])

rawnav_agged_route_ti_speed = (
    rawnav_nofirstdoor
    .groupby(
        [
            'route',
            'route_pattern',
            'pattern', 
            'filename',
            'index_run_start', 
            'id', 
            'way_id'
        ], 
        sort = False
    ) 
    .agg(
        fps_next_sm = ('fps_next_sm', lambda x, df = rawnav_nofirstdoor: wavg(x, df)),
    )
)

# we're kind of running into the QJ related issue again, where some of this non-door open time
# is nonetheless time spent serving the stop. But sometimes there is a little extra traffic delay
# that makes serving that stop slower, so you need to keep track of some sort of minimum time to 
# serve a stop. That would need to be maintained in a separate table and updated regularly
rawnav_agged_route_ti_basic_decomp = (
    rawnav_nofirstdoor
    .groupby(
        [
            'route',
            'route_pattern',
            'pattern', 
            'filename',
            'index_run_start', 
            'id', 
            'way_id',
            'basic_decomp_ext'
        ], 
        sort = False,
        as_index = False
    ) 
    .agg(
        secs_marg = ('secs_marg', 'sum'),
    )
    .pivot(
        index = [
            'route',
            'route_pattern', 
            'pattern', 
            'filename',
            'index_run_start', 
            'id', 
            'way_id'
        ],
        columns = 'basic_decomp_ext',
        values = 'secs_marg'
    )
    .add_suffix('_secs')
    .fillna(0)
    .assign(
        tot_secs = lambda x: x.sum(axis = 1),
        # this is kinda dicey, now have some 0s here. this is all activity not at all related to 
        # doors
        tot_nonstoprel_secs = lambda x: 
            x.accel_nodoors_secs + 
            x.decel_nodoors_secs + 
            x.other_delay_secs +
            x.steady_secs + 
            x.stopped_nodoors_secs
    )
)

rawnav_agged_ti_route = (
    rawnav_agged_route_ti_speed
    .merge(
        rawnav_agged_route_ti_basic_decomp,
        left_index = True,
        right_index = True,
        how = 'outer'
    )
)

rawnav_agged_route = (
    rawnav_agged_ti_route
    # cheating a little, but basically, since pings don't quite occur every second,
    # we will weight values a little more heavily when they have a 'gap' afterwards and those
    # values persist for a while.
    .groupby(['route_pattern', 'pattern', 'id', 'way_id'], sort = False)
    .agg(['mean'])
    .pipe(wr.reset_col_names)
)