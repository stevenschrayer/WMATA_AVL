# -*- coding: utf-8 -*-
"""
Created on Tue Aug 17 05:51:43 2021

@author: WylieTimmerman
"""

rawnav_og = rawnav.copy()

rawnav = rawnav_og.copy()

rawnav_stopped_lims = (
    rawnav
    .loc[rawnav.is_stopped]
    .groupby(['filename','index_run_start','stopped_changes'], sort = False)
    .agg(
        min_odom = ('odom_ft','min')#,
        # max_odom = ('odom_ft','max')
    )
)

# find out when you're near a stop
rawnav = (
    rawnav
    .sort_values(by = ["odom_ft",'index_loc'])
    .pipe(
        pd.merge_asof,
        right = rawnav_stopped_lims,
        by = ['filename','index_run_start'],
        left_on = 'odom_ft',
        right_on = 'min_odom',
        direction = 'forward'
    )
    .assign(
        near_stop = lambda x: x.min_odom - x.odom_ft <= 100, 
    )
)

# if your group is all other_delay and all near_stop, convert to is_stopped
# and collapse the current and prior group into one
# rawnav['movement_leak'] = (
#     rawnav
#     .groupby(['filename','index_run_start','stopped_changes'], sort = False)
#     .transform(
#         lambda x: all(x.basic_decomp.eq('other_delay')) & all(x.near_stop.eq(True))    
#     )   
# )

# rawnav = (
#     rawnav
#     .groupby(['filename','index_run_start','stopped_changes'], sort = False)
#     .assign(
#         movement_leak = lambda x: all(x.basic_decomp.eq('other_delay')) & all(x.near_stop.eq(True))    
#     )   
# )


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

rawnav = rawnav.assign(reset_group = lambda x: x.all_other_delay & x.all_near_stop)

# reset group flag
rawnav['reset_group_lead'] = (
    rawnav
    .groupby(['filename','index_run_start'], sort = False)['reset_group']
    .shift(1, fill_value = False)
)

rawnav = (
    rawnav
    .assign(
        stopped_changes_alt = lambda x:
            np.select(
                [
                (~x.reset_group& ~x.reset_group_lead),
                x.reset_group,
                x.reset_group_lead
                ],
                [
                x.stopped_changes,
                x.stopped_changes - 1,
                x.stopped_changes - 2
                ]
            )
    )
)
