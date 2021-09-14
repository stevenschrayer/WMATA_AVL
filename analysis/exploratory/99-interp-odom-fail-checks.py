# -*- coding: utf-8 -*-
"""
Created on Mon Aug  2 06:10:30 2021

@author: WylieTimmerman
"""

test_fail_interp = (
    rawnav_fil5
    .groupby(['filename','index_run_start'])
    .filter(
        lambda x: x.odom_interp_fail.any()
    )    
)

test_fail_interp_xtab = (
    rawnav_fil5
    .loc[rawnav_fil5.collapsed_rows.notnull()]
    .groupby(['odom_interp_fail'])
    .agg(
        n_interp = ('index_loc','count')
    )
)

test_fail_interp_miss = (
    rawnav_fil5
    .loc[rawnav_fil5.collapsed_rows.notnull()]
    .assign(
        # values are positive for either of these if outside the threshold
        # no allowances for the +1 gap here
        odom_ft_toolow = lambda x: x.odom_ft_min - x.odom_ft,
        odom_ft_toohi = lambda x: x.odom_ft - x.odom_ft_max
    )
    .assign(
        odom_ft_extra = lambda x: np.where(
            (x.odom_ft_toolow > 0) | (x.odom_ft_toohi > 0),
            np.maximum(x.odom_ft_toolow, x.odom_ft_toohi),
            0
        )
    )
)

test_fail_interp_miss.to_csv('checktheseinterpcases.csv')

test_fail_interp_miss_test = test_fail_interp_miss.head(10000)



test5 = (
    rawnav_fil5
    .head(10000)
)