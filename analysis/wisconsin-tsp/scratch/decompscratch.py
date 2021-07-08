# -*- coding: utf-8 -*-
"""
Created on Wed Jul  7 05:39:54 2021

@author: WylieTimmerman
"""

test = (
        rawnav_run_decomp_exp
        .head(10000)
        
        )

check_trip = "rawnav04475210202.txt"
check_index = 14528

test_stop_window_ind = (
    rawnav_stop_window_ind
    .query('filename == @check_trip & index_run_start == @check_index')
)

test_stop_window_res = (
    rawnav
    .query('filename == @check_trip & index_run_start == @check_index')
    )