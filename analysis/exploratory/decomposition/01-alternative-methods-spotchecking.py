# -*- coding: utf-8 -*-
"""
Created on Tue Jun  8 03:57:39 2021

@author: WylieTimmerman
"""

test = rawnav_fil2.head(n = 10000)
filetest = "rawnav02626191023.txt"
index_test = 6680

test_ind = (
    rawnav_stop_window_ind2
    .query('filename == @filetest & index_run_start == @index_test')
    )

test_fil = (
    rawnav_fil
    .query('filename == @filetest & index_run_start == @index_test')
    )

test_ind_raw = (
    rawnav_stop_window_ind
    .query('filename == @filetest & index_run_start == @index_test')
    )

test_fil2 = (
    rawnav_fil2
    .query('filename == @filetest & index_run_start == @index_test')
    )

test_fil3 = (
    rawnav_fil3
    .query('filename == @filetest & index_run_start == @index_test')
    )

test_stop_area = (
    stop_area_decomp
    .query('filename == @filetest & index_run_start == @index_test')
    )

test_fil9 = (
    rawnav_fil9
    .query('filename == @filetest & index_run_start == @index_test')
    )

def reset_categories(x,anarray):
    x.categories = anarray
    return(x)

test_fil9alt = (
    test_fil9
    .assign(
        in_motion_decomp2 = lambda x: 
            reset_categories(x.in_motion_decomp,['<5 mph','>= 5mph'])
    )
)

test_fil9alt = (
    test_fil9
    .assign(
        in_motion_decomp2 = lambda x: 
           x.in_motion_decomp.cat.rename_categories(['<5 mph','>= 5mph'])
    )
)
    
    
test_fil10 = (
    rawnav_fil10
    .query('filename == @filetest & index_run_start == @index_test')
    )

    
test_fil11 = (
    rawnav_fil11
    .query('filename == @filetest & index_run_start == @index_test')
    )