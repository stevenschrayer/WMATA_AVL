# -*- coding: utf-8 -*-
"""
Created on Mon Jul 26 08:11:28 2021

@author: WylieTimmerman
"""


testfile = "rawnav07231210220.txt"
testindex = 9877

# 52098 and 3251 are culprits

rawnav_spl = rawnav.copy()

rawnav_spl = (
    rawnav_spl 
    .query('(filename == "rawnav07231210220.txt") & (index_run_start == 9877)')
    .assign(
        sec_past_st_copy = lambda x: pd.to_numeric(x.sec_past_st),
        # pandas only lets you interpolate over repeated vals when this is 
        # datetime, so we do it this way.
        odom_ft_ts = lambda x: pd.to_datetime(x.odom_ft)
    )
    .set_index(['odom_ft_ts'])    
)

def my_interp(x):
    breakpoint()
    y = x.interpolate(method = "index")
    
    return(y)


rawnav_spl['sec_past_st_alt'] = (
    rawnav_spl
    .query('sec_past_st == 3251')
    .groupby((['filename','index_run_start','sec_past_st']))['sec_past_st_copy']
    .apply(lambda x: my_interp(x))   
)