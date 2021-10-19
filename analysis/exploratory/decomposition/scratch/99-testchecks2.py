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




test4 = (
    rawnav_fil4
    .head(10000)
)


testfile = "rawnav07231210220.txt"
testindex = 9877


            # .query('sec_past_st == 3251')

test6 = (
 rawnav_fil5
 .query("filename == @testfile & index_run_start == @testindex")
 # .to_csv("test_checkfixed6.csv")
 )

    ( rawnav_fil5
     .query("filename == @testfile & index_run_start == @testindex")
     .to_csv(os.path.join(path_sp,"Data","01-Interim","test_checkfixed6_newaccel.csv"))
     )

    ( rawnav_fil5
     .query("filename == @testfile & index_run_start == @testindex")
     .to_csv(os.path.join(path_sp,"Data","01-Interim","test_checkfixed6_newaccel2.csv"))
     )



testcheck4 = (
 rawnav_fil4
 .query("filename == @testfile & index_run_start == @testindex")
 .query('(odom_ft > 13000) & (odom_ft < 15100)')
 )

testcheck3 = (
 rawnav_fil3
 .query("filename == @testfile & index_run_start == @testindex")
 .query('(odom_ft > 13000) & (odom_ft < 15100)')
 )

testcheckfix = (
 rawnav_fix
 .query("filename == @testfile & index_run_start == @testindex")
 .query('(odom_ft > 13000) & (odom_ft < 15100)')
 )


# this one we run into a bunch of issues on the door open/close case
testfile = "rawnav04475210210.txt"
testindex = 4279

    ( rawnav_fil3
     .query("filename == @testfile & index_run_start == @testindex")
     .to_csv(os.path.join(path_sp,"Data","01-Interim","test_broken2.csv"))
     )
    

x.reset_index(inplace = True)

x2 = x[x.duplicated(['sec_past_st'],False)]

# trying a look at that flag i created

test5 = (
    rawnav_fil5
    .head(10000)
)
