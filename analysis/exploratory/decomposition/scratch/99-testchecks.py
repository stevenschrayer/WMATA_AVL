# -*- coding: utf-8 -*-
"""
Created on Fri Jul 23 08:42:47 2021

@author: WylieTimmerman
"""
test = (
    rawnav
    .head(10000)        
)

test_grp = (
    rawnav_grp
    .head(10000)        
)

test_check = (
    rawnav
    .query('(filename == @testfile) & (index_run_start == @testindex)')
)

test_dupe = (
    rawnav_dupe
    .query('(filename == @testfile) & (index_run_start == @testindex)')
)


test.stop_window.unique()


test = (
    rawnav_fil3
    .query("door_state == 'O'")        
)


testfile = "rawnav07231210220.txt"
testindex = 9877
# look at 

testchecoldk = (
    rawnav_fil3
    .query("(filename == @testfile) & (index_run_start == @testindex)")
)

testchecoldk.to_csv("testcheckold.csv")

testcheckweird = (
    rawnav_add
    .query("(filename == @testfile) & (index_run_start == @testindex)")
    .query('(odom_ft >51000 )& (odom_ft < 54000)')
)

testcheck = (
    rawnav_fil4
    .query("(filename == @testfile) & (index_run_start == @testindex)")
)

testcheck.to_csv("testcheckfixed.csv")

# are there cases where the sec_past_st is not incrementing 



test = (
    rawnav_fil3
    .query("door_state == 'O'")        
)


testexport = (
    rawnav_fil4 
    .query('(filename == "rawnav04477210227.txt") & (index_run_start == 19768)')      
)

testexport.to_csv("test_reexport.csv")


testcheck = (
    rawnav_add
    .query("(filename == @testfile) & (index_run_start == @testindex)")
    .query('(odom_ft >51000 )& (odom_ft < 54000)')
)