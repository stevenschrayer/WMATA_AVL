# -*- coding: utf-8 -*-
"""
Created on Fri Jul 23 08:42:47 2021

@author: WylieTimmerman
"""

test = (
    rawnav_fil3
    .query("door_state == 'O'")        
)


testfile = "rawnav07231210220.txt"
testindex = 9877

testchecoldk = (
    rawnav
    .query("(filename == @testfile) & (index_run_start == @testindex)")
)

testcheckweird = (
    rawnav_add
    .query("(filename == @testfile) & (index_run_start == @testindex)")
    .query('(odom_ft >51000 )& (odom_ft < 54000)')
)

testcheck = (
    rawnav_add
    .query("(filename == @testfile) & (index_run_start == @testindex)")
)

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
