# -*- coding: utf-8 -*-
"""
Created on Tue Aug  3 06:19:37 2021

@author: WylieTimmerman
"""

testfile = "rawnav07231210220.txt"
testindex = 9877


rawnav_fil6 = (
    rawnav_fil5
     .query("filename == @testfile & index_run_start == @testindex")
     .pipe(
          wr.decompose_mov   
      )
)

rawnav_fil6.to_csv(os.path.join(path_sp,"Data","01-Interim","test_decomp_mov.csv"))


rawnav_fil6 = (
    rawnav_fil5
     .query("filename == @testfile & index_run_start == @testindex")
     .pipe(
          wr.decompose_mov,
          steady_low_thresh = .25
      )
)

rawnav_fil6.to_csv(os.path.join(path_sp,"Data","01-Interim","test_decomp_mov2.csv"))


# using hte new decel condition
rawnav_fil7 = (
    rawnav_fil5
     .query("filename == @testfile & index_run_start == @testindex")
     .pipe(
          wr.decompose_mov,
          steady_low_thresh = .25
      )
)

rawnav_fil7.to_csv(os.path.join(path_sp,"Data","01-Interim","test_decomp_mov3.csv"))
rawnav_fil6.to_csv(os.path.join(path_sp,"Data","01-Interim","test_decomp_mov3_all.csv"))

