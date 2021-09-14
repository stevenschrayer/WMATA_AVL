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

test = (
    rawnav_fil6
    .query("filename == @testfile & index_run_start == @testindex")
)

# this is getting rid of steady_fps
(
    rawnav_fil6
    .query("filename == @testfile & index_run_start == @testindex")
    .to_csv(os.path.join(path_sp,"Data","01-Interim","test_decomp_mov4.csv"))
)


(
    rawnav_fil6
     .to_csv(os.path.join(path_sp,"Data","01-Interim","test_decomp_mov4_all.csv"))
)


# this is a quick dump of non-stedy fps method
(
    rawnav_fil6
    .query("filename == @testfile & index_run_start == @testindex")
    .to_csv(os.path.join(path_sp,"Data","01-Interim","test_decomp_mov4.csv"))
)


(
    rawnav_fil6
     .to_csv(os.path.join(path_sp,"Data","01-Interim","test_decomp_mov4_all.csv"))
)



# this is same as above in decomp, but new smoothed values
(
    rawnav_fil6
    .query("filename == @testfile & index_run_start == @testindex")
    .to_csv(os.path.join(path_sp,"Data","01-Interim","test_decomp_mov5.csv"))
)


(
    rawnav_fil6
     .to_csv(os.path.join(path_sp,"Data","01-Interim","test_decomp_mov5_all.csv"))
)


# this updates the smoothed values further
(
    rawnav_fil6
    .query("filename == @testfile & index_run_start == @testindex")
    .to_csv(os.path.join(path_sp,"Data","01-Interim","test_decomp_mov6.csv"))
)


(
    rawnav_fil6
     .to_csv(os.path.join(path_sp,"Data","01-Interim","test_decomp_mov6_all.csv"))
)


# uses even more smoothed accel 10 secs
(
    rawnav_fil6
    .query("filename == @testfile & index_run_start == @testindex")
    .to_csv(os.path.join(path_sp,"Data","01-Interim","test_decomp_mov7.csv"))
)


# uses even more smoothed accel 10 secs, but only applies within window
(
    rawnav_fil6
    .query("filename == @testfile & index_run_start == @testindex")
    .to_csv(os.path.join(path_sp,"Data","01-Interim","test_decomp_mov8.csv"))
)


(
    rawnav_fil6
     .to_csv(os.path.join(path_sp,"Data","01-Interim","test_decomp_mov8_all.csv"))
)

# uses even more smoothed accel 9 secs and adds the stop type indicator
test = (
    rawnav_fil6
    .query("filename == @testfile & index_run_start == @testindex")
)

(
    test
    .to_csv(os.path.join(path_sp,"Data","01-Interim","test_decomp_mov9.csv"))
)




# these update to have the stop area decomp and a few more areas of odom interpolation
(
    rawnav_fil7
    .query('filename == @testfile & index_run_start == @testindex')
    .to_csv(os.path.join(path_sp,"Data","01-Interim","test_decomp_mov10.csv"))
)

(
    rawnav_fil7
    .to_csv(os.path.join(path_sp,"Data","01-Interim","test_decomp_mov10_all.csv"))
)

# after doing smoothed from start to finish, new accel calcs
rawnav_fil8.to_csv(os.path.join(path_sp,"Data","01-Interim","test_decomp_mov11.csv"))

rawnav_fil8.to_csv(os.path.join(path_sp,"Data","01-Interim","test_decomp_mov11_all.csv"))

rawnav_fil11.to_csv(os.path.join(path_sp,"Data","01-Interim","test_decomp_mov12_all.csv"))

testfile = "rawnav06491210217.txt"
testindex = 13725

testcase = (
    rawnav_fil10
    .query('filename == @testfile & index_run_start == @testindex')
    .set_index(keys = ['index_loc'])
)

testfile = 'rawnav04476210210.txt'
testindex = 23315

testcase = (
    rawnav_fil10
    .query('filename == @testfile & index_run_start == @testindex')
    .set_index(keys = ['index_loc'])
)

test9 = (
    rawnav_fil8
    .query('filename == @testfile & index_run_start == @testindex')
    .pipe(
        wr.match_stops,
        stop_index
    )
)


testfile = "rawnav06168210225.txt"
testindex = 12267

testcase = (
    rawnav_fil10
    .query('filename == @testfile & index_run_start == @testindex')
    .set_index(keys = ['index_loc'])
)

rawnav_fil11.to_csv(os.path.join(path_sp,"Data","01-Interim","test_decomp_mov15_all.csv"))
    


rawnav_fil11.to_csv(os.path.join(path_sp,"Data","01-Interim","test_decomp_mov16_all.csv"))
    
