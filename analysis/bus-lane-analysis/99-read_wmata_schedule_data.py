# -*- coding: utf-8 -*-
"""
Created by: abibeka, edited by rlesniak@wmata.com
Purpose: Read WMATA schedule data; Schedule_082719-201718.mdb
"""
# https://stackoverflow.com/questions/39835770/read-data-from-pyodbc-to-pandas
import pandas as pd, os, inflection, numpy as np

mdb_to_excel_file_loc = r'C:\Users\e043868\Documents\RawNav'

stop_file = os.path.join(mdb_to_excel_file_loc, '082719-201718 Stop.xlsx')
stop_dat = pd.read_excel(stop_file)
stop_dat = stop_dat.dropna(axis=1)
stop_dat.columns = [inflection.underscore(col_nm) for col_nm in stop_dat.columns]
stop_dat.rename(columns = {'longitude':'stop_lon', 
                           'latitude':'stop_lat',
                           'heading':'stop_heading'}, inplace = True)

pattern_file = os.path.join(mdb_to_excel_file_loc, '082719-201718 Pattern.xlsx')
pattern_dat = pd.read_excel(pattern_file)
pattern_dat = pattern_dat[['PatternID','TARoute','PatternName','Direction',
                           'Distance','CDRoute','CDVariation','PatternDestination',
                           'RouteText','RouteKey','PatternDestination2','RouteText2',
                           'Direction2','PatternName2','TARoute2','PubRouteDir','PatternNotes',
                           'DirectionID']]
#pattern_dat = pattern_dat.dropna(axis=1)
pattern_dat.columns = [inflection.underscore(col_nm) for col_nm in pattern_dat.columns]
pattern_dat.rename(columns={'distance':'trip_length',
                            'cd_route':'route',
                            'cd_variation':'pattern'},inplace=True)

pattern_detail_file = os.path.join(mdb_to_excel_file_loc, '082719-201718 PatternDetail.xlsx')
pattern_detail_dat = pd.read_excel(pattern_detail_file)
pattern_detail_dat = pattern_detail_dat.dropna(axis=1)
pattern_detail_dat = pattern_detail_dat.drop(columns=['SortOrder', 'GeoPathID'])
pattern_detail_dat.columns = [inflection.underscore(col_nm) for col_nm in pattern_detail_dat.columns]
pattern_detail_dat.rename(columns={'distance':'dist_from_previous_stop'},inplace=True)

q_jump_route_list = ['S1']
pattern_q_jump_route_dat = pattern_dat.query('route in @q_jump_route_list')
set(pattern_q_jump_route_dat.route.unique()) - set(q_jump_route_list)

pattern_pattern_detail_stop_q_jump_route_dat = \
    pattern_q_jump_route_dat.merge(pattern_detail_dat,on='pattern_id',how='left')\
    .merge(stop_dat,on='geo_id',how='left')

pattern_pattern_detail_stop_q_jump_route_dat.\
    sort_values(by=['route','pattern','order'],inplace=True)

mask_nan_latlong = pattern_pattern_detail_stop_q_jump_route_dat[['stop_lat', 'stop_lon']].isna().all(axis=1)
assert_stop_sort_order_zero_has_nan_latlong = \
    sum(pattern_pattern_detail_stop_q_jump_route_dat[mask_nan_latlong].stop_sort_order-0)
assert(assert_stop_sort_order_zero_has_nan_latlong==0)

no_nan_pattern_pattern_detail_stop_q_jump_route_dat =\
    pattern_pattern_detail_stop_q_jump_route_dat[~mask_nan_latlong]

no_nan_pattern_pattern_detail_stop_q_jump_route_dat = \
    no_nan_pattern_pattern_detail_stop_q_jump_route_dat.dropna(axis=1)

assert(0== sum(~ no_nan_pattern_pattern_detail_stop_q_jump_route_dat.
               eval('''direction==pub_route_dir& route==ta_route''')))
no_nan_pattern_pattern_detail_stop_q_jump_route_dat.drop(columns=['pub_route_dir','ta_route'],inplace=True)

assert(0== np.sum(no_nan_pattern_pattern_detail_stop_q_jump_route_dat.isna().values))

save_file = os.path.join(mdb_to_excel_file_loc,'wmata_schedule_data_q_jump_routes.csv')
no_nan_pattern_pattern_detail_stop_q_jump_route_dat.to_csv(save_file)

no_nan_pattern_pattern_detail_stop_q_jump_route_dat.iloc[0]