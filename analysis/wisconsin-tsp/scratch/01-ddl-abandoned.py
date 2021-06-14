# -*- coding: utf-8 -*-
"""
Created on Fri Jun 11 08:32:08 2021

@author: WylieTimmerman
"""
# this errors
statement = (
    '''
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        "filename" TEXT,
        "index_run_start" DOUBLE PRECISION,
        "index_run_end" DOUBLE PRECISION,
        "index_loc" DOUBLE PRECISION,
        "lat" DOUBLE PRECISION,
        "long" DOUBLE PRECISION,
        "heading" DOUBLE PRECISION,
        "door_state" TEXT,
        "veh_state" TEXT,
        "odom_ft" DOUBLE PRECISION,
        "sec_past_st" DOUBLE PRECISION,
        "sat_cnt" DOUBLE PRECISION,
        "stop_window" TEXT,
        "blank" DOUBLE PRECISION,
        "lat_raw" DOUBLE PRECISION,
        "long_raw" DOUBLE PRECISION,
        "row_before_apc" DOUBLE PRECISION,
        "route_pattern" TEXT,
        "pattern" DOUBLE PRECISION,
        "start_date_time" TIMESTAMPTZ
        PRIMARY KEY (filename, index_run_start)
    )
    '''.format(
        schema = config['pg_schema'],
        table = tablename  # is this confusing because it matches db name? ohopefully note
    )
)

con.run(statement)

# let's try this
statement = (
    "CREATE TABLE IF NOT EXISTS wisconsin_tsp.rawnav (" 
        "filename TEXT, "
        "index_run_start DOUBLE PRECISION," 
        "index_run_end DOUBLE PRECISION, "
        "index_loc DOUBLE PRECISION, "
        "lat DOUBLE PRECISION, "
        "long DOUBLE PRECISION, "
        "heading DOUBLE PRECISION, "
        "door_state TEXT, "
        "veh_state TEXT, "
        "odom_ft DOUBLE PRECISION, "
        "sec_past_st DOUBLE PRECISION, "
        "sat_cnt DOUBLE PRECISION, "
        "stop_window TEXT, "
        "blank DOUBLE PRECISION, "
        "lat_raw DOUBLE PRECISION, "
        "long_raw DOUBLE PRECISION, "
        "row_before_apc DOUBLE PRECISION, "
        "route_pattern TEXT, "
        "pattern DOUBLE PRECISION, "
        "start_date_time TIMESTAMPTZ "
        "PRIMARY KEY (filename, index_run_start) "
    ")"
)

con.run(statement)

