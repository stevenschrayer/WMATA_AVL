# -*- coding: utf-8 -*-
"""
Created on Mon Jun 22 10:18 2020
@author: abibeka
"""
# NOTE: To run tests, open terminal, activate environment, change directory
# to the repository, and then run
# pytest tests

# Alternatively, to run tests interactively in ipython, change directory to the repository, then run
# run -m pytest ./tests
# breakpoint() can be set to interrupt execution as needed

import pytest
import os
import sys
import pathlib
import pandas as pd
import geopandas as gpd

sys.path.append('.')

import wmatarawnav as wr

test_data_path = pathlib.Path('tests/test_data')

# Fixme: fix the tests
# TODO: setup simple csvs of test data for each test case, read in, process
# TODO: find out about edge cases to test for
# TODO what can be excluded?
###############################################################################
# Load in data for testing
@pytest.fixture(scope="session")
def get_cwd():
    if os.getcwd().split('\\')[-1] == 'tests':
        os.chdir('../')  # assume accidentally set to script directory
    return os.getcwd()


@pytest.fixture(scope="session")
def get_rawnav_summary_dat(get_cwd):
    path_parquet = os.path.join(
        get_cwd,
        "data",
        "00-raw",
        "demo_data",
        "02_notebook_data")

    rawnav_summary_dat = wr.read_cleaned_rawnav(
        analysis_routes_=["H8"],
        analysis_days_=["Sunday"],
        path=os.path.join(path_parquet, "rawnav_summary_demo.parquet")
    )

    rawnav_summary_dat = rawnav_summary_dat.query('not (run_duration_from_sec < 600 | dist_odom_mi < 2)')

    return rawnav_summary_dat


@pytest.fixture(scope="session")
def get_rawnav_data(get_cwd, get_rawnav_summary_dat):
    path_parquet = os.path.join(
        get_cwd,
        "data",
        "00-raw",
        "demo_data",
        "02_notebook_data")

    rawnav_dat = wr.read_cleaned_rawnav(
        analysis_routes_=["H8"],
        analysis_days_=["Sunday"],
        path=os.path.join(path_parquet, "rawnav_data_demo.parquet")
    )

    rawnav_summary_keys_col = get_rawnav_summary_dat[['filename', 'index_run_start']]

    rawnav_qjump_dat = rawnav_dat.merge(rawnav_summary_keys_col,
                                        on=['filename', 'index_run_start'],
                                        how='right')

    rawnav_qjump_gdf = (
        gpd.GeoDataFrame(
            rawnav_qjump_dat,
            geometry=gpd.points_from_xy(rawnav_qjump_dat.long, rawnav_qjump_dat.lat),
            crs='EPSG:4326'
        )
            .to_crs(epsg=2248)
    )

    return rawnav_qjump_gdf


@pytest.fixture(scope="session")
def get_wmata_schedule_data(get_cwd):
    wmata_schedule_dat = (
        pd.read_csv(
            os.path.join(
                get_cwd,
                "data",
                "00-raw",
                "demo_data",
                "02_notebook_data",
                "wmata_schedule_data_q_jump_routes.csv"
            ),
            index_col=0
        )
            .reset_index(drop=True)
    )

    wmata_schedule_gdf = (
        gpd.GeoDataFrame(
            wmata_schedule_dat,
            geometry=gpd.points_from_xy(wmata_schedule_dat.stop_lon, wmata_schedule_dat.stop_lat),
            crs='EPSG:4326')
            .to_crs(epsg=2248)
    )

    return wmata_schedule_gdf


@pytest.fixture(scope="session")
def get_stops_results(get_rawnav_data, get_rawnav_summary_dat, get_wmata_schedule_data):
    stop_summary, stop_index = (
        wr.merge_rawnav_wmata_schedule(
            analysis_route_=["H8"],
            analysis_day_=["Sunday"],
            rawnav_dat_=get_rawnav_data,
            rawnav_sum_dat_=get_rawnav_summary_dat,
            wmata_schedule_dat_=get_wmata_schedule_data
        )
    )
    return (stop_summary, stop_index)


@pytest.fixture(scope="session")
def get_analysis_route():
    return "H8"


@pytest.fixture(scope="session")
def get_analysis_day():
    return "Sunday"


# Tests
######

def test_stop_order_nearest_point_to_rawnav(get_stops_results):
    stop_summary, stop_index = get_stops_results

    each_stop_seq_more_than_last = all(
        stop_index
        .groupby(['filename', 'index_run_start'])
        .index_loc
        .diff()
        .dropna()
        > 0
    )

    assert (each_stop_seq_more_than_last)


def test_stop_dist_nearest_point_to_rawnav(get_stops_results):
    stop_summary, stop_index = get_stops_results
    each_stop_close_enough = all(stop_index.dist_to_nearest_point <= 100)
    assert (each_stop_close_enough)


def test_nearest_point(get_stops_results):
    # These points were visually verified with diagnostic maps, then baked into this test
    stop_summary, stop_index = get_stops_results

    case_file = "rawnav03236191021.txt"
    case_run = 21537

    nearest_pt = (
        stop_index
            .query('filename == @case_file & index_run_start == @case_run & stop_id == 2368')
    )

    nearest_lat_match = (
        nearest_pt
            .lat
            .pipe(round, 2)
            .eq(38.93)
            .all()
    )

    assert (nearest_lat_match)

    nearest_long_match = (
        nearest_pt
            .long
            .pipe(round, 2)
            .eq(-77.04)
            .all()
    )

    assert (nearest_long_match)


def test_merge_rawnav_wmata_schedule(get_analysis_route, get_analysis_day, get_rawnav_data, get_rawnav_summary_dat, get_wmata_schedule_data):
    analysis_route_ = get_analysis_route
    analysis_day_ = get_analysis_day
    rawnav_dat_ = get_rawnav_data
    rawnav_sum_dat_ = get_rawnav_summary_dat
    wmata_schedule_dat_ = get_wmata_schedule_data

    schedule_sum, nearest_stops = wr.merge_rawnav_wmata_schedule(analysis_route_,
                                   analysis_day_,
                                   rawnav_dat_,
                                   rawnav_sum_dat_,
                                   wmata_schedule_dat_)

    assert schedule_sum is not None, "merge totally failed, merged file is None"
    assert isinstance(schedule_sum, pd.DataFrame)
    assert isinstance(nearest_stops, gpd.GeoDataFrame)


def test_add_num_missing_stops_to_sum():
    pass


def test_merge_rawnav_target(get_wmata_schedule_data):
    """ TODO: make test not quite brittle but also pretty exacting
    - all dist is not null?
    """
    mock_stops = gpd.GeoDataFrame(get_wmata_schedule_data)
    mock_stops.crs = 2248
    mock_rawnav_data = gpd.read_file(test_data_path / 'test_rawnav_target.csv') # need to generate fake reports with some extraneous
    mock_rawnav_data.crs = 2248
    merged_data = merge_rawnav_taget(mock_stops, mock_rawnav_data)
    assert (merged_data.shape[0] == 20)
    assert merged_data.columns[0:2] == ['filename','index_run_start','index_loc']
    assert merged_data.shape[0] < mock_rawnav_data.shape[0], "Every ping matched to a stop, not grouping/filtering"


def test_remove_stops_with_dist_over_100ft():
    mock_stops_with_dist = gpd.read_file(test_data_path / 'stop_data_for_dist_test.csv')
    mock_stops_with_dist.crs = 2248
    mock_stops_lte_100 = mock_stops_with_dist[mock_stops_with_dist['dist_to_nearest_point'] < 100]
    filtered_100ft = wr.remove_stops_with_dist_over_100ft(mock_stops_with_dist)
    max_distance_filtered = filtered_100ft['dist_to_nearest_point'].max()

    assert filtered_100ft.shape[0] == mock_stops_lte_100.shape[0], "100ft filtering to different value"
    assert max_distance_filtered < 100, f"maximum distance filtered is incorrect, max is {max_distance_filtered}"


def test_delete_rows_with_incorrect_stop_order():
    mock_stops_with_sequence = pd.read_csv(test_data_path / 'mock_stops_for_seq_test.csv')
    filtered_seq = wr.delete_rows_with_incorrect_stop_order(mock_stops_with_sequence)
    assert (filtered_seq.shape[0] == 48)  # magic number of stops with correct sequence based on using .diff() on index_loc
    assert filtered_seq['index_loc'].value_counts().max() == 1, "one or more stops indexed twice"


def test_include_wmata_schedule_based_summary():
    pass


def test_get_first_last_stop_rawnav():
    # TODO: test multiple trips?
    mock_located_stops = pd.read_csv(test_data_path / 'mock_stops_for_first_last.csv')
    mock_first_stop = mock_located_stops['index_loc'].iloc[0] # first stop in CSV of a single trip
    mock_last_stop = mock_located_stops['index_loc'].iloc[-1]  # last stop in CSV of a single trip
    first_last_stops = wr.get_first_last_stop_rawnav(mock_located_stops)

    assert first_last_stops.shape[0] == 1, "Multiple trips matched, only one trip should be detected"
    assert first_last_stops.index_loc_first_stop == mock_first_stop, "Issue flagging first stop"
    assert first_last_stops.index_loc_last_stop == mock_last_stop, "Issue flagging last stop"
    assert first_last_stops.index_loc_last_stop != first_last_stops.index_loc_first_stop, "First and last stop match"


def test_make_target_rawnav_linestring():
    mock_rawnav_data = gpd.read_file(test_data_path / 'mock_stops_for_first_last.csv')
    mock_rawnav_data.crs = 2248
    gpd_linestring = make_target_rawnav_linestring(mock_rawnav_data)
    assert (gpd_linestring.shape[0] == gpd_linestring['geometry'].contains('LineString').shape[0], \
            "Not all data is converted to LineString")
    assert mock_rawnav_data.shape[0] == gpd_linestring.shape[0], "Data being removed when converted to LineString"
