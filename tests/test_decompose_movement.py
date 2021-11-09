import pytest
import sys
import os
import pandas as pd

sys.path.append('.')
import wmatarawnav as wr


###############################################################################
# Load in data for testing
@pytest.fixture(scope="session")
def get_cwd():
    if os.getcwd().split('\\')[-1] == 'tests':
        os.chdir('../')
    return os.getcwd()


@pytest.fixture(scope="session")
def get_analysis_route():
    analysis_routes = ['S9']
    return (analysis_routes)


@pytest.fixture(scope="session")
def get_seg():
    seg = 'sixteenth_u_stub'
    return (seg)


@pytest.fixture(scope="session")
def get_pattern_stop():
    pattern_stop = (
        pd.DataFrame(
            {'route': ['S9'],
             'pattern': [2],
             'seg_name_id': ['sixteenth_u_stub'],
             'stop_id': [18042]}
        )
    )
    return (pattern_stop)


@pytest.fixture(scope="session")
def get_rawnav(get_analysis_route, get_cwd):
    rawnav_dat = (
        wr.read_cleaned_rawnav(
            analysis_routes_=get_analysis_route,
            path=os.path.join(
                get_cwd,
                "data",
                "00-raw",
                "demo_data",
                "03_notebook_data",
                "rawnav_data.parquet"
            )
        )
    )

    return (rawnav_dat)


@pytest.fixture(scope="session")
def get_segment_summary(get_cwd, get_seg):
    segment_summary = (
        pq.read_table(
            source=os.path.join(
                get_cwd,
                "data",
                "00-raw",
                "demo_data",
                "03_notebook_data",
                "segment_summary.parquet"
            ),
            use_pandas_metadata=True)
            .to_pandas()
    )

    segment_summary_fil = (
        segment_summary
            .query('~(flag_too_far_any | flag_wrong_order_any | flag_too_long_odom)')
    )

    return (segment_summary_fil)


@pytest.fixture(scope="session")
def get_stop_index(get_cwd, get_analysis_route, get_pattern_stop):
    stop_index = (
        pq.read_table(
            source=os.path.join(
                get_cwd,
                "data",
                "00-raw",
                "demo_data",
                "03_notebook_data",
                "stop_index.parquet"
            ),
            columns=['seg_name_id',
                     'route',
                     'pattern',
                     'stop_id',
                     'filename',
                     'index_run_start',
                     'index_loc',
                     'odom_ft',
                     'sec_past_st',
                     'geo_description']
        )
            .to_pandas()
            .assign(pattern=lambda x: x.pattern.astype('int32'))
            .rename(columns={'odom_ft': 'odom_ft_qj_stop'})
    )

    stop_index_fil = (
        stop_index
            .merge(get_pattern_stop,
                   on=['route', 'pattern', 'stop_id'],
                   how='inner')
    )
    return (stop_index_fil)


@pytest.fixture(scope="session")
def get_ff(get_rawnav, get_segment_summary, get_seg):
    segment_ff = (
        wr.decompose_segment_ff(
            get_rawnav,
            get_segment_summary,
            max_fps=73.3
        )
            .assign(seg_name_id=get_seg)
    )

    return (segment_ff)


@pytest.fixture(scope="session")
def get_stop_area_decomp(get_rawnav, get_segment_summary, get_stop_index, get_seg):
    stop_area_decomp = (
        wr.decompose_stop_area(
            get_rawnav,
            get_segment_summary,
            get_stop_index
        )
            .assign(seg_name_id=get_seg)
    )

    return (stop_area_decomp)


################################### end setup helpers ################################################

def test_reset_odom(get_rawnav):

    test_rawnav = get_rawnav
    print(test_rawnav.head())

    odom_reset = wr.reset_odom(
    test_rawnav,
    indicator_var = "index_loc",
    indicator_val = None,
    reset_vars = ['odom_ft','sec_past_st']
    )
    print("####### ODOM RESET ########")
    print(odom_reset.head())

    assert(odom_reset['odom_ft'] > 1)
    assert(odom_reset['sec_past_st'] > 1)
    assert(test_rawnav is not odom_reset)
    assert(test_rawnav.shape[0] > odom_reset.shape[0])

def test_match_stops():
    # TODO: ensure no stop is being matched twice per ping
    # TODO: check that all stops are found within 100ft

    pass

def test_create_stop_segs():
    pass

def test_trim_ends():
    pass

def test_interp_odom():
    pass

def test_agg_sec():
    pass

def test_interp_over_sec():
    pass

def test_calc_speed():
    pass

def test_accel_jerk():
    pass

def test_savitzky_golay():
    pass

def test_apply_smooth():
    pass

def test_smooth_speed():
    pass

def test_calc_rolling():
    pass

