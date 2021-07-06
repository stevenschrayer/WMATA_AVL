# as an aside, does stop_index have everything

test_stop_index = (
    stop_index
    .assign(mdy = lambda x : x.filename.str.extract(r'^rawnav[0-9]{5}([0-9]{6})\.txt'))
)

test_stop_index.mdy.unique()

# this produces some cases where route and route pattery don't quite match up, i think
test_stop_summary = (
    stop_summary[(stop_summary.route_pattern.str.contains("3301")) & stop_summary.route.str.contains('31')]
)

stop_summary_xtab = (
    stop_summary
    .groupby(['route_pattern','direction_wmata_schedule'])
    .size()
    .reset_index()
)

test = (
    stop_index
    .head()
)

test_index = stop_index[stop_index.index.duplicated()]

# test out the new function

rawnav_wstops = (
    wr.assign_stop_area(
        rawnav,
        stop_field = "stop_window",
        upstream_ft = 150,
        downstream_ft = 150
    )
)