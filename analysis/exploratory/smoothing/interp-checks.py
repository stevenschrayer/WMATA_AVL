# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %% [markdown]
# # Evaluate Interpolation Methods
# 
# Let's try out different interpolation methods! 

# %%
import os, sys, pandas as pd, numpy as np
from dotenv import dotenv_values

if os.getlogin() == "WylieTimmerman":
    # Working Paths
    # the following needed to run in vscode jupyter interpreter
    os.environ["GDAL_DATA"] = os.environ["CONDA_PREFIX"] + "\Library\share\gdal"
    path_working = r"C:\Users\WylieTimmerman\Documents\projects_local\WMATA_AVL_datamart"
    os.chdir(os.path.join(path_working))
    sys.path.append(r"C:\Users\WylieTimmerman\Documents\projects_local\WMATA_AVL_datamart")
    path_sp = r"C:\OD\Foursquare ITP\Projects - WMATA Datamart\Task 3 - Bus Priority"
    path_source_data = os.path.join(path_sp,"data","00-Raw")
    path_processed_data = os.path.join(path_sp, "Data","02-Processed")
    # Server credentials
    config = dotenv_values(os.path.join(path_working, '.env'))
    # other things for wylie's dev environment            

# %% [markdown]
# We'll read in some already decomposed data, since it includes some flags for stop window changes
# that will be useful. The error here comes from a duplicated column that comes from a bad join 
#we didn't address earlier.

# %%
rawnav = (
    pd.read_csv(
        os.path.join(path_sp,"data","01-interim","test_decomp_mov9_all.csv")
    )
    .drop(['index_run_end',
           'lat_raw',
           'long_raw',
           'sat_cnt',
           'odom_interp_fail',
           'odom_low',
           'odom_hi',
           'blank'], 
          axis = "columns"
    )
)


# %%
rawnav.info(verbose = True)

# %% [markdown]
# Let's write some functions that evaluate size of miss

# %%
def calc_fail(rawnav, ft_threshold = 1):

    rawnav = (
        rawnav
        # calculate miss from each side of range
        .assign(
            odom_low = lambda x, ft = ft_threshold :
                (x.odom_ft_min - ft) - x.odom_ft,
            odom_hi  = lambda x, ft = ft_threshold :
                x.odom_ft - (x.odom_ft_max + ft),
        )
        # then keep only the ones where this is positive
        .assign(
            odom_low = lambda x: x.odom_low.clip(0),
            odom_hi = lambda x: x.odom_hi.clip(0)
        )
        .assign(
            odom_miss = lambda x: x[['odom_low','odom_hi']].max(axis = 1)
        )
        .assign(
            odom_interp_fail = lambda x:
                np.where(
                    x.collapsed_rows.gt(1),
                    x.odom_miss.gt(0), 
                    pd.NA
                )
        )
    )

    return(rawnav)

# %% [markdown]
# let's double check on the existing files

# If we set ft_threshold to 1, we should get no errors

# %%
rawnav_check = (
    rawnav
    .pipe(
        calc_fail,
        ft_threshold = 1
    )
)

# %%
rawnav_check.odom_interp_fail.value_counts()

rawnav_check.odom_miss.describe()

# %% 
# If we do have ft_threshold to 0, we should see some misses

# %%
rawnav_check = (
    rawnav
    .pipe(
        calc_fail,
        ft_threshold = 0
    )
)

# %%
rawnav_check.odom_interp_fail.value_counts()

rawnav_check.odom_miss.describe()

# %% [markdown]
# All false, as expected

# %%
del rawnav_check

# %% [markdown]
# Before we try new methods, let's clean up our existing rawnav dataframe a bit by adding a 
# timeseries index and removing the older interpolated odometer figures

# %%
rawnav_test = (
    rawnav
    .assign(timest = lambda x: 
            pd.to_datetime(x.start_date_time)+ pd.to_timedelta(x.sec_past_st, unit = "s"))
    .set_index('timest')
    .assign(
        odom_ft = lambda x: np.where(
            x.collapsed_rows.isna(),
            x.odom_ft,
            np.nan
        )
    )
)

# %% [markdown]
# We'll also define a little interpolation function. Because we've run into some indexing issues 
# before, we'll pack in some extra functionality here

# %%
def interp_odom(x, interp_method = "index"):
    if (x.index.duplicated().any()):
        raise ValueError("sec_past_st shouldn't be duplicated at this point")
    else:
        x.odom_ft = x.odom_ft.interpolate(method = interp_method)
        # breakpoint()
        # x.reset_index(inplace = True)
        return(x)

# %% [markdown]
# ## Linear

# %%

rawnav_lin = (
    rawnav_test
    .groupby(['filename','index_run_start'])
    .apply(lambda x: interp_odom(x, interp_method = "index"))
    .pipe(
        calc_fail,
        ft_threshold = 1
    )
    .reset_index()
)


# %%
rawnav_lin.odom_interp_fail.value_counts()

rawnav_lin.odom_miss.describe()

# %%
# this looks pretty good; mean miss is only .87, but the tail on misses can be a bit big
# the max value is somehow 86 ft!

# %% wow, how do we have a 
rawnav_lin_big_miss = (
    rawnav_lin
    .groupby(['filename','index_run_start'])
    .filter(
        lambda x: x.odom_miss.gt(80).any()
    )
)

