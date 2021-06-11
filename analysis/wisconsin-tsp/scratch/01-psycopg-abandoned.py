"""
NOTE: psycopg apparently doesn't play well with azure postgres. was able to connect to server
using both RPostgres and pg8000, but not psycopg. Seems like i'm not the only one who ran into this
https://docs.microsoft.com/en-us/answers/questions/51525/connecting-to-azure-postgresql-server-from-python.html
rolling back wasn't possible because we're on python 3.7
"""
# 1 Import Libraries and Set Global Parameters
########################################################################################################################

# 1.1 Import Python Libraries
############################################
import pandas as pd, os, sys, glob, shutil
import psycopg2 
from dotenv import dotenv_values

if not sys.warnoptions:
    import warnings

    warnings.simplefilter("ignore")  # Stop Pandas warnings

# 1.2 Set Global Parameters
############################################
if os.getlogin() == "WylieTimmerman":
    # Working Paths

    path_working = r"C:\Users\WylieTimmerman\Documents\projects_local\WMATA_AVL_datamart"
    os.chdir(os.path.join(path_working))
    sys.path.append(r"C:\Users\WylieTimmerman\Documents\projects_local\WMATA_AVL_datamart")
    path_sp = r"C:\OD\Foursquare ITP\Foursquare ITP SharePoint Site - Shared Documents\WMATA Datamart Program Support\Task 3 - Bus Priority"
    path_source_data = os.path.join(r"C:\Downloads")
    path_processed_data = os.path.join(path_sp, "Data", "02-Processed") 
    config = dotenv_values(os.path.join(path_working, '.env.alt'))

else:
    raise FileNotFoundError("Define the path_working, path_source_data, gtfs_dir, \
                            ZippedFilesloc, and path_processed_data in a new elif block")
# Globals


# 2 COnnect 
############################################
# this doesn't work for some reason
conn = (
    psycopg2.connect(
        host = config['pg_host'],
        dbname = config['pg_db'],
        user = config['pg_user'],
        password = config['pg_pass'],
        port = config['pg_port'],
        sslmode = config['pg_sslmode']
    )
)

#commenting out port also deosn't work (worreid about string/int thing)
conn = (
    psycopg2.connect(
        host = config['pg_host'],
        dbname = config['pg_db'],
        user = config['pg_user'],
        password = config['pg_pass'],
        # port = config['pg_port'],
        sslmode = config['pg_sslmode']
    )
)

# changing dbname to database didn't work
conn = (
    psycopg2.connect(
        host = config['pg_host'],
        database = config['pg_db'],
        user = config['pg_user'],
        password = config['pg_pass'],
        port = config['pg_port'],
        sslmode = config['pg_sslmode']
    )
)


# this creates newlines adn is probably wrong
conn_string = (
    """host={host} 
    dbname = {dbname} 
    user = {user} 
    password = {password} 
    port = {port} 
    sslmode = {sslmode}"""
    .format(
        host = config['pg_host'],
        dbname = config['pg_db'],
        user = config['pg_user'],
        password = config['pg_pass'],
        port = config['pg_port'],
        sslmode = config['pg_sslmode']
    )
)

# this shows no quotes in the string, btu also doesn't work
conn_string = (
    (
    "host={host} "
    "dbname = {dbname} "
    "user = {user} " 
    "password = {password} "
    "port = {port} " 
    "sslmode = {sslmode}"
    )
    .format(
        host = config['pg_host'],
        dbname = config['pg_db'],
        user = config['pg_user'],
        password = config['pg_pass'],
        port = config['pg_port'],
        sslmode = config['pg_sslmode']
    )
)

# adds quotes, doesn't owkr
conn_string = (
    (
    "host='{host}' "
    "dbname = '{dbname}' "
    "user = '{user}' " 
    "password = '{password}' "
    "port = '{port}' " 
    "sslmode = '{sslmode}'"
    )
    .format(
        host = config['pg_host'],
        dbname = config['pg_db'],
        user = config['pg_user'],
        password = config['pg_pass'],
        port = config['pg_port'],
        sslmode = config['pg_sslmode']
    )
)

conn = psycopg2.connect(conn_string)

# trying without sslmode thing
conn_string = (
    (
    "host='{host}' "
    "dbname = '{dbname}' "
    "user = '{user}' " 
    "password = '{password}' "
    "port = '{port}' " 
    # "sslmode = '{sslmode}'"
    )
    .format(
        host = config['pg_host'],
        dbname = config['pg_db'],
        user = config['pg_user'],
        password = config['pg_pass'],
        port = config['pg_port']#,
        # sslmode = config['pg_sslmode']
    )
)

conn = psycopg2.connect(conn_string)
    
# seems like i can connect locally
conn = (
    psycopg2.connect(
        host = 'localhost',
        dbname = 'gtfslibtest',
        user = 'postgres',
        password = 'Apf^WpJKMZ$0zDq*egJ%xx^7owJgX',
        port = 5432,
        sslmode = 'disable'
    )
)

conn.close()

# what about without quotes in config file
# that didn't seem to matter

# what if i copy connection string frm website
conn_string = "dbname='{your_database}' user='bpadmin@fitp-wmatadatamart-buspriority' host='fitp-wmatadatamart-buspriority.postgres.database.azure.com' password='{your_password}' port='5432' sslmode='require'".format(your_database = config['pg_db'], your_password = 'ePnlTE094Ikx8zm5xUmyFfY')

conn = psycopg2.connect(conn_string)
