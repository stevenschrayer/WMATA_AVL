# NOTE: SQL statements were not really playing well in Python, so we are 
# doing this quickly in R
# sorry for the copied code!
library(DBI)
library(tidyverse)
library(RPostgres)
library(dotenv)
library(arrow)
library(glue)


path_data <-
  "C:/OD/Foursquare ITP/Foursquare ITP SharePoint Site - Shared Documents/WMATA Datamart Program Support/Task 3 - Bus Priority/Data/02-Processed"
load_dot_env(file = ".env")

conn <- 
  DBI::dbConnect(
    RPostgres::Postgres(),
    user = Sys.getenv('pg_user'), 
    password = Sys.getenv('pg_pass'), 
    dbname = Sys.getenv('pg_db'), 
    host = Sys.getenv('pg_host'),
    port = Sys.getenv('pg_port'),
    sslmode = Sys.getenv('pg_sslmode'),
  )

# Rawdata -----------------------------------------------------------------

# we are running out of time, so instead of defining tables in advance,
# we are just pushing the first one and then appending the others after.
filelist <- 
  list.files(path = file.path(path_data,"rawnav_data_202102.parquet"),
             recursive = TRUE,
             full.names = TRUE)

rawnav_data_202102 <- 
  map_dfr(filelist,
          ~ arrow::read_parquet(.x))

dbWriteTable(
  conn,
  name = Id(schema = Sys.getenv('pg_schema'), table = "rawnav"),
  value = rawnav_data_202102,
  overwrite = FALSE, 
  append = TRUE,
  row.names = FALSE
)

# SEt primary key
statement <-
  glue_sql("ALTER TABLE {`schema`}.{`table`} OWNER TO {`groupname`}",
           schema = Sys.getenv("pg_schema"),
           table = "rawnav",
           groupname = "datamart_group",
           .con = conn)

dbExecute(conn, statement)

# Remainder 

thelist <- 
  c(
    "rawnav_data_202103.parquet",
    "rawnav_data_202104.parquet",
    "rawnav_data_202105.parquet"
  )

for (theitem in thelist) {
  filelist <- 
    list.files(path = file.path(path_data,theitem),
               recursive = TRUE,
               full.names = TRUE)
  
  rawnav_data_forup <- 
    map_dfr(filelist,
            ~ arrow::read_parquet(.x))
  
  dbWriteTable(
    conn,
    name = Id(schema = Sys.getenv('pg_schema'), table = "rawnav"),
    value = rawnav_data_forup,
    overwrite = FALSE, 
    append = TRUE,
    row.names = FALSE
  )
}

#update the index
statement <-
  glue_sql('CREATE INDEX {`paste0(`table`,`index`)`} 
           ON {`schema`}.{`table`} ("filename", "index_run_start")',
           schema = Sys.getenv("pg_schema"),
           table = "rawnav",
           index = "fileindex",
           .con = conn)

dbExecute(conn, statement)

statement <-
  glue_sql('CREATE INDEX {`paste0(`table`,`index`)`} 
           ON {`schema`}.{`table`} ("route_pattern")',
           schema = Sys.getenv("pg_schema"),
           table = "rawnav",
           index = "fileindex",
           .con = conn)

dbExecute(conn, statement)


# Summary -----------------------------------------------------------------


filelist <- 
  list.files(path = file.path(path_data,"rawnav_summary_202102.parquet"),
             recursive = TRUE,
             full.names = TRUE)

rawnav_summary_202102 <- 
  map_dfr(filelist,
          ~ arrow::read_parquet(.x))

dbWriteTable(
  conn,
  name = Id(schema = Sys.getenv('pg_schema'), table = "rawnav_summary"),
  value = rawnav_summary_202102,
  overwrite = FALSE, 
  append = TRUE,
  row.names = FALSE
)

statement <-
  glue_sql("ALTER TABLE {`schema`}.{`table`} OWNER TO {`groupname`}",
           schema = Sys.getenv("pg_schema"),
           table = "rawnav_summary",
           groupname = "datamart_group",
           .con = conn)

dbExecute(conn, statement)



thelist <- 
  c(
    "rawnav_summary_202103.parquet",
    "rawnav_summary_202104.parquet",
    "rawnav_summary_202105.parquet"
  )


for (theitem in thelist) {
  filelist <- 
    list.files(path = file.path(path_data,theitem),
               recursive = TRUE,
               full.names = TRUE)
  
  rawnav_data_forup <- 
    map_dfr(filelist,
            ~ arrow::read_parquet(.x))
  
  dbWriteTable(
    conn,
    name = Id(schema = Sys.getenv('pg_schema'), table = "rawnav_summary"),
    value = rawnav_data_forup,
    overwrite = FALSE, 
    append = TRUE,
    row.names = FALSE
  )
}

#update the index
statement <-
  glue_sql('CREATE INDEX {`paste0(`table`,`index`)`} 
           ON {`schema`}.{`table`} ("filename", "index_run_start")',
           schema = Sys.getenv("pg_schema"),
           table = "rawnav_summary",
           index = "fileindex",
           .con = conn)

dbExecute(conn, statement)

statement <-
  glue_sql('CREATE INDEX {`paste0(`table`,`index`)`} 
           ON {`schema`}.{`table`} ("route")',
           schema = Sys.getenv("pg_schema"),
           table = "rawnav_summary",
           index = "fileindex",
           .con = conn)

dbExecute(conn, statement)


dbDisconnect(conn)



# Stop Index --------------------------------------------------------------

filelist <- 
  list.files(path = file.path(path_data,"stop_index.parquet"),
             recursive = TRUE,
             full.names = TRUE)

stop_index <- 
  map_dfr(filelist,
          ~ arrow::read_parquet(.x))

dbWriteTable(
  conn,
  name = Id(schema = Sys.getenv('pg_schema'), table = "stop_index"),
  value = stop_index,
  overwrite = FALSE, 
  append = TRUE,
  row.names = FALSE
)

# Change owner
statement <-
  glue_sql("ALTER TABLE {`schema`}.{`table`} OWNER TO {`groupname`}",
           schema = Sys.getenv("pg_schema"),
           table = "stop_index",
           groupname = "datamart_group",
           .con = conn)

dbExecute(conn, statement)



# Stop Summary --------------------------------------------------------------

filelist <- 
  list.files(path = file.path(path_data,"stop_summary.parquet"),
             recursive = TRUE,
             full.names = TRUE)

stop_summary <- 
  map_dfr(filelist,
          ~ arrow::read_parquet(.x))

dbWriteTable(
  conn,
  name = Id(schema = Sys.getenv('pg_schema'), table = "stop_summary"),
  value = stop_summary,
  overwrite = FALSE, 
  append = TRUE,
  row.names = FALSE
)

# Change owner
statement <-
  glue_sql("ALTER TABLE {`schema`}.{`table`} OWNER TO {`groupname`}",
           schema = Sys.getenv("pg_schema"),
           table = "stop_summary",
           groupname = "datamart_group",
           .con = conn)

dbExecute(conn, statement)



