

# Connect to database -----------------------------------------------------

stin_jar <- function() {
  glue::glue(
    "C:\\Users\\{wmata_username}\\OneDrive - WMATA\\R Projects\\ojdbc6.jar",
    wmata_username = Sys.getenv("wmata_username")
  )
}


connect_planapi <- function(jar_path,
                            user = Sys.getenv("plan_api_username"),
                            pass = Sys.getenv("plan_api_password"),
                            JAVA_HOME = 'C:\\Program Files\\Java\\jre1.8.0_251') {
  Sys.setenv(JAVA_HOME = JAVA_HOME)
  # increases the maximum memory allocation pool (in this case, 2 GB)
  options(java.parameters = "-Xmx2g")
  
  rJava::.jinit() #initialize the Java Virtual Machine
  
  jdbcDriver <-
    RJDBC::JDBC(driverClass = "oracle.jdbc.OracleDriver",
         classPath = jar_path)
  
    DBI::dbConnect(jdbcDriver,
              "jdbc:oracle:thin:@//ctx4-scan:1521/NCSDPRD1.wmata.com",
              user,
              pass)
}


# Point dbplyr to Oracle functions ----------------------------------------


#for dbplyr if it's used
sql_translate_env.JDBCConnection <-
  dbplyr:::sql_translate_env.Oracle
sql_select.JDBCConnection <- dbplyr:::sql_select.Oracle
sql_subquery.JDBCConnection <- dbplyr:::sql_subquery.Oracle


# Query Functions ---------------------------------------------------------

to_oracle_date <- function(x) {
  o_year <- year(x) %>%
    str_sub(., 3, 4)
  
  o_month <- month(x, label = TRUE, abbr = TRUE) %>%
    str_to_upper(.)
  
  o_day <- day(x) %>%
    str_pad(., width = 2, side = "left", pad = "0")
  
  glue::glue("{o_day}-{o_month}-{o_year}")
}



# Read in TSP Logs --------------------------------------------------------

read_tsp_log <- function(path) {
  df <-
    tryCatch(
      read_csv(path, col_types = cols(.default = col_character())),
      warning = function(e)
        return(NULL)
    )
  
  if(!is.null(df))  
    df <- df %>% mutate(filename = basename(path))
  return(df)
}
