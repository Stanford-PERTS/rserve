# packages: httr, callr

big_pipe <- import_module("big_pipe")$big_pipe
logging <- import_module("logging")
qualtrics <- import_module("qualtrics")
sql <- import_module("sql")

env <- import_module("modules/environment")
handler_util <- import_module("modules/handler_util")

NEPTUNE_ANALYSIS_REPLICA_IP <- ''
# TRITON_ANALYSIS_REPLICA_IP <- ''  # backup-2018
TRITON_ANALYSIS_REPLICA_IP <- ''  # production-01-analysis-replica
MYSQL_USER <- 'readonly'
# trition active version: '',  mysql_user = "readonly" (this is the default)
# triton old verion: '', mysql_user = "root"


# todo:
# the conditionally source big_pipe from it's location in common or locally
# have the docker build copy it in locally
# use it to download neptun data!

# Is there an api where I can use a service account to connect to Cloud SQL?

# This is currently working by putting the entire credentials json in the
# request from "neptune".
# {
#   "big_pipe_credentials": {
#     "type": "service_account",
#     "project_id": "",
#     "private_key_id": "",
#     "private_key": "",
#     "client_email": "",
#     "client_id": "",
#     "auth_uri": "https://accounts.google.com/o/oauth2/auth",
#     "token_uri": "https://accounts.google.com/o/oauth2/token",
#     "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
#     "client_x509_cert_url": ""
#   },
#   "neptune_sql_credentials": {
#     "ca": "",
#     "cert": "",
#     "key": ""
#   },
#   "triton_sql_credentials": {
#     "ca": "",
#     "cert": "",
#     "key": ""
#   },
#   "reporting_units": [
#     {
#       "id": "ProjectCohort_001",
#       "post_url": "https://neptune.perts.net/api/reports/ProjectCohort_001"
#     },
#     {}
#   ]
# }

# create dedicated functions for downloading sql tables as separate processes
import_from_neptune <- function (sql, ip, credentials, password, mysql_user, db_name) {
  # Running via callr, so can't reference _any_ external variables.
  print("import_data$import_from_neptune()")
  nep_conn <- sql$connect(
    ip,
    dbname = db_name,
    ssl_credentials = credentials,
    password = password,
    mysql_user = mysql_user
  )
  tables <- list()
  tables$Checkpoint <- sql$get_table(nep_conn, "checkpoint")
  # Note: this line below is not ideal bc it is EP-specific.
  tables$Participant <- sql$query(nep_conn, "SELECT * FROM participant WHERE organization_id = 'triton' OR organization_id LIKE BINARY 'Team_%'")
  sql$disconnect(nep_conn)
  return(tables)
}

import_from_triton <- function (sql, ip, credentials, password, mysql_user, db_name) {
  print("import_data$import_from_triton()")
  tri_conn <- sql$connect(
    ip,
    dbname = db_name,
    ssl_credentials = credentials,
    password = password,
    mysql_user = mysql_user
  )
  tables <- list()
  tables$Program <- sql$get_table(tri_conn, "program")
  tables$Organization <- sql$get_table(tri_conn, "organization")
  tables$Team <- sql$get_table(tri_conn, "team")
  tables$Classroom <- sql$get_table(tri_conn, "classroom")
  tables$User <- sql$get_table(tri_conn, "user")
  tables$Report <- sql$get_table(tri_conn, "report")
  tables$Participant <- sql$get_table(tri_conn, "participant")
  tables$Cycle <- sql$get_table(tri_conn, "cycle")
  sql$disconnect(tri_conn)
  return(tables)
}


import_data <- function (
  big_pipe_credentials = NULL,
  neptune_sql_credentials = NULL,
  neptune_sql_ip = NULL,
  neptune_sql_password = NULL,
  neptune_sql_user = NULL,
  neptune_sql_db_name = NULL,
  triton_sql_credentials = NULL,
  triton_sql_ip = NULL,
  triton_sql_password = NULL,
  triton_sql_user = NULL,
  triton_sql_db_name = NULL,
  # if any other parameters are passed from the request that happen to look like
  # credentials (i.e. match handler_util$grepl_credentials()) just ignore them
  ...
) {
  ### <testing code> ###
  ### skip imports from live dbs if using cached data
  # return(list())
  ### </testing code> ###

  logging$info("import_data$import_data()")

  tables <- list(triton = list(), neptune = list())

  # Various scripts may or may not need all these data sources. If credentials
  # aren't provided, assume we didn't need that data source, and skip it.
  if (is.null(big_pipe_credentials)) {
    logging$info("big_pipe_credentials not found, skipping import")
  } else {
    big_pipe_credentials$buckets <- c(
      'neptune-backup-daily-1',
      'neptune-backup-daily-2'
    )
    big_pipe_tables <- big_pipe(
      'neptuneplatform',
      big_pipe_credentials,
      credential_type = 'service_account_key',
      tables = 'ProjectCohort'
    )
    tables$neptune <- c(tables$neptune, big_pipe_tables)
  }
  sql$disconnect_all()


  if (is.null(neptune_sql_credentials)) {
    logging$info("neptune_sql_credentials not found, skipping import")
  } else {
    ip <- ifelse(
      is.null(neptune_sql_ip),
      NEPTUNE_ANALYSIS_REPLICA_IP,
      handler_util$fix_domain_for_docker(neptune_sql_ip)
    )

    # The R packages that allow us to connect to MySQL databases seem to have a
    # serious bug that persists ssl credentials between connections, so the
    # second ssl-based connection always fails. To force each connection to
    # operate in an isolate fashion, call it in an entirely different OS
    # process with callr.
    neptune_tables <- callr::r(
      import_from_neptune,
      args = list(
        sql = sql,  # must be passed in, imports not available in other process
        ip = ip,
        credentials = neptune_sql_credentials,
        password = neptune_sql_password,
        mysql_user = ifelse(
          is.null(neptune_sql_user),
          MYSQL_USER,
          neptune_sql_user
        ),
        db_name = ifelse(
          is.null(neptune_sql_db_name),
          'neptune',
          neptune_sql_db_name
        )
      )
    )
    tables$neptune <- c(tables$neptune, neptune_tables)
  }

  if (is.null(triton_sql_credentials)) {
    logging$info("triton_sql_credentials not found, skipping import")
  } else {
    ip <- ifelse(
      is.null(triton_sql_ip),
      TRITON_ANALYSIS_REPLICA_IP,
      handler_util$fix_domain_for_docker(triton_sql_ip)
    )

    tables$triton <- callr::r(
      import_from_triton,
      args = list(
        sql = sql,
        ip = ip,
        credentials = triton_sql_credentials,
        password = triton_sql_password,
        mysql_user = ifelse(
          is.null(triton_sql_user),
          MYSQL_USER,
          triton_sql_user
        ),
        db_name = ifelse(
          is.null(triton_sql_db_name),
          'triton',
          triton_sql_db_name
        )
      )
    )
    logging$info("The following sql tables are present:")
    logging$info(lapply(tables,names))
  }

  return(tables)
}
