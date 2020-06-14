# Get name of script to run from command line arguments.
args <- commandArgs(trailingOnly = TRUE)
script_path <- args[1]
json_ids <- args[2]
show_logs <- identical(args[3], 'show_logs')

# script_path <- "beleset.R"
# json_ids <- "[\"Team_3an1oqb0auri\",\"Classroom_77eac7b00c64\",\"Classroom_0921114a1984\"]"
# show_logs <- TRUE

ids_to_reporting_units <- function (ids) {
  unname(Map(
    function(id) {
      kind <- stringr::str_split(id, "_")[[1]][1]
      if (kind == 'Organization') {
        ru <- list(
          id = id,
          organization_id = id,
          team_id = NULL,
          classroom_id = NULL,
          post_url = paste0("http://localhost:8080/api/datasets?parent_id=", id),
          post_report_url = "http://localhost:10080/api/reports"
        )
      } else if (kind == 'Team') {
        ru <- list(
          id = id,
          organization_id = NULL,
          team_id = id,
          classroom_id = NULL,
          post_url = paste0("http://localhost:8080/api/datasets?parent_id=", id),
          post_report_url = "http://localhost:10080/api/reports"
        )
      } else if (kind == 'Classroom') {
        ru <- list(
          id = id,
          organization_id = NULL,
          team_id = NULL, # technically should have team id here...
          classroom_id = NULL,
          post_url = paste0("http://localhost:8080/api/datasets?parent_id=", id),
          post_report_url = "http://localhost:10080/api/reports"
        )
      } else {
        stop(paste0("Unknown kind: ", kind))
      }
      return(ru)
    },
    ids
  ))
}

suppressMessages({
  logs <- capture.output({
    # Ensure package dependencies.

    if (!'modules' %in% utils::installed.packages()) {
      utils::install.packages('modules')
    }
    bootstrap <- modules::use("gymnast/R/bootstrap.R")
    bootstrap$install_dependencies(gymnast_base_path = "gymnast")

    # Import the scripts that RServe's request handlers normally would.
    profiler <- import_module("profiler")
    profiler$add_event("import script")
    script <- import_module(paste0("scripts/", script_path))
    profiler$add_event("import services")
    import_data <- import_module("modules/import_data")$import_data
    saturn <- import_module("saturn")

    reporting_unit_ids <- jsonlite::fromJSON(json_ids)

    profiler$add_event('call script$main()')
    output_list <- script$main(
      auth_header = NA,
      platform_data = import_data(
        neptune_sql_credentials = list(),
        neptune_sql_ip = '127.0.0.1',
        neptune_sql_password = 'neptune',
        neptune_sql_user = 'neptune',
        neptune_sql_db_name = 'neptune-test-rserve',
        triton_sql_credentials = list(),
        triton_sql_ip = '127.0.0.1',
        triton_sql_password = 'triton',
        triton_sql_user = 'triton',
        triton_sql_db_name = 'triton-test-rserve'
      ),
      qualtrics_service = NULL,
      # ip and other credentials default to localhost
      saturn_service = saturn$create_service(db_name = 'saturn-test-rserve'),
      sheets_service = NULL,
      emailer = function (...) NULL,
      script_params = list(
        # report_date,  # assumes next monday
        reporting_units = ids_to_reporting_units(reporting_unit_ids)
      ),
      should_post = FALSE
    )
  })
})

report_datasets <- output_list$report_data_list

# Write full dataset, including chart base64 data, to disk for debugging.
profiler$add_event('write datasets to disk')
if (file.exists('test/data')) {
  for (dataset in report_datasets) {
    writeLines(
      jsonlite::toJSON(dataset, auto_unbox = TRUE),
      paste0('test/data/', dataset$id, '.json')
    )
  }
}

# Combine the empty report notes with the report datasets so tests can check
# whether reports were generated or not.
datasets_to_test <- output_list$report_data_list
for (id in names(output_list$empty_reports)) {
  datasets_to_test[[id]] <- output_list$empty_reports[[id]]
}

# Strip base 64 image data before piping through stdout. Jest (JavaScript)
# can't assert on that data anyway.
profiler$add_event('strip charts')
stripped <- "<stripped for brevity>"
for (x in sequence(length(datasets_to_test))) {
  dataset <- datasets_to_test[[x]]
  if ('learning_conditions' %in% names(dataset)) {
    for (y in sequence(length(dataset$learning_conditions))) {
      datasets_to_test[[x]]$learning_conditions[[y]]$bar_chart_64 <- stripped
      datasets_to_test[[x]]$learning_conditions[[y]]$timeline_chart_64 <- stripped
    }
  }
  if ('items' %in% names(dataset) && length(dataset$items) > 0) {
    for (y in sequence(length(dataset$items))) {
      num_plots <- length(datasets_to_test[[x]]$items[[y]]$team_by_week_plots)
      replacement <- I(rep(stripped, num_plots))  # don't unbox single plots
      datasets_to_test[[x]]$items[[y]]$team_by_week_plots <- replacement
    }
  }
}

if (show_logs) {
  # Can print captured logs for debugging; they should appear in jest tests but
  # they will break JSON parsing of the output.
  print(logs)
  profiler$print()
} else {
  # Effectively returns JSON to jest tests, which run this file via Node's `exec`.
  print(jsonlite::toJSON(datasets_to_test, auto_unbox = TRUE))
}

profiler$clear_all()
