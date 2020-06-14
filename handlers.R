google_sheets <- import_module("google_sheets")
logging <- import_module("logging")
profiler <- import_module("profiler")
qualtrics <- import_module("qualtrics")
saturn <- import_module("saturn")

handler_util <- import_module("modules/handler_util")
http_error <- import_module("modules/http_error")
import_data <- import_module("modules/import_data")$import_data

r_function_handler <- list(
  GET = function (request) list(body = '"Hello rumen"')
  #POST = function (request) {
  #  response <- list(body = "r_function")
  #  params <- handler_util$get_json_params(request)
  #  print(params)
  #  return(response)
  #}
)

qltr_responses <- list(
  POST = function (request) {
    # asks Qualtrics for export_id and initiates the creation of export file
    params <- handler_util$get_json_params(request)
    #response <- list(body = "qltr_api")
    # make sure you don't log sensitive information
    safe_params <- params[!handler_util$grepl_credentials(params)]
    print(safe_params)
    qualtrics_service <- qualtrics$create_service(params$qualtrics_credentials)
    response <- qualtrics_service$get_responses(params$survey_key)
    print ("Dimensionality: ")
    print(dim(response))
    return(handler_util$json_response(response))
  }
)

eval_script <- list(
  # when active, evaluates script and returns the output
  # when inactive, returns "hello world"
  # I will activate it only for testing purposes, normally it will be inactive
  # example call from json request in /api/load_test
  # {
  # "script": "x <- 5\n print(x)"
  #}
  POST = function (request) {
    result <- "Script failed!"
    # comment out the line bellow to activate the function
    return(handler_util$json_response(result))
    params <- handler_util$get_json_params(request)
    print("Running script evaluation with the following parameters:")
    print(params)
    try({
      result <- eval(parse( text=params$script))
    })
    return(handler_util$json_response(result))
  }
)

load_test <- list(
  # example call from json request in /api/load_test
  #{
  #  "size" : 100,
  #  "iterations": 100,
  #  "increase_factor": 1,
  #  "window_length": 10
  #}
  POST = function (request) {
    params <- handler_util$get_json_params(request)
    print("Running load test with the following parameters:")
    print(params)
    result <- "Test did not pass!"
    ram_log <- peakRAM::peakRAM(result <- do.call(handler_util$processing_load_test, params))
    msg <- paste0("Peak RAM used: ",
                  ram_log$Peak_RAM_Used_MiB,
                  "MiB, Total RAM used:",
                  ram_log$Total_RAM_Used_MiB,
                  "MiB.")
    logging$info("####### LOAD TESTING ########: ",msg)
    return(handler_util$json_response(result))
  }
)

# This authenticates, loads data, and calls the specified script.
scripts_handler <- list(
  POST = function (request, script_name) {
    profiler$add_event("START scripts handler")

    # Authenticate.
    auth_result <- handler_util$jwt_auth(request)
    if (!is.null(auth_result$error)) {
      return(auth_result$error)
    }

    # Find the appropriate script to run.
    profiler$add_event('import requested script')
    file_name <- paste0('scripts/', script_name, '.R')
    if (!file.exists(file_name)) {
      return(http_error$not_found())
    }
    script <- import_module(file_name)

    # Process parameters, which is fairly complex.
    params <- handler_util$get_json_params(request)

    # Capture some credentials in closures/services for convenience of
    # script writers.
    q_api_key <- NULL
    if ('qualtrics_credentials' %in% names(params)) {
      q_api_key <- params$qualtrics_credentials$api_key
      params$qualtrics_credentials <- NULL
    }
    qualtrics_service <- qualtrics$create_service(q_api_key)

    saturn_service <- saturn$create_service(params$saturn_sql_credentials)
    params$saturn_sql_credentials <- NULL

    sheets_service <- NULL
    if ('rserve_service_account_credentials' %in% names(params)) {
      sheets_service <- google_sheets$open(
        params$rserve_service_account_credentials,
        credential_type = 'service_account_key'
      )
      # ... many other services could be created here
      params$rserve_service_account_credentials <- NULL
    }

    # Remaining parameters can be split into 1) data import credentials and
    # 2) custom script parameters.
    is_creds <- handler_util$grepl_credentials(params)
    import_params <- params[is_creds]
    script_params <- params[!is_creds]

    # Import data from all sources where credentials are given.
    profiler$add_event('import data')
    platform_data <- do.call(import_data, import_params)

    # Now finally arrange the arguments the script's `main` function will
    # receive. These arguments are standard and always come first.
    script_args <- list(
      auth_header = request$HTTP_AUTHORIZATION,
      platform_data = platform_data,
      qualtrics_service = qualtrics_service,
      saturn_service = saturn_service,
      sheets_service = sheets_service,
      emailer = handler_util$get_emailer(request)
    )

    # Then include any custom arguments (besides credentials) passed in the
    # request.
    script_args$script_params <- script_params

    # Run the script.
    profiler$add_event('call script$main()')
    response <- do.call(script$main, script_args)

    profiler$add_event('return response')
    profiler$print()
    return(handler_util$json_response(response))
  }
)

# This authenticates, loads data (once!), and then loops over all daily scripts.
daily_handler <- list(
  POST = function (request) {
    auth_result <- handler_util$jwt_auth(request)
    if (!is.null(auth_result$error)) {
      return(auth_result$error)
    }

    params <- handler_util$get_json_params(request)

    is_creds <- handler_util$grepl_credentials(params)
    import_params <- params[is_creds]
    script_params <- params[!is_creds]

    platform_data <- do.call(import_data, import_params)

    qualtrics_service <- qualtrics$create_service(params$qualtrics_credentials)

    # These arguments always come first.
    script_args <- c(
      list(
        auth_header = request$HTTP_AUTHORIZATION,
        platform_data = platform_data,
        qualtrics_service = qualtrics_service
      ),
      # Then include any arguments (besides credentials) passed in the request.
      script_params
    )

    all_responses <- c()
    daily_dir <- 'scripts/daily'
    for (file_name in list.files(daily_dir)) {
      script <- import_module(paste0(daily_dir, '/', file_name))
      response <- do.call(script$main, script_args)
      all_responses <- c(all_responses, response)
    }

    return(handler_util$json_response(all_responses))
  }
)
