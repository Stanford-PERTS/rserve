# Utilities for RServe request handlers.

# packages: modules, httr, jsonlite, readr, dplyr
modules::import("dplyr", `%>%`)

# Export everything that doesn't start with a dot, i.e. things starting with
# a dot are private to this module.
modules::export("^[^.]")

http_helpers <- import_module("http_helpers")
logging <- import_module("logging")
perts_ids <- import_module("perts_ids")

jwt <- import_module("modules/jwt")
env <- import_module("modules/environment")
http_error <- import_module("modules/http_error")


NEPTUNE_PUBLIC_KEY <- readr::read_file('./neptune_public_key.pem')
DEFAULT_PUBLIC_KEY <- readr::read_file('./default_public_key.pem')


log_call <- function (request, response) {
  # Attempt to follow Apache log format, although we don't seem to have access
  # to the true protocol (HTTP 1.0? 1.1? 2?).
  # See https://httpd.apache.org/docs/1.3/logs.html#common
  cat(paste0(
    request$SERVER_NAME, ':', request$SERVER_PORT,
    ' - - ',
    '[', format(Sys.time(), '%d/%b/%Y:%X %z'), '] ',
    '"', request$REQUEST_METHOD, " ", request$PATH_INFO, request$QUERY_STRING,
    ' HTTP/1.0" ',
    response$status, " ", nchar(response$body),
    "\n"
  ))
}

log_response_body <- function (response) cat(
  paste0(
    ifelse(
      nchar(response$body) > 50,
      paste0(substr(response$body, 1, 50), "..."),
      response$body
    ),
    "\n"
  )
)

get_body <- function (request) {
  request$rook.input$rewind()
  request$rook.input$read_lines()
}

get_json_params <- function (request, simplifyDataFrame = FALSE) {
  body <- get_body(request)
  if (length(body) == 0 || body == '') {
    return(list())
  }
  jsonlite::fromJSON(get_body(request), simplifyDataFrame = simplifyDataFrame)
}

json_response <- function (x) list(
  headers = list('Content-Type' = 'application/json'),
  body = jsonlite::toJSON(x, auto_unbox = TRUE)
)

grepl_credentials <- function (params) {
    grepl('(_credentials|_sql_)', names(params))
}

testing_auth_header <- 'Bearer testing'

jwt_auth <- function (request) {

  result <- list(jwt_payload = list(), error = NULL)

  # Allow tests to bypass auth with a special value.
  # In the future, we'll only allow the testing_auth_header to pass when running
  # the testthat suite, but for now let it pass all the time so we can develop quickly.
  if (
    (env$is_testing() || env$is_localhost())&&
    'HTTP_AUTHORIZATION' %in% names(request) &&
    request$HTTP_AUTHORIZATION == testing_auth_header
  ) {
    return(result)
  }

  if (!'HTTP_AUTHORIZATION' %in% names(request)) {
    result$error <- http_error$unauthorized("JWT missing")
    return(result)
  }
  token <- substring(request$HTTP_AUTHORIZATION, 8)

  # NOTE: If manually uploading reports to Copilot,
  # comment out the if-block below and replace with:   pubkey <- NEPTUNE_PUBLIC_KEY
  if (env$is_deployed()) {
    logging$info("Deployed, using production public key for jwt.")
    pubkey <- NEPTUNE_PUBLIC_KEY
  } else {
    logging$warning("Not deployed, using default public key for jwt.")
    pubkey <- DEFAULT_PUBLIC_KEY
  }
  # pubkey <- NEPTUNE_PUBLIC_KEY

  jwt_result <- jwt$decode(token, pubkey = pubkey)

  if (!is.null(jwt_result$error)) {
    result$error <- http_error$unauthorized(jwt_result$error)
  }
  result$jwt_payload <- jwt_result$payload
  return(result)
}

fix_domain_for_docker <- function(url) {
  if (env$is_localhost() && env$is_docker()) {
    # This is a weird case where the internal network of the docker container
    # is mapped to the network of the host system via a specific ip address.
    # See https://docs.docker.com/engine/reference/run/#network-bridge
    # The quick way to do this is use the special DNS name docker creates.
    # https://stackoverflow.com/questions/24319662/from-inside-of-a-docker-container-how-do-i-connect-to-the-localhost-of-the-mach#24326540
    url <- gsub('(localhost|127\\.0\\.0\\.1)', 'host.docker.internal', url)
    print(paste0('...rewrote local address for internal bridge: ', url))
  }
  return(url)
}

get_response_body <- http_helpers$get_response_body

single_platform_post <- function (url, auth_header, data) {
  logging$info("handler_util$single_platform_post(): POST ", url)
  url <- fix_domain_for_docker(url)
  response <- httr::POST(
    url,
    httr::add_headers(Authorization = auth_header),
    encode = 'json',
    body = data
  )

  if (response$status_code >= 300) {
    stop(paste0("Unexpected reponse: ", httr::content(response)))
  }

  content <- get_response_body(response)

  logging$info(
    "...received: ",
    response$status_code,
    paste(utils::head(content, n = 100L))
  )

  return(content)
}

retry <- http_helpers$retry

post_report_data <- function (report_date,
                              auth_header,
                              reporting_unit,
                              neptune_report_template,
                              report_data) {
  # POST a dataset to Neptune.
  dataset_payload = list(
    content_type = 'application/json', # either application/json or text/csv
    filename = report_data$filename, # any filename, eg. 'my_report.html'
    data = report_data # csv or json text as a unitary character vector
  )
  dataset <- post_to_platform(
    # Any appropriate endpoint on triton or neptune, should be specified in
    # the initial request.
    reporting_unit$post_url,
    auth_header, # RSA-encrypted jwt, signed by platform's private key.
    dataset_payload
  )

  # Customize the template for organizations.
  if (perts_ids$get_kind(report_data$id) == 'Organization') {
    # Organization/Community reports use a different template.
    template <- paste0(neptune_report_template, '_organization')
  } else {
    template <- neptune_report_template
  }

  # POST a reference to this dataset to a report on Triton/Copilot.
  report_payload = list(
    dataset_id = dataset$uid,
    filename = report_data$filename, # current convention is "YYYY-MM-DD.html"
    id = reporting_unit$id,
    issue_date = report_date,
    template = template
  )
  report <- post_to_platform(
    reporting_unit$post_report_url,
    auth_header,
    add_report_payload_ids(report_payload)
  )

  logging$info("Successfully posted report:", report$uid)
}

add_report_payload_ids <- function(payload) {
  # Customize associated ids based on report type.
  report_kind <- perts_ids$get_kind(payload$id)
  if (report_kind == 'Team') {
    payload$team_id <- payload$id
    # Team reports and Classroom reports use the same template.
  } else if (report_kind == 'Classroom') {
    payload$classroom_id <- payload$id
    # Team reports and Classroom reports use the same template.
  } else if (report_kind == 'Organization') {
    payload$organization_id <- payload$id
  }
  return(payload)
}

post_to_platform <- function (url, auth_header, data, tries = 5) {
  response <- retry(
    function () single_platform_post(url, auth_header, data),
    tries
  )
  return(response)
}

fix_open_response_flag <- function (report_data, survey_label) {
  # In the future, we'll base `use_open_responses` on whether any open
  # responses were present in the saturn meta data, but we're not that
  # organized at the moment. The CSET program is known to not involve open
  # responses at all, while BELE-SET (Copilot-Elevate) does.
  if (survey_label == 'beleset19') {
    use_open_responses <- TRUE
  } else if (survey_label == 'cset19') {
    use_open_responses <- FALSE
  } else {
    stop(
      "Must specify whether this survey is supposed to display open " %+%
      "responses on reports"
    )
  }
  report_data$use_open_responses <- use_open_responses
  return(report_data)
}

default_email_recipients <- c(
  '',
  '',
  '',
  '',
  ''
)
#default_email_recipients <- c("") # use for testing
send_email <- function (api_key, to, subject, body_text, force = FALSE) {
  sig <- '
```
          ,     ,
         (\\____/)
          (_oo_)
            (O)
          __||__    \\)
       []/______\\[] /
       / \\______/ \\/
      /    /__\
     (\\   /____\
```
'
  # This structure has to become the JSON text: [{"email": ...}, ...]
  formatted_to <- list()
  for (x in 1:length(to)) {
    address <- to[[x]]
    formatted_to[[x]] <- list(email = address, type = 'to')
  }

  post_body <- list(
    key = api_key,
    message = list(
      # html = 'foo',
      text = paste0(body_text, "\n\n", sig),
      subject = subject,
      from_email = '',
      from_name = 'RServe',
      inline_css = TRUE,
      to = formatted_to
    )
  )

  if (env$is_localhost() && !force) {
    logging$info(
      "NOT sending via Mandrill on localhost. Use force = TRUE to override."
    )
    logging$info("to:", to)
    logging$info("subject:", subject)
    logging$info("body_text:", body_text)
    return()
  }

  response <- httr::POST(
    'https://mandrillapp.com/api/1.0/messages/send.json',
    httr::add_headers('Cotent-Type' = 'application/x-www-form-urlencoded'),
    encode = 'json',
    body = post_body
  )

  content = get_response_body(response)
  if (response$status_code != 200) {
    logging$error("Non-200 response from Mandrill, email may not be sent.")
  }
  logging$info("Mandrill response status: ", response$status_code)
  logging$info(content)

  return(content)
}

.emailer <- function (to, subject, body_text, ...) {
  logging$info(paste0(
    "No email credential provided, just logging instead.\nto:      ",
    to,
    "\nsubject: ",
    subject,
    "\nbody:    ",
    body_text
  ))
}

get_emailer <- function (request) {
  params <- get_json_params(request)
  if ('mandrill_api_key' %in% names(params)) {
    api_key <- params$mandrill_api_key
    return(function (...) send_email(api_key, ...))
  } else {
    return(.emailer)
  }
}

processing_load_test <- function(size, iterations, increase_factor = 1, window_length = 5) {
  # runs lm on a simulated data and reports time passed
  # if increase_factor is not 1, the data frames gradually increases with each iteration
  start_time = Sys.time()
  for (i in 1:iterations){
    size = size*increase_factor
    data <- data.frame(
      y = stats::runif(size),
      x1 = stats::runif(size),
      x2 = stats::runif(size),
      x3 = stats::runif(size),
      x4 = stats::runif(size),
      x5 = stats::runif(size)
    )

    fit <- stats::lm(y ~ x1*x2*x3*x4*x5, data = data)

    if (i %% window_length == 0) {
      end_time <- Sys.time()
      passed_time = (end_time - start_time) %>% round(.,2)
      msg <- paste0(
        "iteration ",
        i,
        " time passed: ",
        passed_time,
        ". Size of data frame: ",
        format(utils::object.size(data), units = "Kb"),
        ", Total memory used: ",
        round(pryr::mem_used() / (1e6),1),
        "Mb ."
      )
      logging$info("####### LOAD TESTING ########: ",msg)
      # you might want to print other system level information, such as free memory or
      # processor load
      start_time <- Sys.time()
    }
  }
  return("Test passed!")
}

compare_dependencies_with_installed <- function (deps) {
  # Use this to figure out what a docker image comes with, to optimize package
  # installation times. For example, there's no reason to spend a long time
  # installing dplyr 0.8.4 from source (b/c it's in package.json as a
  # dependency) when you can get away with 0.8.3 and it comes pre-installed with
  # the image.
  for (package in names(deps)) {
    installed_version <- '(not installed)'
    tryCatch({
      installed_version <- utils::packageVersion(package)
    }, error = function (...) { })
    print(package)
    print(paste0('Requested: ', deps[[package]]))
    print(paste0('Installed: ', installed_version))
    print('--------------------')
  }
}
