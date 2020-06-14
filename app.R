logging <- import_module("logging")
regex <- import_module("regex")

env <- import_module("modules/environment")
handler_util <- import_module("modules/handler_util")
http_error <- import_module("modules/http_error")

handlers <- import_module("handlers")

routes <- list(
  # App Engine requires that instances respond to this endpoint with a 200. The
  # default response will be sufficient.
  '/_ah/health' = list(
    GET = function (request) list()
  ),
  '/api/daily' = handlers$daily_handler,
  '/api/load_test' = handlers$load_test,
  '/api/eval_script' = handlers$eval_script,
  '/api/scripts/(\\S+)' = handlers$scripts_handler,
  '/qltr_responses' = handlers$qltr_responses,
  # Just to be friendly.
  '/' = list(
    GET = function (request) list(body = '"Hello world"')
  )
)

router <- function (routes, request) {
  # Try to set up email ability right away, so we can report errors.
  emailer <- handler_util$get_emailer(request)
  if (is.null(emailer)) {
    logging$warning("No email credentials in request, can't report errors.")
  }

  # Pick the right handler for this path and method. Respond with 404s and 405s
  # if the handler isn't found.
  path <- request$PATH_INFO

  routes_match <- unlist(Map(function (p) grepl(p, path), names(routes)))

  if (!any(routes_match)) {
    return(http_error$not_found())
  }

  # Of many possible matching routes, always choose the first one, so the
  # hard-coded order of routes represents a priority.
  route_pattern <- names(routes)[routes_match][[1]]
  path_handler <- routes[[route_pattern]]

  if (!request$REQUEST_METHOD %in% names(path_handler)) {
    return(http_error$method_not_allowed())
  }
  method_handler <- path_handler[[request$REQUEST_METHOD]]

  # Route patterns should match the whole path beginning to end, so we always
  # use the first instance of the match.
  path_args <- regex$extract_groups(route_pattern, path)[[1]]

  # Call the handler and also log a stack trace if there's an error.
  # N.B. httpuv will respond with a 500 if there's an uncaught error.
  # Got the idea for nesting tryCach and withCallingHandlers from:
  # https://cran.r-project.org/web/packages/tryCatchLog/vignettes/tryCatchLog-intro.html#workaround-2-withcallinghandlers-trycatch
  response <- tryCatch(
    withCallingHandlers(
      # Handlers will always receive the request as their first argument, and any
      # capturing groups from the route regex pattern as further arguments.
      do.call(method_handler, c(list(request), path_args)),
      error = function (e) {
        logging$error(e$message)

        if (env$is_deployed()) {
          # Don't bother on localhost, since the send_email function will log
          # this.
          logging$error(utils::tail(sys.calls(), n=9))
        }

        if (!is.null(emailer)) {
          emailer(
            handler_util$default_email_recipients,
            paste0("RServe errror: ", e$message),
            paste0(
              date(),
              ": ",
              e$message
              # DO NOT PUT TRACEBACKS OR SYS.CALLS OUTPUT HERE
              # These often have dumps of the environment, which will contain
              # sensitive credentials, and we don't want these going over email.
            )
          )
        }
      },
      warning = function (w) {
        print(w)
      }
    ),
    error = function (e) {
      stop(e)  # trigger 500 response
    }
  )

  return(response)
}

call <- function (request) {
  response <- router(routes, request)

  # Provide some defaults for the response to make handler code simpler.
  if (is.null(response) || length(response) == 0) {
    response <- list()
  }
  if (!'status' %in% names(response)) {
    response$status <- 200
  }
  if (!'headers' %in% names(response)) {
    response$headers <- list()
  }
  if (!'Content-Type' %in% names(response$headers)) {
    response$headers[['Content-Type']] <- 'text/plain'
  }
  if (!'body' %in% names(response)) {
    response$body <- ''
  }

  # App Engine does a fine job with basic call logging, so don't bother
  # repeating that unless you're debugging locally.
  if (env$is_localhost()) {
    handler_util$log_call(request, response)
  }
  handler_util$log_response_body(response)

  return(response)
}
