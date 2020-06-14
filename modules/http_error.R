bad_request <- function (msg = '400 Bad Request') list (
  status = 400,
  headers = list('Content-Type' = 'text/plain'),
  body = msg
)

unauthorized <- function (msg = '401 Unauthorized') list(
  status = 401,
  headers = list(
    'Content-Type' = 'text/plain',
    # State that we expect a JWT in the authorization header.
    'WWW-Authenticate' = 'Bearer'
  ),
  body = msg
)

not_found <- function (msg = '404 Not Found') list(
  status = 404,
  headers = list('Content-Type' = 'text/plain'),
  body = msg
)

method_not_allowed <- function (msg = '405 Method Not Allowed') list(
  status = 405,
  headers = list('Content-Type' = 'text/plain'),
  body = msg
)

internal_server_error <- function (msg = '500 Internal Server Error') list(
  status = 500,
  headers = list('Content-Type' = 'text/plain'),
  body = msg
)
