# Core file for RServe. Initializes the server and imports other scripts.

# N.B. .Rprofile puts `import_module` in global environment.
bootstrap <- import_module("bootstrap")
bootstrap$install_dependencies(gymnast_base_path = "gymnast")

app <- import_module("app")

# When running locally you may specify a port number.
args <- commandArgs(trailingOnly = TRUE)

PORT <- ifelse(length(args) == 0, 8080, as.numeric(args[1]))
INTERFACE <- "0.0.0.0"  # listen to all traffic, not just the loopback device

print(paste0("Server listening to ", INTERFACE, ":", PORT))
httpuv::runServer(INTERFACE, PORT, app)

# Typical available names of request (additional headers prefixed with HTTP_ may
# be availble also, e.g. HTTP_AUTHORIZATION):
#  [1] "HTTP_CACHE_CONTROL"             "HTTP_CONNECTION"
#  [3] "HTTP_UPGRADE_INSECURE_REQUESTS" "HTTP_ACCEPT"
#  [5] "HTTP_ACCEPT_LANGUAGE"           "QUERY_STRING"
#  [7] "httpuv.version"                 "SERVER_NAME"
#  [9] "SCRIPT_NAME"                    "SERVER_PORT"
# [11] "REMOTE_PORT"                    "rook.input"
# [13] "PATH_INFO"                      "rook.version"
# [15] "rook.errors"                    "REMOTE_ADDR"
# [17] "rook.url_scheme"                "HTTP_ACCEPT_ENCODING"
# [19] "HTTP_COOKIE"                    "REQUEST_METHOD"
# [21] "HTTP_USER_AGENT"                "HTTP_HOST"
