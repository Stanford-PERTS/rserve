# Ways you can check what environment you're in. Not currently used, nor is
# using these advised. They're here for documentation.

# set by the package testthat when running tests
is_testing <- function () identical(Sys.getenv("TESTTHAT"), "true")

is_docker <- function () identical(Sys.getenv("RSERVE_DOCKER"), "true")

is_localhost <- function () !is_deployed()

# set by app.yaml when deployed
is_deployed <- function () identical(Sys.getenv("RSERVE_DEPLOYED"), "true")

neptune_domain <- function() {
  # return the neptune domain for this environment
  if (is_localhost()) {
    return("http://localhost:8080")
  } else {
    return("https://neptune.perts.net")
  }
}

triton_domain <- function() {
  # return the triton domain for this environment
  if (is_localhost()) {
    return("http://localhost:10080")
  } else {
    return("https://copilot.perts.net")
  }
}
