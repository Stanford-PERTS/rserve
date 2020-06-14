# Example for a daily script.

handler_util <- import_module("handler_util")

main <- function (auth_header, platform_data, qualtrics_service, post_url) {
  response <- handler_util$post_to_platform(post_url, auth_header, 'foo')
  return('immediate response 1')
}
