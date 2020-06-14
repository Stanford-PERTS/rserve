# Manage JSON web tokens (jwt) with the package `jose`
# (jose stands for JavaScript Object Signing and Encryption)

modules::import("jose", "jwt_decode_sig")

decode <- function (token, pubkey) {
  out <- list(payload = NULL, error = NULL)

  tryCatch(
    {
      out$payload <- jwt_decode_sig(token, pubkey = pubkey)
    },
    error = function (err) {
      out$error <<- paste0('JWT ', err$message)
    },
    finally = {
      if (!is.null(out$error)) {
        return(out)
      }
    }
  )

  # Make sure the jwt meets requirements.
  if (!(
    'exp' %in% names(out$payload) &&
    is.numeric(out$payload$exp) &&
    length(out$payload$exp) == 1 &&
    'jti' %in% names(out$payload) &&
    is.character(out$payload$jti) &&
    length(out$payload$jti) == 1
  )) {
    out$error <- "JWT payload must include claims: jti, exp."
    return(out)
  }

  # exp is the unix time as an integer, seconds since the epoc.
  if (out$payload$exp < as.numeric(Sys.time())) {
    # jwt has expired and is invalid
    out$error <- "JWT expired."
    return(out)
  }

  return(out)
}
