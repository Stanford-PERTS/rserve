modules::import("dplyr", `%>%`, "filter", "mutate")

logging <- import_module("logging")
util <- import_module("util")

`%+%` <- paste0

filter_response_data <- function(response_data, report_date) {
  # This section performs all operations that filter response-level data.

  # tag responses for removal. (We tag instead of just removing for logging
  # purposes.)
  response_data_tagged <- response_data %>%
    mutate(
      filter_after_reports = StartDate >= as.Date(report_date),
      filter_too_early = cycle_name %in% "(pre-first-cycle)",
      filter_testers = !util$is_blank(testing)
    )

  # log removals of responses from data and check resulting dataset
  n_removed_after_reports <- sum(response_data_tagged$filter_after_reports)
  n_removed_too_early <- sum(response_data_tagged$filter_too_early)
  if(n_removed_after_reports > 0) {
    logging$info("Some records were removed from the response data because the start " %+%
                   "date was later than the report date. N = " %+% n_removed_after_reports)
  }
  if(n_removed_too_early > 0){
    logging$info("N = " %+% n_removed_too_early %+% " of " %+% nrow(response_data_tagged) %+%
                   " total rows in the response data removed because responses occurred before first cycle")
  }

  response_data_filtered <- filter(
    response_data_tagged,
    !filter_after_reports,
    !filter_too_early,
    !filter_testers
  )

  return(response_data_filtered)
}

check_filtered_data_rows_exist <- function(response_data_filtered){
  # @to-do: replace with better checks
  # if data has 0 rows after filtering, exit with a message
  if (nrow(response_data_filtered) == 0 ) {
    return(
      "After filtering the response data there are no rows left " %+%
      "for processing. Exiting the metascript. Dimensions of data:" %+%
      paste0(dim(response_data_filtered), collapse = ", ")
    )
  }
}