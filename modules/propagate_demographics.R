modules::import(
  "dplyr",
  `%>%`,
  "arrange",
  "filter",
  "group_by",
  "matches",
  "mutate",
  "select"
)

propagate_demographics <- function(response_data, demographic_items){
  # Make sure that we only filter on demographic items that are actually
  # present in the data. The items csv may have items that aren't in the data,
  # and that's fine.

  demographic_items_present <- demographic_items[
    demographic_items %in% names(response_data)
  ]

  # To pull demographics, we revert to response_data_tagged,
  # which is the last dataset before filtering.
  # @to-do: move this to before filtering so that we don't have to do this


  cols <- c("participant_id", "StartDate_formatted", demographic_items_present)
  user_demographics_wide <- response_data[cols] %>%
    arrange(participant_id, StartDate_formatted) %>%
    group_by(participant_id) %>%
    mutate(ordinal = 1:length(participant_id)) %>%
    filter(ordinal %in% 1) %>%
    select(-ordinal)

  # now, user_demographics contains authoritative demographic information about demographics
  # for every user. So, we'll go ahead and merge it into the master dataset,
  # and retain the user_demographics values as authoritative over whatever is there now
  # be it NA or not

  response_data_wdem <- merge(
    response_data,
    user_demographics_wide,
    by = "participant_id",
    all.x = TRUE,
    all.y = FALSE,
    suffixes = c("_origXXX", "")
  ) %>%
    select(-matches("_origXXX"))

  return(response_data_wdem)

}


propagate_demographics_row_check <- function(
  response_data_wdem,
  response_data_nodem
){
  # make sure the merge didn't drop any rows! (it shouldn't, but you never know.)
  if(!nrow(response_data_nodem) == nrow(response_data_wdem)){
    return("propagating demographic data resulted in added or dropped rows!")
  }
}