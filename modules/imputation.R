modules::import(
  "dplyr",
  `%>%`,
  "arrange",
  "desc",
  "filter",
  "first",
  "group_by",
  "left_join",
  "mutate",
  "n",
  "n_distinct",
  "one_of",
  "rowwise",
  "select",
  "summarise"
)
modules::import("reshape2", "melt")
modules::import("tidyr", "fill")

logging <- import_module("logging")
util <- import_module("util")

helpers <- import_module("scripts/copilot/saturn_engagement_helpers")

`%+%` <- paste0

impute_response_data <- function(
  response_data_recoded,
  imputed_cols,
  cycle_tbl,
  MAX_CYCLES_MISSING,
  SUBSET_TYPES
){

  # The workflow for data imputation is as follows:
  ## Set up important variables and cut nonsense rows (blank rows, same-cycle duplicates)
  ## Add all possible user-time rows, and flag them as such
  ## Remove rows that should not be kept for imputation:
  ### rows that are before anyone ever started
  ### entire students who have been missing longer than MAX_CYCLES_MISSING
  ## impute NAs downward to fill in vars of interest within subjects
  ## Save a copy of the post-imputation data

  logging$debug("########## Set up important variables and cut duplicates within cycle")


  # Define combinatorial ID that uniquely identifies students nested within their groups:
  # Study, team, class, user
  response_data_pre_imputation <- response_data_recoded
  response_data_pre_imputation$comb_id <- paste(
    response_data_pre_imputation$team_id,
    response_data_pre_imputation$class_id,
    response_data_pre_imputation$participant_id,
    sep = "@"
  )
  # @data_exclusion move this to the filtering step to the response data filtering section!!!
  # cut nonsense rows: duplicates within the same cycle (keep the last record)
  response_data_pre_imputation <- response_data_pre_imputation %>% arrange(desc(EndDate))
  response_data_pre_imputation <- response_data_pre_imputation[
    !duplicated(response_data_pre_imputation[,c("comb_id", "cycle_name")]),
    ]

  # @data_exclusion move this to the filtering step to the response data filtering section!!!
  # cut nonsense rows: rows where the student logged in but never answered one LC item
  response_data_pre_imputation$at_least_one_metric <-
    apply(response_data_pre_imputation[, imputed_cols], 1, helpers$any_non_na)
  response_data_pre_imputation <- response_data_pre_imputation[
    response_data_pre_imputation$at_least_one_metric,
    ]

  # record which is the first cycle on record for each student, and merge that info back in
  # @to-do sorting on cycle name seems risky at best. What if the naming conventions
  # change????
  first_cycle_df <- response_data_pre_imputation %>%
    group_by(comb_id) %>%
    summarise(first_cycle_in_record = min(cycle_name))
  rd_pi_with_first_obs <- merge(
    response_data_pre_imputation,
    first_cycle_df,
    by = "comb_id",
    all.x = TRUE,
    all.y = FALSE)

  # tag the rows here as real originals
  rd_pi_with_first_obs$imputed_row <- FALSE


  logging$debug("########## Add all possible user-cycle rows, and flag them as such")

  # create df with all combinations of comb-id and POSSIBLE cycles for that comb-id
  # (Note: possible cycles depend on the team to which the comb-id belongs.)
  # @to-do these calls to `first` make me extremely uncomfortable
  comb_id_to_team <- rd_pi_with_first_obs %>%
    group_by(comb_id) %>%
    summarise(team_id = first(team_id))

  all_comb_id_cycle_combos <- merge(
    comb_id_to_team,
    cycle_tbl[, c("team_id", "cycle_name")],
    by = "team_id",
    all = TRUE
  ) %>%
    util$apply_columns(as.character) %>%
    select(-team_id) %>%
    filter(!is.na(comb_id)) %>%  # Some teams created cycles but have no data
    arrange(comb_id, cycle_name)

  # If no cycles are defined for a comb-id's team, then that comb_id gets a line with "(no cycles defined)".
  all_comb_id_cycle_combos$cycle_name[is.na(all_comb_id_cycle_combos$cycle_name)] <- "(no cycles defined)"

  # merge the real data with the all-combinations df to create blank rows for unobserved cycles,
  # flag the blank rows as not originals,
  # and merge it with first_cycle_df to add info about the first cycle of real data.
  rd_pi_w_blanks <- merge(
    rd_pi_with_first_obs,
    all_comb_id_cycle_combos,
    by = c("comb_id", "cycle_name"),
    all = TRUE
  ) %>%
    mutate(imputed_row = ifelse(is.na(imputed_row), TRUE, imputed_row)) %>%
    select(-first_cycle_in_record) %>%
    merge(
      .,
      first_cycle_df,
      by = "comb_id",
      all.x = TRUE,
      all.y = FALSE
    )

  # Fill in missing identity values in the blank cycle rows using the comb_id
  str_split_matrix <- strsplit(rd_pi_w_blanks$comb_id, "@") %>%
    unlist %>%
    matrix(ncol = 3, byrow = TRUE)
  rd_pi_w_blanks[, c("team_id", "class_id", "participant_id")] <- str_split_matrix

  logging$debug("########## Remove rows that should not be kept for imputation:")
  logging$debug("########## Remove rows that are before anyone ever started")

  # Each student has a first cycle that they showed up - this is stored in first_cycle_in_record.
  # only keep rows where cycle_name is <= first_cycle_in_record.
  # This works because cycle names are strictly alphabetical.
  rd_trim_early_non_data <- rd_pi_w_blanks[
    rd_pi_w_blanks$first_cycle_in_record <= rd_pi_w_blanks$cycle_name,
    ]

  logging$debug("########## Remove entire students who have been missing longer than MAX_CYCLES_MISSING")

  # Each student is now represented by a block of contiguous ordered rows - one for each possible cycle
  # that they COULD have data from their first real cycle onward.
  # Fake rows are identified by imputed_row == TRUE.
  # And they can have a mix of real and fake rows.
  # So within each student, if you count upward from the last row until you hit a real row,
  # and the number of fake rows is greater than MAX_CYCLES_MISSING,
  # then this student has been gone too long and we drop them entirely.
  # Accomplish this by sorting BACKWARDS in time within students.

  # helper function: number of entries in a vector before a value
  num_entries_before_val <- function(vec, val) {
    if(val %in% vec) {
      return(which(vec %in% val)[1] - 1)
    } else {
      stop("Error - target value not found in vector")
    }
  }

  # find missing students
  students_missing_too_long <- rd_trim_early_non_data %>%
    arrange(comb_id, desc(cycle_name)) %>%
    group_by(comb_id) %>%
    summarise(missing_too_long = num_entries_before_val(imputed_row, FALSE) > MAX_CYCLES_MISSING) %>%
    filter(missing_too_long %in% TRUE)


  # drop missing students
  rd_trimmed <- rd_trim_early_non_data[!rd_trim_early_non_data$comb_id %in%
                                         students_missing_too_long$comb_id, ]

  # clean up
  # rm(students_missing_too_long)

  logging$debug("########## impute NAs downward to fill in vars of interest within subjects")

  # Add subset_types to imputed_cols, otherwise the current imputation
  # does not impute demographic vars (saved in subset_types).
  imputed_cols <- c(imputed_cols, SUBSET_TYPES)
  # add team_id to cols_to_impute. In the previous version we did not have to do this
  # but now we have use team_id as part of the index, so we have to add the team_id manually
  imputed_cols <- c(imputed_cols, "team_id", "class_name", "team_name",
                           "code", "expected_n", "teacher_name", "teacher_email")
  imputed_cols <- imputed_cols[imputed_cols %in% names(rd_trimmed)]

  # Within each comb_id, impute in all NA values below a non-NA value of imputed_cols.
  rd_imputed_down <- rd_trimmed %>%
    group_by(comb_id) %>%
    fill(one_of(imputed_cols), .direction = "down")

  # now do backwards imputation -
  # Within each comb_id, impute in all NA values ABOVE a non-NA value of imputed_cols.
  response_data_imputed_base <- rd_imputed_down %>%
    group_by(comb_id) %>%
    fill(one_of(imputed_cols), .direction = "up") %>%
    as.data.frame()  # turns it back from tibble to df for consistency

  row.names(response_data_imputed_base) <- NULL

  return(response_data_imputed_base)

}

check_imputed_response_demographics <- function(
  response_data_imputed_wdem,
  demographic_items
){
  # because we impute forwards but not backwards, it should be the case that
  # every demographic group has as many or more students in each demographic
  # category in cycle n+1 than cycle n.

  demographic_items_present <- demographic_items[
    demographic_items %in% names(response_data_imputed_wdem)
  ]

  check_df_base <- response_data_imputed_wdem %>%
    select(
      one_of(c("cycle_name", demographic_items_present))
    ) %>%
    melt(
      .,
      id.vars = "cycle_name"
    ) %>%
    group_by(.dots = c("cycle_name", "variable")) %>%
    summarise(
      n_na = sum(is.na(value)),
      n_distinct_values = n_distinct(value[!is.na(value)])
    ) %>%
    group_by(variable) %>%
    arrange(cycle_name) %>%
    mutate(
      ordinal_time = 1:n(),
      previous_ordinal_time = ifelse(ordinal_time > 1, ordinal_time - 1, NA)
    )

  # now link the current to next
  check_df_next_ordinal <- check_df_base %>%
    filter(!is.na(previous_ordinal_time))

  check_df <- left_join(
    check_df_base,
    check_df_next_ordinal,
    by = c("variable", "ordinal_time" = "previous_ordinal_time"),
    suffix = c("_current", "_next")
  )

  if(!all(
    check_df$n_distinct_values_next >= check_df$n_distinct_values_current,
    na.rm = T
  )){
    return(
      "After imputation, some demographic variables have more distinct " %+%
      "values at earlier timepoints than later timepoints. This should " %+%
      "not be possible with imputed data. Stopping."
    )
  }
}


check_imputed_agg_metrics <- function(
  agg_metrics,
  combined_index = c("cycle_name", "reporting_unit_id", "metric", "subset_value")
){
  # This function checks properties of the imputed agg_metrics object. Specifically,
  # we expect an imputed agg_metrics object to have n values such that n at
  # cycle i <= n at cycle i + 1. This is expected because during imputation,
  # values are always carried forward. So there can be NEW respondents at cycle
  # i + 1, but any students present at cycle i should have their data carried
  # forward to i + 1 and so they will always be in cycle i + 1. This function
  # tests whether this is the case.

  # we start by building `agm_current`, which now that I think about it is a bit
  # of a misnomer. In `agm_current`, all of the cycle labels correspond to the
  # cycle in which data were collected/imputed for that row. So the "current"
  # label is in reference to the row of data itself. (This will be contrasted with
  # agm_next_ordinal), which contains all of the cycle > 0 rows and aligns them
  # to the previous cycle so as to be able to compare the numbers in one cycle
  # (the "current" cycle) to the numbers in the cycle immediately before it ("previous")
  agm_current <- agg_metrics %>%
    select(
      one_of(unique(c(
        combined_index,
        "cycle_name",
        "subset_value",
        "n"
      )))
    ) %>%
    group_by(
      .dots = unique(c(
        "subset_value",
        setdiff(combined_index, "cycle_name")
      ))
    ) %>%
    arrange(cycle_name) %>%
    mutate(
      ordinal_time = 1:n(),
      previous_ordinal_time = ifelse(ordinal_time > 1, ordinal_time - 1, NA)
    )

  # make sure agm_current had ordinal values coded correctly. These rows are
  # "wrong ordinals" because the `ordinal_time` field contains a string not found
  # in the corresponding `cycle_name` field. This check is important because we're
  # computing these ordinals by sorting cycle_name within index variables of
  # agg_metrics. If the wrong index variables were used, then the sorting will be
  # wrong and the ordinals will be wrong.
  wrong_ordinal_rows <- agm_current %>%
    rowwise() %>%
    filter(!grepl(ordinal_time, cycle_name))

  if(nrow(wrong_ordinal_rows) > 0){
    return(
      "check_imputed_agg_metrics() is not valid because something went wrong " %+%
      "with the combined index. Stopping."
    )
  }
  agm_next_ordinal <- agm_current %>%
    filter(!is.na(previous_ordinal_time))

  agm_check <- left_join(
    agm_current,
    agm_next_ordinal,
    by = c(
      setdiff(combined_index, "cycle_name"),
      "ordinal_time" = "previous_ordinal_time"
    ),
    suffix = c("_current", "_previous")
  )

  if(!all(agm_check$n_current <= agm_check$n_previous, na.rm = T)){
    return(
      "In check_imputed_agg_metrics, cells were found where the previous n " %+%
      "is less than the current n. This should be impossible with imputed " %+%
      "data. Stopping."
    )
  }
}