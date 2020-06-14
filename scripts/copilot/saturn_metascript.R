modules::import("httr", "PATCH", "add_headers")
modules::import("urltools", "url_parse")

modules::import('tidyr')
modules::import('jsonlite')
modules::import('readr')
modules::import('stats')
modules::import('dplyr')
modules::import('stringr')
modules::import('reshape2')
modules::import('ggplot2')
modules::import('utils')

env <- import_module("modules/environment")
json_utils <- import_module("json_utils")
logging <- import_module("logging")
perts_ids <- import_module("perts_ids")
profiler <- import_module("profiler")
summarize_copilot <- import_module("summarize_copilot")
util <- import_module("util")

create_org_report <- import_module("scripts/copilot/create_org_report")$create
create_report <- import_module("scripts/copilot/create_saturn_report")$create_report
filter_response_data <- import_module("modules/filter_response_data")
handler_util <- import_module("modules/handler_util")
helpers <- import_module("scripts/copilot/saturn_engagement_helpers")
imputation <- import_module("modules/imputation")
propagate_demographics <- import_module("modules/propagate_demographics")
recode_response_data <- import_module("modules/recode_response_data")

`%+%` <- paste0

metascript <- function(
    auth_header,
    survey_label,
    neptune_report_template,
    should_post,
    triton_tbl,
    org_tbl,
    user_tbl,
    class_tbl,
    team_tbl,
    cycle_tbl,
    program_tbl,
    triton_participant_tbl,
    neptune_participant_tbl,
    saturn_data_input,
    items,
    drivers,
    subsets,
    reporting_units,
    reporting_unit_ids,
    REPORT_DATE = next_monday(),
    emailer = NULL,
    run_program = NULL,
    save_workspace_only = NULL
  ){
  # Args:
  #   reporting_unit_ids - character, optional, only requested organizations, teams,
  #     and classrooms will have reports created.
  #   save_workspace_only - logical, default NULL, if true, and if the script
  #     finds a dedicated crypt folder, it will automatically save its input
  #     and output

  profiler$add_event('start', 'metascript')

  # Make sure this flag is a length-1 logical.
  save_workspace_only <- !is.null(save_workspace_only) && save_workspace_only %in% TRUE
  # save_workspace_only <- FALSE # for SG debugging line-by-line

  # !! These variables needed for load points.
  crypt_path <- util$find_crypt_paths(list(root_name = "rserve_data"))
  should_save_rds <- length(crypt_path$root_name) > 0  # if not found value will be character(0)
  rds_paths <- list(
    args = paste0(crypt_path$root_name,"/rds/saturn_metascript_args.rds"),
    pre_imputation = paste0(crypt_path$root_name,"/rds/saturn_metascript_pre_imputation.rds"),
    pre_class_reports = paste0(crypt_path$root_name,"/rds/saturn_metascript_pre_class_reports.rds"),
    pre_team_reports = paste0(crypt_path$root_name,"/rds/saturn_metascript_pre_team_reports.rds"),
    pre_org_reports = paste0(crypt_path$root_name,"/rds/saturn_metascript_pre_org_reports.rds"),
    return = paste0(crypt_path$root_name,"/rds/saturn_metascript_output_workspace.rds")
  )

  # Save load point.
  if (should_save_rds) {
    saveRDS(as.list(environment()), rds_paths$args)
    logging$info("Crypt folder found, saving rds to ", rds_paths$args)
  }

  ############################################################################
  ########################## LOAD POINT: args ################################
  ############################################################################

  # list2env(readRDS(rds_paths$args), globalenv())

  # check requested RUs, if zero, stop
  if (length(reporting_unit_ids) == 0) {
    msg <- "No reporting units were requested. Exiting."
    logging$error(msg)
    stop(msg)
  }

  logging$info("Saturn metascript")
  logging$info("REPORT_DATE:", REPORT_DATE)
  logging$info("length(reporting_unit_ids):", length(reporting_unit_ids))

  # output:
  ## report_data_list a list with all inputs for all selected classes and teams

  logging$debug("########## SET OPTIONS ##########")

  original_stringsAsFactors <- getOption('stringsAsFactors')
  options(stringsAsFactors = FALSE)

  logging$debug("########## HARDCODED PARAMETERS ##########")

  # Teams who haven't participated for more days than the threshold are
  # excluded from the report generating part. I choose 9 rather than 7 days
  # because the weekend could be confusing (it could add 2 days) disaggregation
  # groups.
  TIME_LAG_THRESHOLD <- 9
  # Collapse together these disaggregation groups if needed for cell size.
  # NOTE: beleset doesn't have any financial stress questions.
  SUBSET_TYPES <- subsets$subset_type %>% unique
  # reporting unit id types
  REPORTING_UNIT_ID_TYPES <- c("class_id", "team_id")
  MAX_CYCLES_MISSING <- 35 # Arbitrary huge number - since cycles are >2 wks each, this is >70 wks
  # this controls what is the longest strike of missing values which we will accept
  # before removing the participant. Any participants having missing data on all metrics
  # for longer than the parameter will be removed from the dataset.
  REPORTING_UNIT_ID_VAR_TYPES <- c("class", "team")
  MIN_CELL <- 5   # no disaggregation if resulting cells would be smaller
  SATURN_INTERNAL_COLUMNS <- c(
    "first_login",
    "learning_conditions",
    "open_response_lcs",
    "saw_demographics",
    "showOpenResponses",
    "show_validation",
    "token",
    "testing",
    "uid"
  )

  logging$debug("########## DEBUGGING OUTPUT   ##########")

  debugging_output <- list(
    unimputed = NULL,
    imputed = NULL,
    classroom_report_errors = list(),
    team_report_errors = list(),
    org_report_errors = list()
  )

  report_notes <- list(
    # Notes on non-report results of reporting units. Each is a character of
    # reporting unit ids.
    code_not_in_responses = character(),
    not_recently_cycled = character(),
    no_associations = character(),
    create_report_error = character(),
    unexplained = character()
  )

  logging$debug("########## CHECK INPUTS   ##########")

  if(!helpers$is_Monday(REPORT_DATE)) {
    stop("The REPORT_DATE is not Monday!")
  }

  kinds <- perts_ids$get_kind(reporting_unit_ids)
  org_ids <- reporting_unit_ids[kinds %in% "Organization"]
  team_ids <- reporting_unit_ids[kinds %in% "Team"]
  class_ids <- reporting_unit_ids[kinds %in% "Classroom"]

  # Check items object for expected data types and missing values
  items_character_columns <- c('question_code', 'question_text', 'variable',
                               'response_options', 'introductory_text',
                               'source_for_question_text', 'driver')
  items_integer_columns <- c('min_good','max_good')

  cols_to_check <- c(items_character_columns, items_integer_columns)
  items_expected_types <- c(
    rep("character", length(items_character_columns)),
    rep("integer", length(items_integer_columns))
  )

  items_actual_types <- items[cols_to_check] %>%
    lapply(., typeof) %>%
    unlist

  if(!all(items_expected_types == items_actual_types)){
    stop("unexpected column types found in `items` input df")
  }

  # check that all columns in the Saturn df appear in the items df and vice versa

  saturn_cols <- names(saturn_data_input)
  missing_from_items <- saturn_cols[
    !saturn_cols %in% c(items$question_code, SATURN_INTERNAL_COLUMNS)
  ]

  if(length(missing_from_items) > 0){
    stop("The following items in the Saturn data are not present in the `items` input df: " %+%
           paste0(missing_from_items, collapse = ", "))
  }

  # items are missing from Saturn if they appear in the items df
  # AND if they are not composite metrics
  missing_from_saturn <- items$question_code[
    !items$question_code %in% saturn_cols &
      !items$is_composite
    ]

  # Log and fill in missing columns in Saturn data.
  saturn_all_items <- saturn_data_input
  if(length(missing_from_saturn) > 0){
    logging$info(
      "The following variables are absent from the Saturn data, despite " %+%
        "being either specified in `copilot_items.csv` " %+%
        "or listed in SATURN_INTERNAL_COLUMNS:" %+%
        paste0(missing_from_saturn, collapse = ", ") %+%
        ". These fields were added to the saturn_data_input as all-NA cols"
    )
    for(col in missing_from_saturn){
      saturn_all_items[[col]] <- NA
    }
  }

  # check the min_good and max_good values. All items that are either composites
  # or metrics for reports ought to have min_good AND max_good values
  items_min_max_good <- items %>% dplyr::filter(metric_for_reports | is_composite)
  if(any(util$is_blank(items_min_max_good$min_good) | util$is_blank(items_min_max_good$max_good))){
    stop("Blanks were found where min and/or max values were expected." %+%
         "Stopping without creating reports.")
  }

  # Saturn data categorical var checks
  items_for_cat_check <- items$question_code[
    !util$is_blank(items$response_options) & items$categorical
  ]
  for(item in items_for_cat_check){
    allowed_vals <- items$response_options[items$question_code %in% item] %>%
      strsplit(., "; ") %>%
      unlist
    present_vals_wblanks <- saturn_all_items[[item]] %>% unique
    present_vals <- present_vals_wblanks[!util$is_blank(present_vals_wblanks)]
    if(!all(present_vals %in% allowed_vals)){
      stop("Disallowed values were found in Saturn data field " %+% item %+% ": " %+%
             paste0(present_vals[!present_vals %in% allowed_vals], collapse = ", ") %+%
             ". Please update the items df and/or Saturn coding scheme to " %+%
             "render reports.")
    }
  }

  # Check the drivers/items overlap (should be perfect), because we're
  # filtering both objects by program
  drivers_not_in_items <- drivers$driver[!drivers$driver %in% items$driver]
  items_not_in_drivers <- items$driver[!items$driver %in% drivers$driver] %>%
    unique() %>%
    na.omit()

  if(length(drivers_not_in_items) > 0){
    stop("Some drivers in the drivers df were not found in the items df: " %+%
           paste0(drivers_not_in_items, ", "))
  }
  if(length(items_not_in_drivers) > 0){
    stop("Some drivers in the items df were not found in the drivers df: " %+%
           paste0(items_not_in_drivers, ", "))
  }
  if(any(duplicated(drivers$driver))){
    stop("The following drivers are duplicated in drivers.csv: " %+%
           paste0(drivers$driver[duplicated(drivers$driver)]))
  }

  # Check and the Triton tables
  # If there is no Triton information in any one table (e.g. because tables are
  # wiped for the summer), then gracefully exit. Note: cycle_tbl not included
  # because cycles aren't essential for reports.
  triton_tbls <- c("triton_tbl", "triton_participant_tbl", "user_tbl",
                   "team_tbl", "class_tbl")
  for(tbl_name in triton_tbls) {
    if(nrow(get(tbl_name)) %in% 0) {
      logging$warning(
        "Warning: Triton table " %+% tbl_name %+% " has no rows, so " %+%
        "reports cannot be rendered. Stopping."
      )
      return(list(email_msg = "One or more critical Triton tables are " %+%
        "empty (perhaps cleared for the summer), so no reports were generated."
      ))
    }
  }

  #############################################################################
  ########### Merge Data Sources ##############################################

  # clean white spaces and non-alphanumeric characters before merging
  triton_tbl[ ,c("team_id", "class_name")] <-
    triton_tbl[ ,c("team_id", "class_name")]  %>%
    lapply(., stringr::str_trim) %>% as.data.frame()

  saturn_triton <- dplyr::left_join(saturn_all_items, triton_tbl, by = "code")

  # add in teacher information:
  sat_trit_class <- dplyr::left_join(
    saturn_triton,
    class_tbl[, c("class_id", "contact_id")],
    by = "class_id"
  )

  # then linked to user_tbl$name and user_tbl$email via user_tbl$user_id
  stc_user <- sat_trit_class %>%
    dplyr::rename(user_id = contact_id) %>%
      dplyr::left_join(
        .,
        user_tbl[, c("email", "name", "user_id")],
        by = "user_id"
      ) %>%
    rename(
      teacher_id = user_id,
      teacher_name = name,
      teacher_email = email
    )

  # Merge in raw student IDs from Neptune data. These are needed to link
  # participant ids in Saturn (issued by Neptune) to roster information in
  # Copilot/Triton.
  neptune_participant_tbl$raw_id <- neptune_participant_tbl$name
  neptune_participant_tbl$participant_id <- neptune_participant_tbl$uid
  response_data_merged <- dplyr::left_join(
    stc_user,
    neptune_participant_tbl[, c("participant_id", "raw_id")],
    by = "participant_id"
  )

  # Check the join operations. The new data object should have the same number
  # of rows as the Saturn input data, because (a) we only did left joins; and
  # (b) there is no principled reason to have filtered out or added any response
  # records at this point. So verify that the number of responses (i.e., rows in
  # Saturn data) has not changed.

  if(nrow(response_data_merged) != nrow(saturn_data_input)){
    stop("Merging Saturn, Neptune, and Triton data data resulted in " %+%
      "unexpected row counts. Reports will not be rendered because this " %+%
      "could pose a serious threat to data integrity.")
  }

  ##############################################################################
  ########## Define cycles #####################################################

  logging$debug("########## DEFINE CYCLES ##########")
  ## We use cycles, not weeks, to track participation over time.
  ## Cycles are unique relative to teams/projects, just like week_start.
  ## Here we use the cycle_tbl from Copilot to define cycles for teams.
  ## Chris has confirmed that cycle dates never overlap within teams.

  # remove cycles with an NA start_date - these might be placeholders created by
  # Copilot but they have not been defined by users and are meaningless in terms
  # of report display.
  cycle_table_orig <- cycle_tbl # save for debugging
  cycle_tbl <- dplyr::filter(cycle_tbl, !is.na(start_date))

  # sort table and format cycle dates
  cycle_tbl <- cycle_tbl %>%
    arrange(team_id, ordinal) %>%
    rename(start_date_raw = start_date,
          end_date_raw = end_date)
  cycle_tbl$start_date <- lubridate::ymd(cycle_tbl$start_date_raw)
  cycle_tbl$end_date <- lubridate::ymd(cycle_tbl$end_date_raw)
  REPORT_DATE_FORMATTED <- lubridate::ymd(REPORT_DATE)

  # Remove cycles that start after the report-date.
  # Teams may have defined these in advance, but they haven't started yet, so
  # they shouldn't be in reports
  cycle_tbl <- cycle_tbl[cycle_tbl$start_date < REPORT_DATE_FORMATTED, ]


  # Make sure that all time from first cycle start date to today is continually
  # covered by intervals.
  # "end_date_x" = end date eXtended.
  # The rules are:
  # * If you are the most recent cycle in your team, then your end date is
  #   extended to the REPORT_DATE.
  # * Otherwise, your end date is extended to the day before the start of the
  #   next # cycle.
  # Note that time is NOT covered before the start of the first cycle for a
  # team.
  cycle_tbl <- cycle_tbl %>%
    dplyr::group_by(team_id) %>%
    dplyr::mutate(
      end_date_x = lubridate::as_date(ifelse(ordinal %in% max(ordinal),
      REPORT_DATE_FORMATTED,
      lead(start_date) - lubridate::days(1)))
    )

  # create zero-padded ordinal on single-digit ordinals for proper sorting (e.g.
  # replace "1" with "01") This ensures that "10" doesn't come right after "1"
  # alphabetically! Note: this breaks if anyone reaches 100 cycles.
  cycle_tbl$ordinal_padded <- ifelse(nchar(cycle_tbl$ordinal) %in% 1,
                                     paste0("0", cycle_tbl$ordinal),
                                     as.character(cycle_tbl$ordinal))

  # Create alphabetically-ordered cycle names based on start dates. Also create
  # "long" cycle names (with end date) for participation table. DO NOT use the
  # extended dates (which would make people think there's a mistake, since
  # Copilot doesn't show those dates either).
  cycle_tbl$start_month_abb <- month.abb[lubridate::month(cycle_tbl$start_date)]
  cycle_tbl$start_day <- lubridate::day(cycle_tbl$start_date) %>% as.character()
  cycle_tbl$end_month_abb <- month.abb[lubridate::month(cycle_tbl$end_date)]
  cycle_tbl$end_day <- lubridate::day(cycle_tbl$end_date) %>% as.character()
  cycle_tbl <- cycle_tbl %>%
    group_by(team_id) %>%
    mutate(
      cycle_name = (
        "Cycle " %+% ordinal_padded %+% " (" %+% start_month_abb %+% ". " %+%
        start_day %+% ")"
      ),
      cycle_name_long = (
        "Cycle " %+% ordinal_padded %+% " (" %+% start_month_abb %+% ". " %+%
        start_day %+% " - " %+% end_month_abb %+% ". " %+% end_day %+% ")"
      )
    )
  logging$debug(names(cycle_tbl) %>% sort())

  ########## MAP CYCLES TO RESPONSES ##########

  response_data_cycles_merged <- summarize_copilot$map_responses_to_cycles_orig(
    response_data_merged,
    cycle_tbl
  )

  # Also map on long cycle names for participation table reporting, merging on team-cycle combo
  response_data_cycles <- merge(response_data_cycles_merged,
                cycle_tbl[, c("team_id", "cycle_name", "cycle_name_long")],
                by = c("team_id", "cycle_name"),
                all.x = TRUE,
                all.y = FALSE)

  # Observe early data and uncycled data!
  early_data <- response_data_cycles_merged[response_data_cycles_merged$cycle_name %in% "(pre-first-cycle)",
                     c("team_id", "team_name", "class_id", "class_name",
                       "StartDate_formatted", "first_cycle_start_date")]
  uncycled_data <- response_data_cycles_merged[response_data_cycles_merged$cycle_name %in% "(no cycles defined)",
                        c("team_id", "team_name", "class_id", "class_name",
                          "StartDate_formatted", "first_cycle_start_date")]

  # Record the earliest recorded data for each of these teams, along with the
  # team_id, so you can warn them about missing data in their reports. If there
  # are no "early" recorded responses, define earliest_data as an empty
  # data.frame. create_reports.R uses it, but only to check for present teams,
  # so if it has zero rows it's ok.
  if(nrow(early_data) > 0){
    earliest_data <- early_data %>%
      group_by(team_id) %>%
      summarise(earliest_response_date = min(StartDate_formatted))
  } else{
    earliest_data <- data.frame(matrix(ncol = 2, nrow = 0)) %>%
      setNames(c("team_id", "earliest_response_date"))
  }

  #####################################################################
  ##### Merge with Neptune to get target groups #######################

  triton_participant_tbl_original <- triton_participant_tbl

  #### FIX TRITON INDEX restore triton_participant_tbl
  # Chris has changed the MySQL database, so the triton_participant_tbl is different
  # He added team_id and colapsed rows with the same student_id into one row, which
  # means that there might be multiple classrooms per row (classroom_id is now classroom_ids)

  # 1. Origianlly there was no team_id, so I will rename it
  triton_participant_tbl <- triton_participant_tbl %>% dplyr::rename(
    team_id_triton = team_id
  )

  # clean up classroom ids
  triton_participant_tbl$classroom_ids <- gsub("\\\\", "\\", triton_participant_tbl$classroom_ids)
  triton_participant_tbl$classroom_ids <- gsub("\\[", "", triton_participant_tbl$classroom_ids)
  triton_participant_tbl$classroom_ids <- gsub("\\]", "", triton_participant_tbl$classroom_ids)

  # count how many times classroom is mentioned
  triton_participant_tbl$classroom_ids_n <- sapply(
    gregexpr("Classroom", triton_participant_tbl$classroom_ids, fixed=TRUE),
    function(i) sum(i > 0)
  )
  total_classroom_rows <- sum(triton_participant_tbl$classroom_ids_n)
  triton_participant_tbl <- triton_participant_tbl %>%
    dplyr::mutate(classroom_id = strsplit(as.character(classroom_ids), ",")) %>%
    tidyr::unnest(classroom_id) %>%
    dplyr::mutate(classroom_id = base::trimws (classroom_id, which = "both"))
  if(total_classroom_rows != nrow(triton_participant_tbl)) {
    logging$error("########## THERE ARE LOST / ADDED ROWS  triton_participant_tbl ##########")
    # no need to stop script, but should be investigaged if happens (e.g. there might be dropped NAs)
  }

  logging$debug("########## ADD TARGET GROUP INFORMATION ##########")
  # first, I need to get the participant_id from neptune

  triton_participant_tbl <- merge(triton_participant_tbl,
                                  triton_tbl %>% select(team_id, class_id),
                                  by.x = "classroom_id",
                                  by.y = "class_id",
                                  all.x = TRUE,
                                  all.y = FALSE)

  # Check if there are duplicated student_ids in the neptune table before
  # merging. There shouldn't be b/c the corresponding SQL table has a unique
  # index on these two columns.
  if(sum(duplicated(neptune_participant_tbl[,c("organization_id", "name") ])) > 0) {
    stop("There are duplicated student_ids in a neptune table")
  }

  # Map _triton_ participant IDs to _neptune_ participant IDs (the triton ones
  # are not useful for anything).
  participant_id_map <- merge(
    # The triton ids are not useful for anything, and can only be confused
    # with the "real" neptune ones, so drop them to prevent mistakes.
    triton_participant_tbl_original %>% dplyr::rename(
      triton_participant_id = uid
    ),
    neptune_participant_tbl %>% dplyr::rename(
      neptune_participant_id = uid
    ),
    by.x = c("team_id", "stripped_student_id"),
    by.y = c("organization_id", "name"),
    all.x = TRUE,
    all.y = FALSE
  ) %>% dplyr::select(neptune_participant_id, triton_participant_id, in_target_group)

  non_blank_neptune_ids <- participant_id_map$neptune_participant_id[!is.na(participant_id_map$neptune_participant_id)]
  if (any(duplicated(non_blank_neptune_ids))) {
    stop("There are duplicated student_ids after merging.")
  }

  # A merge that Chris understands: take the participant_ids in Saturn, filter
  # the Neptune participant table to just those IDs. Then, do the merge above, so
  # Triton merged to the now-filtered Neptune as an outer join. And now, there may
  # be Triton participants that have no Neptune correspondence because they didn't
  # do the survey, and there may be Neptune participants with no Triton correspondence,
  # and the only legitimate reason for this would be if they got deleted from the
  # roster after they completed the survey. So can't put any bounds on the size
  # of those sets.

  # problematic_set <- on Neptune, in correct team, but not Triton.
  saturn_participant_ids <- saturn_data_input$participant_id %>% unique()
  relevant_neptune_rows <- neptune_participant_tbl %>%
    dplyr::filter(participant_id %in% saturn_participant_ids)
  test_merge <- merge(
    triton_participant_tbl,
    relevant_neptune_rows,
    by.x = c("team_id_triton", "stripped_student_id"),
    by.y = c("organization_id", "name"),
    all.x = TRUE,
    all.y = TRUE,
    suffixes = c("_triton", "_relevant_neptune")
  )

  problematic_set <- test_merge %>%
    dplyr::filter(
      !util$is_blank(participant_id),
      util$is_blank(uid_triton)
    )

  if (nrow(problematic_set) > 0) {
    n_prob_rows <- nrow(problematic_set)
    affected_teams <- problematic_set$team_id_triton %>% unique()
    logging$warning("Problematic merge detected: " %+% n_prob_rows %+%
      " participants were found with Neptune data and no corresponding" %+%
      " records on Triton. This can only happen when users delete students " %+%
      " from rosters AFTER students have completed the survey. If this " %+%
      " number seems large, it could be a good idea to verify that " %+%
      " students indeed were removed from rosters after being surveyed on affected" %+%
      " teams. Affected teams include " %+%
      paste0(affected_teams, collapse = ", "))
  }

  response_data_neptune <- merge(
    response_data_cycles,
    participant_id_map,
    by.x = "participant_id",
    by.y = "neptune_participant_id",
    all.x = TRUE,
    all.y = FALSE
  )

  if (nrow(response_data_neptune) != nrow(response_data_cycles)) {
    stop("added or dropped rows after merging in_target_group information")
  }

  # compute team target messages. These will be displayed to teams under
  # different conditions in the reports.

  # the target_msg_df must return counts of target group students for each team whenever
  # a team has SOME target group information; i.e., when there are some blank and some non-blank
  # values. But where ALL the values are blank
  target_msg_df <- response_data_neptune %>% group_by(team_id) %>%
    summarise(
      all_target_info_blank = all(util$is_blank(in_target_group)),
      in_target_count = ifelse(all_target_info_blank, NA, sum(in_target_group %in% "Target Grp."))
    )
  team_tbl_tg <- merge(target_msg_df,
                       team_tbl,
                       by = "team_id",
                       all.x = T,
                       all.y = T)
  if(nrow(team_tbl) != nrow(team_tbl_tg)){
    stop("merging target group info with team_tbl resulted in added or dropped rows. No reports created.")
  }

  team_tbl_tg$target_msg <- NA
  # if any teams have set a target group name but don't have any target group students present in the data,
  # they should get a message telling them to mark their students
  team_tbl_tg$target_msg <- ifelse(
    team_tbl_tg$in_target_count %in% 0 & !util$is_blank(team_tbl_tg$target_group_name),
    "Your team’s target group is “group_name_XXXX”, but no students have been marked as in this group." %+%
      "To mark students, update your classroom roster at website_yyy.",
    team_tbl_tg$target_msg)
  # if any team has set students to be part of target groups but have not named their target group,
  # they should be prompted to name their target group.
  team_tbl_tg$target_msg <- ifelse(
    !team_tbl_tg$in_target_count %in% 0 & !util$is_blank(team_tbl_tg$in_target_count) &
      util$is_blank(team_tbl_tg$target_group_name),
    "Your team has not defined a target group, but there are students marked on " %+%
      "classroom rosters as being in a target group. To learn more and define" %+%
      " a target group for your team, check out the website_xxx.",
    team_tbl_tg$target_msg)
  # otherwise, for properly configured target groups, just say what the target group name is.
  team_tbl_tg$target_msg <- ifelse(
    !team_tbl_tg$in_target_count %in% 0 & !util$is_blank(team_tbl_tg$in_target_count) &
      !util$is_blank(team_tbl_tg$target_group_name),
    "Your team’s target group is “group_name_XXXX”.",
    team_tbl_tg$target_msg)

  # replace placeholders with correct strings
  f <- function(pattern,replacement,input) {
    if(util$is_blank(input)) return(input)
    else return(gsub(pattern,replacement,input))
  }
  gsub_vect <- Vectorize(f, SIMPLIFY = FALSE)
  team_tbl_tg$target_msg <- gsub_vect("group_name_XXXX",team_tbl_tg$target_group_name, team_tbl_tg$target_msg) %>% unlist %>% unname
  team_tbl_tg$target_msg <- gsub_vect(
    "website_xxx",'<a href="https://www.perts.net/engage/faq#what-is-target-group">Copilot FAQ</a>',
    team_tbl_tg$target_msg
  ) %>%
    unlist %>%
    unname
  team_tbl_tg$target_msg <- gsub_vect(
    "website_yyy",'<a href="https://www.copilot.perts.net">copilot.perts.net</a>',
    team_tbl_tg$target_msg
  ) %>%
    unlist %>%
    unname

  ##############################################################################
  ######################## Propagate demographics # 1 ##########################

  # Note: demographics get propagated twice in the metascript. The first time,
  # it's in order to recode the response data columns effectively: we need
  # propagated demographic info in order to recode the subset categories and
  # so-forth. We then do it again after imputation, because imputation adds
  # rows to the data and demographic information needs to be appended to those
  # new rows. All of that is to say, IF YOU NOTICE THIS OPERATION HAPPENING
  # TWICE AND THINK YOU SHOULD DELETE REDUNDANT CODE, DON'T DO IT!!!!
  # Or at least, check yourself before you wreck yourself <3 SG

  demographic_items <- items$question_code[items$demographics]
  response_data_wdem <- propagate_demographics$propagate_demographics(
    response_data_neptune,
    demographic_items
  )
  propagate_row_check <- propagate_demographics$propagate_demographics_row_check(
    response_data_wdem,
    response_data_neptune
  )
  if(!is.null(propagate_row_check)) stop(propagate_row_check)

  ##############################################################################
  ######################## Clean response data columns #########################
  # ALL operations to recode or add columns to the filtered response data belong
  # here.

  # declare the recoded response data object and do basic item type corrections

  response_data_recoded <- recode_response_data$recode_response_data(
    response_data_wdem,
    items,
    subset_config = subsets
  )

  # save the subset columns that were found in the data for later
  expected_subset_cols <- SUBSET_TYPES %+% "_cat"
  PRESENT_SUBSET_COLS <- expected_subset_cols[
    expected_subset_cols %in% names(response_data_recoded)
  ]

  ######### Check integrity of recoded data
  # check for present subset columns here because prior to this point in the script,
  # we wouldn't expect them to exist.
  recode_checks <- list(
    present_subsets = recode_response_data$check_present_recoded_subsets(
      response_data_recoded,
      subsets
    ),
    numerics = recode_response_data$check_recoded_numerics(
      response_data_recoded,
      items
    ),
    composites = recode_response_data$check_recoded_composites(
      response_data_recoded,
      items
    ),
    correct_subsets = recode_response_data$check_recoded_subsets(
      response_data_recoded,
      subsets,
      emailer
    )
  ) %>%
    unlist()

  if(length(recode_checks) > 0){
    stop(
      length(recode_checks) %+% " error(s) found in checks for " %+%
      "recode_response_data: " %+%
      paste0(recode_checks, collapse = ", ")
    )
  }

  #############################################################################
  ###################### Filter Responses #####################################

  response_data_filtered <- filter_response_data$filter_response_data(
    response_data_recoded,
    report_date = REPORT_DATE
  )
  filter_row_check <- filter_response_data$check_filtered_data_rows_exist(
    response_data_filtered
  )
  if(!is.null(filter_row_check)) {
    return(list(email_msg = filter_row_check))
  }

  # If there are NO recent data with which to render reports, log and exit
  threshold_date <- lubridate::as_date(REPORT_DATE) - TIME_LAG_THRESHOLD
  if (all(response_data_filtered$StartDate < threshold_date)) {
    msg <- "There were no survey responses within the pre-defined number " %+%
      "of threshold days before the report date, so no reports were generated."
    logging$warning(msg)
    return(list(email_msg = msg))
  }

  #############################################################################
  ###################### Identify present metrics #############################

  logging$debug("########## IDENTIFY THE PRESENT_METRICS ##########")
  ##  The present_metrics are the questions that will be reported on.
  ##  To qualify, data must be present and corresponding question_code must exist
  ##  in `items`, marked as TRUE in the `metric_for_reports` field.
  ##  A second check will be run at each subsetting level to ensure that
  ##  reporting would not violate MIN_CELL.

  all_metrics  <- items$question_code[items$metric_for_reports] %>% unique
  data_all_metrics <- response_data_recoded[,names(response_data_recoded) %in% all_metrics]

  n_answers_by_column <- util$apply_columns(data_all_metrics, function(col){
      sum( !is.na(col) )
  })

  # calculate all present metrics. Metrics are present if they appear in the dataset
  # AND have cell sizes of at least the MIN_CELL size
  present_metrics <- names(n_answers_by_column)[n_answers_by_column >= MIN_CELL]
  # drop metrics that are actually subsetting fields (demographics)
  present_metrics <- present_metrics[present_metrics %in% all_metrics]

  # Save load point.
  if (should_save_rds) {
    saveRDS(as.list(environment()), rds_paths$pre_imputation)
    logging$info("Crypt folder found, saving rds to ", rds_paths$pre_imputation)
  }

  ############################################################################
  ######################## Impute data #######################################
  ############################################################################

  # list2env(readRDS(rds_paths$pre_imputation), globalenv())


  logging$debug("########## DATA IMPUTATION ##########")
  profiler$add_event('data imputation', 'metascript')

  # Choose columns you want to impute (@to-do @data_exclusion ... why are we restricting to
  # present metrics here? Metrics are "present" if the number of non-blank responses
  # is >= min_cell; i.e., >= 5. Don't we still want to impute responses where
  # n < 5? Seems like the wrong way to exclude those data. Very implicit and
  # hard-to-follow.)

  imputed_cols <- present_metrics[present_metrics %in% colnames(response_data_recoded)]

  response_data_imputed_base <- imputation$impute_response_data(
    response_data_filtered,
    imputed_cols,
    cycle_tbl,
    MAX_CYCLES_MISSING,
    SUBSET_TYPES
  )
  ##############################################################################
  ######################## Propagate demographics # 2 ##########################
  # propagate demographics here again because imputation has created new rows,
  # which won't have the appropriate demographic values unless they are
  # propagated

  demographic_items_incl_recoded <- c(
    items$question_code[items$demographics],
    subsets$subset_type %>% unique() %+% "_cat"
  )

  response_data_imputed_wdem <- propagate_demographics$propagate_demographics(
    response_data_imputed_base,
    demographic_items_incl_recoded
  )
  imputation$check_imputed_response_demographics(
    response_data_imputed_wdem,
    demographic_items_incl_recoded
  )

  logging$debug("########## Imputation complete!")

  logging$debug("########## BUILD COMPLETE PARTICIPATION TABLE (LONG FORMAT) ##########")
  profiler$add_event('melt ppn table', 'metascript')
  # Inputs:
  #  response_data_neptune [team_id x class_id x student_ID x cycle_name]
  #  response_data_neptune [team_id x class_id x student_ID x cycle_name]
  # Outputs:
  #  part_data_long_exp [cycle_name]: Participation data, long format, not expanded (only shows defined cycles)

  # First, create a vector of ID var names for any reporting units that we want to aggregate.
  reporting_unit_id_var_id_names <- REPORTING_UNIT_ID_VAR_TYPES %+% "_id"

  # Create a giant expected_ns table for all reporting units.
  expected_ns <- lapply(
    reporting_unit_id_var_id_names,
    helpers$get_expected_ns_table,
    triton_tbl
  ) %>%
    util$rbind_intersection()

  # Melt all the data to get long participation data -
  # Each row is defined by a reporting_unit_id/cycle_name_long combination.
  part_data_long_exp <- response_data_filtered %>%
    melt(
      id.vars = c("cycle_name_long"),
      measure.vars = reporting_unit_id_var_id_names,
      variable.name = "reporting_unit_type",
      value.name = "reporting_unit_id"
    ) %>%
    dplyr::group_by(reporting_unit_id, cycle_name_long) %>%
    dplyr::summarise(n = dplyr::n())

  #  Calculate percentages at scale for every cell and format to char as needed
  part_data_long_exp <- merge(
    part_data_long_exp,
    expected_ns,
    by = "reporting_unit_id",
    all.x = TRUE
  )

  percent_formatted <- ifelse(
    part_data_long_exp$expected_n > 0,
    paste0(
      " (",
      round(part_data_long_exp$n / part_data_long_exp$expected_n * 100, 0),
      "%)"
    ),
    "" # if no denom for a %, display just the count, no parentheses.
  )
  part_data_long_exp$n_formatted <- paste0(part_data_long_exp$n, percent_formatted)
  part_data_long_exp <- dplyr::rename(part_data_long_exp, cycle_name = cycle_name_long)

  logging$debug("########## CREATING AGG_METRICS: MAKE SUPER-MELTED RECENT DATA ##########")
  profiler$add_event('agg metrics', 'metascript')

  # Inputs:
  #   data_cat [team_id x class_id x student_ID x cycle_name]
  #   REPORTING_UNIT_ID_TYPES ("team_id", "classroom_id", etc.)
  #   subset_types ("race", "gender", etc.)
  #   All_metrics (“mw1_1”, mw1_2”, etc.)
  # In the middle:
  #   Filter data_cat to teams that are currently active, because they are the ones for whom we will make reports
  #   Melt all_metrics columns down into rows, creating new columns “metric” and “value”
  #   Melt subset_types columns down into rows, creating new columns “subset_type” and “subset_value”
  #   Filter to most recent cycle_name per team (for calculating p-values later)
  # Outputs:
  #   d_super_melted_active [team_id x classroom_id x student_ID x cycle_name x metric x subset_type x subset_value]
  #   d_super_melted_active_recent [team_id x classroom_id x student_ID x cycle_name x metric x subset_type x subset_value]

  # Identify active teams and filter data_cat to active teams.
  # Active teams are teams that have data that are more recent than TIME_LAG_THRESHOLD,
  # OR they have modified one or more cycles in the last 7 days.

  days_since_last_vist_per_team_df <- helpers$compute_days_since_last_visit(
    grouping_var = response_data_imputed_wdem$team_id,
    date_var = lubridate::date(response_data_imputed_wdem$StartDate),
    current_date = lubridate::date(REPORT_DATE)
  )

  recent_data_teams <-
    days_since_last_vist_per_team_df[days_since_last_vist_per_team_df$min_lag <= TIME_LAG_THRESHOLD, "grouping_var"]

  recent_cycled_teams_df <- cycle_tbl %>%
    dplyr::mutate(
      days_since_last_cycle_mod = lubridate::date(REPORT_DATE) - lubridate::date(modified),
      recent_cycle_mod = days_since_last_cycle_mod <= 7
    ) %>%
    dplyr::group_by(team_id) %>%
    dplyr::summarise(recent_cycle_mod = any(recent_cycle_mod)) %>%
    dplyr::filter(recent_cycle_mod %in% TRUE)

  # Controls which team and class reports are rendered. They must
  # 1. have responses in the past week
  # 2. have a scheduled cycle that includes the report date
  # NOTE: this assumes that cycle dates have already been extended.
  recent_cycled_teams <- recent_cycled_teams_df$team_id
  active_teams_strict <- intersect(recent_data_teams, recent_cycled_teams)

  # NOTE: this definition is old (pre-Saturn) and may still be wrong. Notice
  # how team that EITHER have recent data OR are in recent cycled are in
  # active_teams.
  active_teams <- c(recent_data_teams, recent_cycled_teams_df$team_id)

  rdi_active_teams <- response_data_imputed_wdem[response_data_imputed_wdem$team_id %in% active_teams, ]

  # Identify and merge in most recent cycles for each team
  most_recent_cycles_per_team <- response_data_filtered %>%
    arrange(team_id, cycle_name) %>%
    dplyr::group_by(team_id) %>%
    summarise(most_recent_observed_cycle = last(cycle_name))

  ##############################################################################
  ############# The final imputed data object is called response_data_imputed
  ##############################################################################
  response_data_imputed <-  merge(
    rdi_active_teams,
    most_recent_cycles_per_team,
    by = "team_id",
    all.x = TRUE
  )

  d_melted_active <- response_data_imputed %>%
    melt(id.vars = c(REPORTING_UNIT_ID_TYPES, "participant_id", "cycle_name", PRESENT_SUBSET_COLS,
                     "most_recent_observed_cycle", "imputed_row", "team_name", "class_name"),
         measure.vars = all_metrics,
         variable.name = "metric") %>%
    rename(metric_value = value)

  # Add in metric-level information and calculate if the response is in the
  # good range
  d_melted_active <- d_melted_active %>%
    merge(., items[,c("question_code","min_good","max_good")],
          by.x = "metric", by.y = "question_code",
          all.x = TRUE) %>%
    mutate(good_range = (metric_value >= min_good &
             metric_value <= max_good))

  # Now melt subsets down into rows
  d_super_melted_active <- d_melted_active %>%
    melt(id.vars = c(REPORTING_UNIT_ID_TYPES, "participant_id", "cycle_name", "metric", "metric_value",
                     "most_recent_observed_cycle", "good_range", "imputed_row",  "team_name", "class_name"),
         measure.vars = PRESENT_SUBSET_COLS,
         variable.name = "subset_type") %>%
    rename(subset_value = value)

  # Some people have NA for some subset values (e.g. no gender recorded),
  # even after imputation. Since we have melted subset values down into the
  # rows, and we are going to calculate aggregate statistics for subset types
  # and subset values, we need to remove the rows with NA subset values. This
  # will not affect the rest of each participant's data.
  d_super_melted_active_orig <- d_super_melted_active
  d_super_melted_active <- d_super_melted_active[
    !is.na(d_super_melted_active$subset_value),
  ]

  # Filter super-melted data to most recent observed week per team
  d_super_melted_active_recent <- d_super_melted_active[
    d_super_melted_active$cycle_name %in% d_super_melted_active$most_recent_observed_cycle,
  ]

  logging$debug("########## CREATING AGG_METRICS: GET P-VALS FOR RECENT DATA ##########")

  # Inputs:
  #   d_super_melted_active_recent [team_id x classroom_id x student_ID x cycle_name x metric x subset_type x subset_value]
  #   REPORTING_UNIT_ID_TYPES ("team_id", "classroom_id", etc.)
  # In the middle:
  #   For each reporting_unit_type "r"...
  #     group_by [r x cycle_name x metric x subset_type] (across people and subset_values) and summarize to calculate
  #     p-values in column “p” using the “value” and “subset_type” columns.
  #     Rename column r to “reporting_unit_id”.
  #   Save all resulting data frames in a list.
  #   Also rbind the list together to make All_pvals_df!
  # Outputs:
  #   Pvals_dfs:
  #     A list of data frames of the form [reporting_unit_id x cycle_name x metric x subset_type]
  #     As many data frames as there are reporting_unit_types
  #   All_pvals_df [reporting_unit_id x cycle_name x metric x subset_type]

  pvals_dfs <- list()

  for(r in REPORTING_UNIT_ID_TYPES) {
    ru_pval_data <- d_super_melted_active_recent %>%
      dplyr::group_by(.dots = c(r, "cycle_name", "metric", "subset_type")) %>%
      summarise(p = suppressWarnings(helpers$p_chi_sq(good_range, subset_value)))
    names(ru_pval_data)[names(ru_pval_data) %in% r] <- "reporting_unit_id"
    pvals_dfs[[r]] <- ru_pval_data
  }

  all_pvals_df <- util$rbind_intersection(pvals_dfs)


  logging$debug("########## CREATING AGG_METRICS: GET METRIC RESULTS ##########")

  # Inputs:
  #   d_super_melted_active [team_id x classroom_id x student_ID x cycle_name x metric x subset_type x subset_value]
  #   reporting_unit_types ("team_id", "classroom_id", etc.)
  #   Agg_group_col_sets: list( c(“subset_type”, “subset_value”), NULL)
  #     Note: this object allows us to group_by subset values to get metric results for each one,
  #     but also to reuse the same code to get results across subsets using the NULL value.
  # In the middle:
  ## BUILD THE METRIC-RESULT DATA FRAMES FOR SUBSETS
  #   for each reporting_unit_type "r"...
  #       index_cols <- c(r, subset_type, subset_value, cycle_name, metric)
  #       new_df <- group_by(data, .dots = index_cols) %>% summarize metric results and save reporting_unit_type
  #       as “r” and reporting_unit_label as the human_readable name.
  #       Rename column r to “reporting_unit_id”.
  #       Save to list.
  ## BUILD THE METRIC-RESULT DATA FRAMES FOR ALL STUDENTS (using the "medium-melted" data with subsets in columns)
  #   for each reporting_unit_type "r"...
  #       index_cols <- c(r, cycle_name, metric)
  #       new_df <- group_by(data, .dots = index_cols) %>% summarize metric results and save reporting_unit_type
  #       as “r” and reporting_unit_label as the human_readable name.
  #       Rename column r to “reporting_unit_id”.
  #       Set subset_type and subset_value to be "All Students"
  #       Save to list.
  # Finally rbind the two lists together to make All_metric_results_df!
  # Outputs:
  #   subset_metric_results_dfs:
  #     A list of data frames of the form [reporting_unit_id x cycle_name x metric x subset_type x subset_value]
  #     As many data frames as there are reporting_unit_types
  #   all_students_metric_results_dfs:
  #     A list of data frames of the form [reporting_unit_id x cycle_name x metric]
  #     As many data frames as there are reporting_unit_types
  #   All_metric_results_df [reporting_unit_id x cycle_name x metric x subset_type x subset_value]

  # setup
  subset_metric_results_dfs <- list()
  all_students_metric_results_dfs <- list()

  # Loop 1 of 2: Build subset_metric_results_dfs
  for(r in REPORTING_UNIT_ID_TYPES) {
    # set index columns
    index_cols <- c(r, "subset_type", "subset_value", "cycle_name", "metric")
    # group and summarise to get metric results
    metric_results_df <- d_super_melted_active %>%
      dplyr::group_by(.dots = index_cols) %>%
      summarise(pct_imputed = mean(imputed_row, na.rm=TRUE),
                pct_good = mean(good_range, na.rm=TRUE) ,
                mean_value = mean(metric_value, na.rm=TRUE),
                se = helpers$se(good_range),
                n = length(good_range),
                class_name = first(class_name),
                team_name = first(team_name))
    # relabel reporting unit identifying information for proper rbinding and future access
    metric_results_df$reporting_unit_type <- r
    names(metric_results_df)[names(metric_results_df) %in% r] <- "reporting_unit_id"
    # and reporting_unit_name as the human_readable name.
    # NOTE: this is hacky and not a design that generalizes across RU types.
    metric_results_df$reporting_unit_name <- ifelse(metric_results_df$reporting_unit_type %in% "class_id",
                                                    metric_results_df$class_name,
                                                    ifelse(metric_results_df$reporting_unit_type %in% "team_id",
                                                           metric_results_df$team_name, NA))
    if(any(is.na(metric_results_df$reporting_unit_name))) {
      stop("Error - reporting unit names not properly mapped while getting metric results.")
    }
    # Add a column to denote the grand mean group (needed by Rmd)
    metric_results_df$grand_mean <- "Subset"
    # save to list metric_results_dfs
    subset_metric_results_dfs[[r]] <- metric_results_df
  }

  # Loop 2 of 2: Build all_students_metric_results_dfs
  for(r in REPORTING_UNIT_ID_TYPES) {
    # set index columns
    index_cols <- c(r, "cycle_name", "metric")
    # group and summarise to get metric results
    metric_results_df <- d_melted_active %>%
      dplyr::group_by(.dots = index_cols) %>%
      summarise(pct_imputed = mean(imputed_row, na.rm=TRUE),
                pct_good = mean(good_range, na.rm=TRUE) ,
                mean_value = mean(metric_value, na.rm=TRUE),
                se = helpers$se(good_range),
                n = length(good_range),
                class_name = first(class_name),
                team_name = first(team_name))
    # relabel reporting unit identifying information for proper rbinding and future access
    metric_results_df$reporting_unit_type <- r
    names(metric_results_df)[names(metric_results_df) %in% r] <- "reporting_unit_id"
    # and reporting_unit_name as the human_readable name.
    # NOTE: this is hacky and not a design that generalizes across RU types.
    metric_results_df$reporting_unit_name <- ifelse(metric_results_df$reporting_unit_type %in% "class_id",
                                                    metric_results_df$class_name,
                                                    ifelse(metric_results_df$reporting_unit_type %in% "team_id",
                                                           metric_results_df$team_name, NA))
    if(any(is.na(metric_results_df$reporting_unit_name))) {
      stop("Error - reporting unit names not properly mapped while getting metric results.")
    }
    # Add a column to denote the grand mean group (needed by Rmd)
    metric_results_df$grand_mean <- "All Students"
    # Add columns for subset_type and subset_value (simply "All Students" for everyone)
    metric_results_df$subset_type <- "All Students"
    metric_results_df$subset_value <- "All Students"
    # save to list metric_results_dfs
    all_students_metric_results_dfs[[r]] <- metric_results_df
  }


  # Finally, rbind the lists together to make all_metric_results_df!
  all_metric_results_df <- util$rbind_intersection(c(
    subset_metric_results_dfs,
    all_students_metric_results_dfs
  ))

  logging$debug("########## CREATING AGG_METRICS: MERGE P-VALS WITH METRIC RESULTS TO GET AGG_METRICS ##########")

  # Inputs:
  #   All_pvals_df [reporting_unit_id x cycle_name x metric x subset_type]
  #   All_metric_results_df [reporting_unit_id x cycle_name x metric x subset_type x subset_value]
  # In the middle:
  #   Merge by [reporting_unit_id x cycle_name x metric x subset_type], keeping all elements of both frames
  #   Should expect to have the same number of rows as All_metric_results_df, but with some repeated p-values because those were calculated at a higher level of aggregation
  #   Should have NAs for p-value for “All Students” rows - that’s good!
  # Outputs:
  #   Agg_metrics [reporting_unit_id x cycle_name x metric x subset_type x subset_value]

  agg_metrics <- merge(all_pvals_df,
                       all_metric_results_df,
                       by = c("reporting_unit_id", "cycle_name", "metric", "subset_type"),
                       all = TRUE)

  # check that nothing weird happened with text-percentage blanks
  # all blank values in pct good correspond to blank pct_good_text values
  if(!all(util$is_blank(agg_metrics$pct_good_text[util$is_blank(agg_metrics$pct_good)]))){
      stop("In the newly-programmed text-percentages feature, some blank percent-good-text values " %+%
           "were spotted where non-blank values were present in the raw pct_good column. Investigate further.")
  }
  if(!all(util$is_blank(agg_metrics$pct_good[util$is_blank(agg_metrics$pct_good_text)]))){
      stop("In the newly-programmed text-percentages feature, some blank percent-good values " %+%
               "were spotted where non-blank values were present in the pct_good_text field. Investigate further.")
  }

  ######### check imputation yet again in agg_metrics!
  imp_check_agm <- imputation$check_imputed_agg_metrics(agg_metrics)
  if(!is.null(imp_check_agm)) stop(imp_check_agm)

  ###########################################################################
  ########## Create control structures for looping through reports ##########
  ###########################################################################

  team_class_with_responses <- triton_tbl %>%
    dplyr::select(team_id, team_name, class_name, code, class_id) %>%
    # Keep only teams for which we have data.
    dplyr::filter(code %in% response_data_imputed$code)

  # Note reporting units excluded by the filter line above.
  report_notes$code_not_in_responses <- class_ids %>%
    Filter(function(id) !id %in% team_class_with_responses$class_id, .) %>%
    c(report_notes$code_not_in_responses)
  report_notes$code_not_in_responses <- team_ids %>%
    Filter(function(id) !id %in% team_class_with_responses$team_id, .) %>%
    c(report_notes$code_not_in_responses)

  team_class_df <- team_class_with_responses %>%
    # Keep only teams and classes that have recent data and are within a
    # cycle.
    dplyr::filter(team_id %in% active_teams_strict)

  # Note reporting units excluded by the filter line above.
  report_notes$not_recently_cycled <- class_ids %>%
    Filter(function(id) !id %in% team_class_df$class_id, .) %>%
    c(report_notes$not_recently_cycled)
  report_notes$not_recently_cycled <- team_ids %>%
    Filter(function(id) !id %in% team_class_df$team_id, .) %>%
    c(report_notes$not_recently_cycled)

  # Keep separate control structures for call the class report function
  # and team report function, so each class/team respectively appears once.
  class_report_df <- team_class_df %>%
    dplyr::filter(class_id %in% reporting_unit_ids)
  team_report_df <- team_class_df %>%
    dplyr::filter(!duplicated(team_id)) %>%
    dplyr::filter(team_id %in% reporting_unit_ids)


  # Save load point.
  if (should_save_rds) {
    saveRDS(as.list(environment()), rds_paths$pre_class_reports)
    logging$info("Crypt folder found, saving rds to ", rds_paths$pre_class_reports)
  }

  ############################################################################
  #################### LOAD POINT: pre_class_reports #########################
  ############################################################################

  # list2env(readRDS(rds_paths$pre_class_reports), globalenv())


  logging$debug("########## CLASS-LEVEL REPORTS ##########")
  profiler$add_event('class reports', 'metascript')

  #######################################################################
  ######## Hide single-roster team data #################################

  # Configure agg_metrics so that reports will hide team data from the class
  # report when there's only one class (roster) on a team. In these cases,
  # users should receive a class report, not a team report.
  # (Because the class report contains open responses, but
  # the team report does not.) But the class report should NOT panel the graphs
  # or the participation table by class vs. team,
  # because this would present two columns of redundant data.

  # The most efficient way to create this behavior is to remove team-level
  # data from the dfs used to generate the graphs and participation tables
  # wherever the number of classes
  # per team == 1. That way, when ggplot is told to panel by (a mutated version
  # of) the field, `reporting_unit_name` ... these classes will only have
  # one unique value in that field, and it will be the name of the roster,
  # NOT the name of the project. If we want it to be the name of the project,
  # then that's trickier but doable with some tweaks.

  # Note that this move does NOT prevent team reports from being rendered.
  # For that, we'll have to do more things.

  # Use the crosswalk table `team_class_df` to pull solo-classes
  solo_class_team_ids <- class_report_df %>%
    dplyr::group_by(team_id) %>%
    dplyr::filter(n_distinct(class_id) == 1) %>%
    dplyr::pull(team_id)

  report_data_list = list()

  for(i in sequence(nrow(class_report_df))) {
    # Declare local variable names for this report
    team_name <- class_report_df[i,"team_name"]
    class_id <- class_report_df[i,"class_id"]
    code <- class_report_df[i,"code"]
    class_name <- class_report_df[i,"class_name"]
    team_id <- class_report_df[i,"team_id"]

    logging$debug(
      "Starting class: ", class_id, " (", i, " of ",
      nrow(class_report_df), ")"
    )

    # if class was not found, replace with NONE
    if(length(class_id) == 0 )  class_id <- "NONE" %+% (sample(1:10000,1) %>% as.character)
    if(util$is_blank(class_id))  class_id <- "NONE" %+% (sample(1:10000,1) %>% as.character)

    # filter the big agg_metrics df to this particular report. Since this
    # is the loop for class reports, agg_metrics_small will exclude team_ids
    # that correspond to solo-roster projects (the object
    # solo_class_team_ids).
    agg_metrics_small <- agg_metrics %>%
      dplyr::filter(
        reporting_unit_id %in% c(team_id, class_id),
        !reporting_unit_id %in% solo_class_team_ids
      )
    # make sure all of the class_id didn't get deleted from agg_metrics_small
    if(!class_id %in% agg_metrics_small$reporting_unit_id){
      stop("Filtering agg_metrics_small to exclude team-level data from " %+%
        "single-roster projects resulted in rosters/classes being " %+%
        "excluded. This needs to be investigated. Reports not generated."
      )
    }

    # make participation table for these RUs and fix column names
    participation_table_df <- part_data_long_exp %>%
      dplyr::filter(
        reporting_unit_id %in% c(team_id, class_id),
        !reporting_unit_id %in% solo_class_team_ids
      ) %>%
      dplyr::select(-n, -expected_n) %>%
      reshape2::dcast(cycle_name ~ reporting_unit_id, value.var = "n_formatted") %>%
      dplyr::arrange(cycle_name)
    # add an order variable to confirm later cutting of this df in the Rmd
    participation_table_df$cycle_order <- 1:nrow(participation_table_df)
    # order the columns so team is always before classroom
    team_column_name <- names(participation_table_df)[
      grepl("Team_", names(participation_table_df))
    ]
    class_column_name <- names(participation_table_df)[
      grepl("Classroom_", names(participation_table_df))
    ]
    participation_table_df <- participation_table_df[
      , c("cycle_name", team_column_name, class_column_name, "cycle_order")
    ]
    # then replace the column names with human-readable labels where appropriate
    # (note that this fails gracefully if no team column exists)
    names(participation_table_df)[
      names(participation_table_df) %in% team_column_name
    ] <- team_name

    names(participation_table_df)[
      names(participation_table_df) %in% class_column_name
    ] <- class_name

    names(participation_table_df)[
      names(participation_table_df) %in% "cycle_name"
    ] <- "Cycle"

    # Assemble the open responses for this class and most recent cycle
    class_raw_data <- response_data_filtered[
      response_data_filtered$class_id %in% class_id,
    ]
    class_most_recent_cycle <- class_raw_data$cycle_name %>% unique %>% sort %>% dplyr::last()
    all_or_question_ids <- items[items$open_response, "question_code"]
    class_recent_open_responses <- class_raw_data %>%
      dplyr::filter(cycle_name %in% class_most_recent_cycle) %>%
      dplyr::select(one_of(all_or_question_ids))

    # Calculate fidelity information for this class (honesty and TUQ):
    temp <- list()
    for(q_name in c("fidelity_class_better", "fidelity_honest")) {
      recent_data <- class_raw_data[class_raw_data$cycle_name %in% class_most_recent_cycle, q_name]
      if(sum(!is.na(recent_data)) < 5) {
        temp[[q_name]] <- NA
      } else {
        min_good <- items[items$question_code %in% q_name, "min_good"]
        max_good <- items[items$question_code %in% q_name, "max_good"]
        pct_good <- sum(recent_data >= min_good & recent_data <= max_good, na.rm = T) /
          sum(!is.na(recent_data))
        temp[[q_name]] <- round(pct_good * 100, 0)
      }
    }
    fidelity_tuq <- temp[["fidelity_class_better"]]
    fidelity_honest <- temp[["fidelity_honest"]]


    # Set up report name
    report_name  <- class_id %+% "." %+% REPORT_DATE %+% ".html" %>% gsub(" ", "", .)

    # Get ready to run!
    logging$info("Preparing to run report:", report_name)

    # This step renders the .Rmd file. Note that the .Rmd file
    # is not configured to be renderable on its own, and relies on
    # variables from the metascript global namespace.
    if (!save_workspace_only) {
      tryCatch(
        {
          report_data <- create_report(
            SUBSET_TYPES = SUBSET_TYPES,
            PRESENT_SUBSET_COLS = PRESENT_SUBSET_COLS,
            REPORT_DATE = REPORT_DATE,
            TEAM_ONLY = FALSE,
            REPORTING_UNIT_ID_TYPES = REPORTING_UNIT_ID_TYPES,
            MAX_CYCLES_MISSING =  MAX_CYCLES_MISSING,
            MIN_CELL = MIN_CELL,
            team_id = team_id,
            class_id = class_id,
            data_cat = response_data_imputed,
            team_name = team_name,
            class_name = class_name,
            agg_metrics_small = agg_metrics_small,
            participation_table_df = participation_table_df,
            class_recent_open_responses = class_recent_open_responses,
            all_or_question_ids = all_or_question_ids,
            items = items,
            drivers = drivers,
            subsets = subsets,
            fidelity_tuq = fidelity_tuq,
            fidelity_honest = fidelity_honest,
            earliest_data = earliest_data,
            target_msg = team_tbl_tg$target_msg[team_tbl_tg$team_id == team_id],
            tg_is_on = !util$is_blank(
              team_tbl_tg$target_group_name[team_tbl_tg$team_id == team_id]
            )
          )

          report_data$filename <- REPORT_DATE %+% ".html"
          report_data$id <- report_data$classroom_id
          report_data_list[[report_data$classroom_id]] <- handler_util$fix_open_response_flag(
            report_data,
            survey_label
          )

          if (should_post) {
            rep_unit <- Find(function (ru) ru$id == class_id, reporting_units)
            handler_util$post_report_data(
              REPORT_DATE,
              auth_header,
              rep_unit,
              neptune_report_template,
              report_data
            )
          }
        },
        error = function (err) {
          logging$error(class_id, team_id, " : ", err)

          debugging_output$classroom_report_errors[[class_id]] <<- list(
            class_id = class_id,
            team_id = team_id,
            error = err
          )
          report_notes$create_report_error <<- class_id %>%
            c(report_notes$create_report_error)

          return(err)
        }
      )
    }
  }

  # Save load point.
  if (should_save_rds) {
    saveRDS(as.list(environment()), rds_paths$pre_team_reports)
    logging$info("Crypt folder found, saving rds to ", rds_paths$pre_team_reports)
  }

  ############################################################################
  #################### LOAD POINT: pre_team_reports ##########################
  ############################################################################

  # list2env(readRDS(rds_paths$pre_team_reports), globalenv())

  logging$debug("########## TEAM ONLY REPORTS ##########")
  profiler$add_event('team reports', 'metascript')

  team_report_index <- 1
  for (active_team_id in team_report_df$team_id) {
    logging$debug(
      "Starting team: ", active_team_id, " (", team_report_index, " of ",
      nrow(team_report_df), ")"
    )
    team_report_index <- team_report_index + 1

    # This is unique by team id, so we get a 1-row df, from which we can pull
    # facts about the team, like name.
    team_report_row <- team_report_df %>%
      dplyr::filter(team_id %in% active_team_id)

    # filter the big agg_metrics df to this particular report:
    agg_metrics_small <- agg_metrics[
      agg_metrics$reporting_unit_id %in% c(active_team_id),
    ]

    # Set up report name
    report_name  <- active_team_id %+% "." %+% REPORT_DATE %+% ".html" %>%
      gsub(" ", "", .)

    # Cut down the big participation table to the team,
    # and make participation table and fix column names
    # In this case, it is the team.
    ru_ids_for_part_table <- active_team_id
    participation_table_df <- part_data_long_exp %>%
      dplyr::filter(reporting_unit_id %in% ru_ids_for_part_table) %>%
      dplyr::select(-n, -expected_n) %>%
      dcast(cycle_name ~ reporting_unit_id, value.var = "n_formatted") %>%
      arrange(cycle_name)
    names(participation_table_df) <- c("Cycle", team_report_row$team_name)

    # Calculate fidelity information for this team (honesty and TUQ):
    team_raw_data <- response_data_recoded[response_data_recoded$team_id %in% active_team_id, ]
    team_most_recent_cycle <- team_raw_data$cycle_name %>% unique %>% sort %>% last
    temp <- list()
    for(q_name in c("fidelity_class_better", "fidelity_honest")) {
      recent_data <- team_raw_data[team_raw_data$cycle_name %in% team_most_recent_cycle, q_name]
      if(sum(!is.na(recent_data)) < 5) {
        temp[[q_name]] <- NA
      } else {
        min_good <- items[items$question_code %in% q_name, "min_good"]
        max_good <- items[items$question_code %in% q_name, "max_good"]
        pct_good <- sum(recent_data >= min_good & recent_data <= max_good, na.rm = T) /
          sum(!is.na(recent_data))
        temp[[q_name]] <- round(pct_good * 100, 0)
      }
    }
    fidelity_tuq <- temp[["fidelity_class_better"]]
    fidelity_honest <- temp[["fidelity_honest"]]

    if (!save_workspace_only) {
      tryCatch(
        {
          report_data <- create_report(
            SUBSET_TYPES = SUBSET_TYPES,
            PRESENT_SUBSET_COLS = PRESENT_SUBSET_COLS,
            REPORT_DATE = REPORT_DATE,
            TEAM_ONLY = TRUE,
            REPORTING_UNIT_ID_TYPES = REPORTING_UNIT_ID_TYPES,
            MAX_CYCLES_MISSING =  MAX_CYCLES_MISSING,
            MIN_CELL = MIN_CELL,
            team_id = active_team_id,
            class_id = NULL,
            data_cat = response_data_imputed,
            team_name = team_report_row$team_name,
            class_name = '',
            agg_metrics_small = agg_metrics_small,
            participation_table_df = participation_table_df,
            class_recent_open_responses = data.frame(),
            all_or_question_ids = NA,
            items = items,
            subsets = subsets,
            drivers = drivers,
            fidelity_tuq = fidelity_tuq,
            fidelity_honest = fidelity_honest,
            earliest_data = earliest_data,
            target_msg = team_tbl_tg$target_msg[team_tbl_tg$team_id == active_team_id],
            tg_is_on = !util$is_blank(
              team_tbl_tg$target_group_name[team_tbl_tg$team_id == active_team_id]
            )
          )

          report_data$filename <- REPORT_DATE %+% ".html"
          report_data$id <- report_data$team_id
          report_data_list[[report_data$team_id]] <- handler_util$fix_open_response_flag(
            report_data,
            survey_label
          )

          if (should_post) {
            rep_unit <- Find(function (ru) ru$id == active_team_id, reporting_units)
            handler_util$post_report_data(
              REPORT_DATE,
              auth_header,
              rep_unit,
              neptune_report_template,
              report_data
            )
          }
        },
        error = function (err) {
          logging$error(active_team_id, " : ", err)

          debugging_output$team_report_errors[[active_team_id]] <<- list(
            team_id = active_team_id,
            error = err
          )
          report_notes$create_report_error <<- active_team_id %>%
            c(report_notes$create_report_error)

          return(err)
        }
      )
    }
  }

  logging$debug("########## ORGANIZATION REPORTS ##########")
  profiler$add_event('org reports', 'metascript')

  # Save load point.
  if (should_save_rds) {
    saveRDS(as.list(environment()), rds_paths$pre_org_reports)
    logging$info("Crypt folder found, saving rds to ", rds_paths$pre_org_reports)
  }

  ############################################################################
  ###################### LOAD POINT: pre_org_reports #########################
  ############################################################################

  # list2env(readRDS(rds_paths$pre_org_reports), globalenv())

  # Create an easy-to-use association table showing how all orgs, teams, and
  # classrooms are nested.
  org_team_class <- team_tbl %>%
    # long form of many-to-many of team to org combos
    json_utils$expand_string_array_column(organization_ids) %>%
    # take out orgs that aren't in the requested list
    dplyr::rename(organization_id = organization_ids) %>%
    dplyr::filter(organization_id %in% reporting_unit_ids) %>%
    dplyr::left_join(class_tbl, 'team_id') %>%
    dplyr::select(organization_id, team_id, team_name, class_id, code)

  # Note any requested orgs that have been dropped because there are no teams
  # or classes associated with it.
  report_notes$no_associations <- org_ids %>%
    Filter(function(id) !id %in% org_team_class$organization_id, .) %>%
    c(report_notes$no_associations)

  org_team_class_w_data <- org_team_class %>%
    filter(class_id %in% unique(response_data_recoded$class_id))

  # Note any requested orgs that have been dropped because they have no data.
  report_notes$code_not_in_responses <- org_ids %>%
    Filter(function(id) !id %in% org_team_class_w_data$organization_id, .) %>%
    c(report_notes$code_not_in_responses)

  orgs_to_render <- unique(org_team_class_w_data$organization_id)

  # Recall that triton organizations are the same as copilot communities.
  org_report_index <- 1
  for (org_id in orgs_to_render) {
    logging$debug(
      "Starting org: ", org_id, " (", org_report_index, " of ",
      length(orgs_to_render), ")"
    )
    org_report_index <- org_report_index + 1

    # These should always have rows because we excluded all non-associated,
    # no-data orgs above.
    assoc <- filter(org_team_class_w_data, organization_id %in% org_id)
    org_data_cat <- filter(response_data_recoded, class_id %in% assoc$class_id)
    if (nrow(assoc) == 0 || nrow(org_data_cat) == 0) {
      stop(paste0("Failed to exclude org without data: ", org_id))
    }

    tryCatch(
      {
        report_data <- create_org_report(
          org_id,
          REPORT_DATE,
          items,
          org_tbl,
          assoc,
          org_data_cat,
          subsets,
          team_tbl
        )

        report_data_list[[org_id]] <- handler_util$fix_open_response_flag(
          report_data,
          survey_label
        )
        if (should_post) {
          rep_unit <- Find(function (ru) ru$id == org_id, reporting_units)
          handler_util$post_report_data(
            REPORT_DATE,
            auth_header,
            rep_unit,
            neptune_report_template,
            report_data
          )
        }
      },
      error = function (err) {
        logging$error(org_id, " : ", err)
        debugging_output$org_report_errors[[org_id]] <<- list(
          organization_id = org_id,
          error = err
        )
        report_notes$create_report_error <<- org_id %>%
          c(report_notes$create_report_error)
        return(err)
      }
    )
  }

  logging$debug("########## POST EMPTY REPORTS ##########")

  # "Empty" reports have no associated dataset and aren't intended to be viewed.
  # They exist only in the triton.report table and serve to identify to Copilot
  # and the PERTS team that RServe has chosen not to produce a visible report.

  requested_ids <- reporting_unit_ids
  posted_ids <- names(report_data_list)
  unposted_ids <- setdiff(requested_ids, posted_ids)
  noted_ids <- unique(Reduce(c, report_notes))

  # @todo: warn devs about this.
  unexplained_ids <- setdiff(unposted_ids, noted_ids)

  if (length(intersect(posted_ids, noted_ids)) > 0) {
    stop(
      "Non-rendering notes were collected on reporting units that " %+%
      "nevertheless appear to have a report: " %+%
      paste(intersect(posted_ids, noted_ids), collapse = ", ")
    )
  }

  batch_payload <- list()
  empty_reports <- list()
  for (ru_id in unposted_ids) {
    rep_unit <- Find(function(ru) ru$id %in% ru_id, reporting_units)
    rep_unit$notes <- helpers$get_empty_report_notes(ru_id, report_notes)

    # Whether or not we post the empty reports, the notes on why these reports
    # weren't rendered should be returned.
    empty_reports[[rep_unit$id]] <- rep_unit$notes

    parsed_url <- url_parse(rep_unit$post_report_url)
    call <- list(
      method = 'POST',
      path = paste0('/', parsed_url$path),
      body = handler_util$add_report_payload_ids(list(
        dataset_id = NA,
        filename = paste0(REPORT_DATE, ".html"),
        id = ru_id,
        issue_date = REPORT_DATE,
        notes = paste(rep_unit$notes, collapse = "; "),
        template = 'empty'
      ))
    )
    batch_payload[[length(batch_payload) + 1]] <- call
  }

  if (should_post) {
    # Send all the empty reports to Copilot back in one big batch, since each
    # report's data is tiny, and PATCH tells Copilot to make use of the task
    # queue, which is more robust.
    handler_util$retry(function() {
      PATCH(
        paste0(env$triton_domain(), "/api/reports"),
        add_headers(Authorization = auth_header),
        encode = "json",
        body = batch_payload
      )
    })
  }

  logging$debug("########## RESTORE OPTIONS ##########")

  options(stringsAsFactors = original_stringsAsFactors)

  metascript_output_list = list(
    report_data_list = report_data_list,
    empty_reports = empty_reports
  )

  if (length(debugging_output$classroom_report_errors) > 0) {
    logging$error("These CLASS reports experienced errors and were skipped.")
    logging$error(debugging_output$classroom_report_errors)
  }
  if (length(debugging_output$team_report_errors) > 0) {
    logging$error("These TEAM reports experienced errors and were skipped.")
    logging$error(debugging_output$team_report_errors)
  }
  if (length(debugging_output$org_report_errors) > 0) {
    logging$error("These ORG reports experienced errors and were skipped.")
    logging$error(debugging_output$org_report_errors)
  }

  # Save load point.
  if (should_save_rds) {
    saveRDS(as.list(environment()), rds_paths$return)
    logging$info("Crypt folder found, saving rds to ", rds_paths$return)
  }

  ############################################################################
  ########################## LOAD POINT: return ##############################
  ############################################################################

  # list2env(readRDS(rds_paths$return), globalenv())


  profiler$print('metascript')
  profiler$clear('metascript')

  return(metascript_output_list)
}
