modules::import('tidyr')
modules::import('jsonlite')
modules::import('readr')
modules::import('stats')
modules::import('dplyr')
modules::import('stringr')
modules::import('reshape2')
modules::import('ggplot2')
modules::import('utils')

github_base_path <- "https://raw.githubusercontent.com/PERTS/gymnast/master/"
source(paste0(github_base_path,"R/util_legacy.R"), local = TRUE)
source(paste0(github_base_path,"R/util_qualtrics_cleaning.R"), local = TRUE)

helpers <- import_module("scripts/copilot/engagement_helpers.R")
list2env(helpers, environment())

json_utils <- import_module("json_utils")
logging <- import_module("logging")
summarize_copilot <- import_module("summarize_copilot")
util <- import_module("util")

create_report <- import_module("scripts/copilot/create_report")$create_report
community_graphs <- import_module("scripts/copilot/community_graphs")
create_active_rus_summary <- import_module("scripts/copilot/public_summaries")$create_active_rus_summary
create_req_deliv_summary <- import_module("scripts/copilot/public_summaries")$create_req_deliv_summary
create_msg <- import_module("scripts/copilot/public_summaries")$create_msg

metascript <- function(
    triton_tbl,
    org_tbl,
    user_tbl,
    class_tbl,
    team_tbl,
    cycle_tbl,
    program_tbl,
    triton_participant_tbl,
    neptune_participant_tbl,
    qualtrics_data_input,
    items,
    REPORT_DATE = next_monday(),
    ANONYMOUS = FALSE,
    TEAM_ONLY = FALSE,
    TEAMS_LIST = NULL,
    requested_rus = NULL,
    run_program = NULL,
    save_workspace_only = NULL
  ){
  # Args:
  #   TEAMS_LIST - character, optional, data will be limited selected team(s)
  #     before imputation and aggregated metrics, defaults to any teams listed
  #     in `requested_rus`.
  #   requested_rus - character, optional, only requested organizations, teams,
  #     and classrooms will have reports created.
  #   save_workspace_only - logical, default NULL, if true, and if the script
  #     finds a dedicated crypt folder, it will automatically save its input
  #     and output

  # Make sure this flag is a length-1 logical.
  save_workspace_only <- !is.null(save_workspace_only) && save_workspace_only %in% TRUE
  # save_workspace_only <- FALSE # for SG debugging line-by-line

  # !! These variables needed for load points.
  crypt_path <- util$find_crypt_paths(list(root_name = "rserve_data"))
  should_save_rds <- length(crypt_path$root_name) > 0  # if not found value will be character(0)
  rds_paths <- list(
    args = paste0(crypt_path$root_name,"/rds/metascript_args.rds"),
    pre_imputation = paste0(crypt_path$root_name,"/rds/metascript_pre_imputation.rds"),
    pre_class_reports = paste0(crypt_path$root_name,"/rds/metascript_pre_class_reports.rds"),
    pre_team_reports = paste0(crypt_path$root_name,"/rds/metascript_pre_team_reports.rds"),
    pre_org_reports = paste0(crypt_path$root_name,"/rds/metascript_pre_org_reports.rds"),
    return = paste0(crypt_path$root_name,"/rds/metascript_output_workspace.rds")
  )

  # Save load point.
  if (should_save_rds) {
    saveRDS(as.list(environment()), rds_paths$args)
    logging$info("Crypt folder found, saving rds to ", rds_paths$args)
  }

  ############################################################################
  ########################## LOAD POINT: args ################################
  ############################################################################

  # # Uncomment to debug here
  # list2env(readRDS(rds_paths$args), globalenv())
  # stop(paste0("Stopped for debugging, environment loaded from ", rds_paths$args))

  if (is.null(TEAMS_LIST)) {
    TEAMS_LIST <- grep('^Team_', requested_rus, value = TRUE)
  }

  # Separate out the organization reporting units. N.B. may have zero length.
  org_logical <- grepl('^Organization_', requested_rus)
  ORGS_LIST <- requested_rus[org_logical]
  requested_rus <- requested_rus[!org_logical]

  logging$info("metascript()")
  logging$info("REPORT_DATE:", REPORT_DATE)
  logging$info("ANONYMOUS:", ANONYMOUS)
  logging$info("TEAM_ONLY:", TEAM_ONLY)
  logging$info("Separated organization reporting units:", length(ORGS_LIST))
  logging$info("length(requested_rus):", length(requested_rus))

  # output:
  ## report_data_list a list with all inputs for all selected classes and teams

  logging$debug("########## SET OPTIONS ##########")
  logging$debug("Memory Usage: ", top_mem_objects(environment()))

  original_stringsAsFactors <- getOption('stringsAsFactors')
  options(stringsAsFactors = FALSE)

  logging$debug("########## HARDCODED PARAMETERS ##########")

  SINGLE_RUN <- FALSE
  EXCLUDE_TEAMS <- TRUE # if true, it removes the teams described in excluded_team_names
  # in the engagement_helpers.R file
  TIME_LAG_THRESHOLD <- 9 # Teams who haven't participated for more days than the threshold are excluded
  # from the report generating part.
  # I choose 9 rather than 7 days because the weekend could be confusing (it could add 2 days)
  # disaggregation groups
  # collapse together these disaggregation groups if needed for cell size
  # SUBSET_TYPES <- c("gender", "race_cat", "ELL_status")
  SUBSET_TYPES <- c("gender", "race_cat", "in_target_group")
  # reporting unit id types
  REPORTING_UNIT_ID_TYPES <- c("class_id", "team_id")
  MAX_CYCLES_MISSING <- 35 # Arbitrary huge number - since cycles are >2 wks each, this is >70 wks
  # this controls what is the longest strike of missing values which we will accept
  # before removing the participant. Any participants having missing data on all metrics
  # for longer than the parameter will be removed from the dataset.
  REPORTING_UNIT_ID_VAR_TYPES <- c("class", "team")
  MIN_CELL <- 5   # no disaggregation if resulting cells would be smaller


  # if the qualtrics input is a single dataframe I will wrap it in a list
  if (class(qualtrics_data_input) == 'data.frame') {
    qualtrics_data_input <- list(qualtrics_data_input)
  }

  logging$debug("########## DEBUGGING OUTPUT   ##########")

  debugging_output <- list(
    unimputed = NULL,
    imputed = NULL
  )

  logging$debug("########## CHECK INPUTS   ##########")

  if(!is_Monday(REPORT_DATE)) {
    stop("The REPORT_DATE is not Monday!")
  }

  # Make sure that certain columns from the google sheet are interpreted the right way
  items_character_columns <- c(
    'question_code',
    'question_text',
    'variable',
    'response_options',
    'introductory_text',
    'source_for_question_text',
    'driver'
  )
  items_numeric_columns <- c(
    'min_good',
    'max_good'
  )
  for (n in items_character_columns) {
    if (!identical(typeof(items[[n]]), "character")) {
      stop(paste0("items.csv column ", n," not read as character."))
    }
  }
  for (n in items_numeric_columns) {
    if (!identical(typeof(items[[n]]), "integer")) {
      stop(paste0("items.csv column ", n," not read as integer."))
    }
  }

  # When Qualtrics imports data, it adds extra columns starting with "DO.Q" to indicate
  # the order in which randomized response options were displayed for each respondent.
  # But the DO.Q column name contains the name of the question itself, so the code can
  # mix up the actual question with the DO.Q info. So remove all DO.Q info here.
  qualtrics_data_input[[1]][grep("DO.Q", names(qualtrics_data_input[[1]]))] <- NULL

  data_list <-  qualtrics_data_input %>%
      lapply(., function(df) repair_2nd_row(df)) %>% # repair the problem with pdds
      lapply(., function(df) qc.clean_qualtrics(df))


  # if there's more than one survey file, rbind them
  # together, keeping only the columns they share in common.
  # otherwise, data is just the first element in the list.
  if(length(data_list) > 1){
      data <- util.rbind_union(data_list)
  } else{
      data <- data_list[[1]]
  }

  # delete records for whom we have no learning conditions information
  data_raw <- data # keep raw data for additional summary reports
  data <- data[!util.is_blank(data$learning_conditions),]


  # Check Triton information:
  # If there is no Triton information in any one table (e.g. because tables are wiped for the summer),
  # then gracefully exit. Note: cycle_tbl not included because cycles aren't essential for reports.
  triton_tbls <- c("triton_tbl", "triton_participant_tbl", "user_tbl",
                   "team_tbl", "class_tbl")
  for(tbl_name in triton_tbls) {
    if(nrow(get(tbl_name)) %in% 0) {
      logging$warning("Warning: Triton table " %+% tbl_name %+% " has no rows, so reports cannot be rendered. Stopping.")
      return(list(email_msg = "One or more critical Triton tables are empty (perhaps cleared for the summer), so no reports were generated."))
    }
  }


  # add information from Triton
  # clean white spaces and non-alphanumeric characters first

  triton_tbl[ ,c("team_id", "class_name")] <-
    triton_tbl[ ,c("team_id", "class_name")]  %>%
    lapply(., util.trim) %>% as.data.frame()
    #lapply(., function(x) gsub("&", "and", x)) %>% as.data.frame()
  # it seems that white spaces break the script
  # @todo you might also consider adding non-ascii checks for names and titles

  data <- merge(data, triton_tbl, by = "code", all.x = TRUE, all.y = FALSE)

  # add in teacher information:
  # data$class_id linked to class_tbl$contact_id via class_tbl$class_id
  data <- merge(data,
                class_tbl[, c("class_id", "contact_id")],
                by = "class_id",
                all.x = TRUE,
                all.y = FALSE)

  # then linked to user_tbl$name and user_tbl$email via user_tbl$user_id
  data <- dplyr::rename(data, user_id = contact_id)
  data <- merge(data,
                user_tbl[, c("email", "name", "user_id")],
                by = "user_id",
                all.x = TRUE,
                all.y = FALSE)
  data <- rename(data,
                 teacher_id = user_id,
                 teacher_name = name,
                 teacher_email = email)

  if(nrow(data) == 0) {
    stop("Stopping script - no data exist after restricting to one team/class.")
  }


  # merge in raw student IDs from Neptune data
  neptune_participant_tbl$raw_id <- neptune_participant_tbl$name
  neptune_participant_tbl$participant_id <- neptune_participant_tbl$uid
  data <- merge(data,
                neptune_participant_tbl[, c("participant_id", "raw_id")],
                by = "participant_id",
                all.x = TRUE,
                all.y = FALSE)

  ##### Fixing responses that have no associated raw ID:
  # According to Chris, there was a database bug early on that prevented some raw IDs before 9/9 from being saved.
  # People who showed up w no raw ID got a new participant_id every time,
  # and there's no way to track them over time, so they are basically one-off anomaly students in the data.
  # To reflect how these students are seen by the system, we replace the blank raw IDs in these responses
  # with a unique random value. The participant ID of the response will do fine.
  # NOTE: test data may also have a blank raw_id if it never showed up in Neptune.
  data$raw_id[is.na(data$raw_id)] <- data$participant_id[is.na(data$raw_id)]

  # finally, create a "universal ID" (raw ID + team) with these fixed raw IDs
  data$universal_id <- data$raw_id %+% "__" %+% data$team_id

  #remove classes which are not in Triton. A user can delete a class, yet the old
  # data will still be in Qualtrics. We want to exclude Qualtrics. data from deleted classes.
  # I could also do this at the merge level, but this way is easier to check what is going on
  data <- data[!is.na(data$class_id),]

  # Remove responses that have embedded data "testing" set to "true".
  # These are teacher survey previews from Copilot.
  if(!is.null(data$testing)) { # the function was is_null(), I changed it to is.null(), I hope it was a typo
    data <- data[!data$testing %in% "true", ]
  }

  # if data has 0 rows after filtering, exit with a message
  if (nrow(data) == 0 ) {
    logging$warning("After filtering testing data and data that were unmatched to Triton, there are no rows left for processing. Exiting the metascript. Dimensions of data:",
                    paste0(dim(data), collapse = ", ")
    )
    return(list(email_msg = "After filtering testing data and data that were unmatched to Triton, there were no rows left for processing, so no reports were generated."))
  }

  # We are constantly changing which items are in the survey and thus what columns are in the data,
  # but we want to use this script to handle all possible item combinations.
  # Instead of doing a lot of if-then checking, we add NA columns for all missing items, because it's
  # like they were never asked.
  # TODO this is actually stupid and confusing, I regret putting it in and want to remove this (Dan)
  for(q_code in items$question_code) {
    if(!q_code %in% names(data)) {
      data[, q_code] <- NA
    }
  }


  # Temporarily add study ID info and some items info
  data$Study_ID <- "Study 1"
  # names(items)[names(items) %in% "lausd_general_category"] <- "driver"
  # items$driver[items$driver %in% "Belonging"] <- "belonging"
  # items$driver[items$driver %in% "Relevance"] <- "relevance"
  # items$driver[items$driver %in% c("GMS", "growth_mindset")] <- "growth_mindset_(general)"

  # remove data collected after the report creation date


  data_pre_report_date <- data[as.Date(data$StartDate) <= as.Date(REPORT_DATE),]
  if (nrow(data) != nrow(data_pre_report_date)) {
      logging$info(
        "Some records were removed from the Qualtrics data because the start date was later than the report date. N = ",
        paste0(nrow(data) - nrow(data_pre_report_date))
      )
  }
  data <- data_pre_report_date


  # This section is for restricting the dataset for testing purposes.
  # The RESTRICT_DATA_FOR_TESTING flag prevents downstream datasets from being saved.
  if (!identical(TEAMS_LIST, character(0))) {
    logging$info(
      "Restricting data to following teams:",
      paste0(TEAMS_LIST, collapse = ", ")
    )

    teams_without_data <- TEAMS_LIST[!TEAMS_LIST %in% data$team_id]
    if (length(teams_without_data) > 0) {
      logging$warning(
        "The following teams are not found in the Qualtrics data and will",
        "be excluded from further calculations:",
        paste0(TEAMS_LIST[!TEAMS_LIST %in% data$team_id], collapse = ", ")
      )
      TEAMS_LIST <- TEAMS_LIST[!TEAMS_LIST %in% teams_without_data]
    }
    data <- data[data$team_id %in% TEAMS_LIST, ]
  }

  # if data has 0 rows after filtering, exit with a message
  if (nrow(data) == 0 ) {
    logging$warning("After filtering the qualtrics data there are no rows left for processing. Exiting the metascript. Dimensions of data:",
                 paste0(dim(data), collapse = ", ")
    )
    return(list(email_msg = "After filtering the Qualtrics data there were no rows left for processing, so no reports were generated."))
  }

  # If there are NO recent data with which to render reports, log and exit
  if(all(data$StartDate < (lubridate::as_date(REPORT_DATE) - TIME_LAG_THRESHOLD))) {
    logging$warning("There are no survey responses within the pre-defined number of threshold days before the report date. Exiting the metascript.")
    return(list(email_msg = "There were no survey responses within the pre-defined number of threshold days before the report date, so no reports were generated."))
  }

  # save a copy of the original data for debugging
  data_orig <- data


  logging$debug("########## CHUNK DATES ##########")
  ##  Survey responses are grouped by day for tracking purposes

  qualtrics_date_format <- "%Y-%m-%d %H:%M:%S"
  stripped_time <- strptime( data$StartDate , qualtrics_date_format )
  data$week_start <- lubridate::floor_date(stripped_time, unit = "week") %>% as.Date()
  data$day_start <- lubridate::floor_date(stripped_time, unit = "week") %>% as.Date()


  logging$debug("########## DEFINE CYCLES ##########")
  ## We use cycles, not weeks, to track participation over time.
  ## Cycles are unique relative to teams, just like week_start.
  ## Here we use the cycle_tbl from Copilot to define cycles for teams.
  ## Chris has confirmed that cycle dates never overlap within teams.
  # For reference: 18 unique teams in the data as of Jan 2019.

  # remove cycles with an NA start_date - these might be placeholders created by Copilot
  # but they have not been defined by users and are meaningless in terms of report display.
  cycle_tbl <- dplyr::filter(cycle_tbl, !is.na(start_date))

  # sort table and format cycle dates
  cycle_tbl <- cycle_tbl %>%
    arrange(team_id, ordinal) %>%
    rename(cycle_id = uid,
          start_date_raw = start_date,
          end_date_raw = end_date)
  cycle_tbl$start_date <- lubridate::ymd(cycle_tbl$start_date_raw)
  cycle_tbl$end_date <- lubridate::ymd(cycle_tbl$end_date_raw)
  REPORT_DATE_FORMATTED <- lubridate::ymd(REPORT_DATE)

  # remove cycles that start after the report-date
  # teams may have defined these in advance, but they haven't started yet, so they shouldn't be in reports
  cycle_tbl <- cycle_tbl[cycle_tbl$start_date < REPORT_DATE_FORMATTED, ]


  # Make sure that all time from first cycle start date to today is continually covered by intervals.
  # "end_date_x" = end date eXtended.
  # The rules are:
  # If you are the most recent cycle in your team, then your end date is extended to the REPORT_DATE.
  # Otherwise, your end date is extended to the day before the start of the next cycle.
  # Note that time is NOT covered before the start of the first cycle for a team.
  cycle_tbl <- cycle_tbl %>%
    group_by(team_id) %>%
    mutate(end_date_x = lubridate::as_date(ifelse(ordinal %in% max(ordinal),
                                             REPORT_DATE_FORMATTED,
                                             lead(start_date) - lubridate::days(1))))

  # create zero-padded ordinal on single-digit ordinals for proper sorting (e.g. replace "1" with "01")
  # This ensures that "10" doesn't come right after "1" alphabetically!
  # Note: this breaks if anyone reaches 100 cycles.
  cycle_tbl$ordinal_padded <- ifelse(nchar(cycle_tbl$ordinal) %in% 1,
                                     paste0("0", cycle_tbl$ordinal),
                                     as.character(cycle_tbl$ordinal))

  # Create alphabetically-ordered cycle names based on start dates.
  # Also create "long" cycle names (with end date) for participation table. Use the eXtended end date.
  cycle_tbl$start_month_abb <- month.abb[lubridate::month(cycle_tbl$start_date)]
  cycle_tbl$start_day <- lubridate::day(cycle_tbl$start_date) %>% as.character()
  cycle_tbl$end_month_abb <- month.abb[lubridate::month(cycle_tbl$end_date_x)]
  cycle_tbl$end_day <- lubridate::day(cycle_tbl$end_date_x) %>% as.character()
  cycle_tbl <- cycle_tbl %>%
    group_by(team_id) %>%
    mutate(cycle_name = "Cycle " %+% ordinal_padded %+% " (" %+%
             start_month_abb %+% ". " %+% start_day %+% ")",
           cycle_name_long = "Cycle " %+% ordinal_padded %+% " (" %+%
             start_month_abb %+% ". " %+% start_day %+% " - " %+%
             end_month_abb %+% ". " %+% end_day %+% ")")



  logging$debug("########## MAP CYCLES TO RESPONSES ##########")
  # Here we attempt to map all survey responses to cycles in the cycle_tbl (using extended end dates).
  # Successfully-mapped responses get a cycle_name.
  # Unmapped responses are expected to fall into two categories:
  ### 1 - Responses made in a team that has never defined ANY cycles.
  ### These responses get the generic label "(no cycles defined)".
  ### 2 - Responses made before the start of a team's first cycle.
  ### These responses are CUT!


  # Helper function for assigning dates to cycle_names.
  vectorized_between <- Vectorize(between)
  get_cycle_for_date <- function(date, team_id, cycle_tbl) {
    # given an input date, an associated team_id, and a cycle_tbl,
    # return the name of the first cycle that contains the date for that team.
    # if team id is not in cycle_tbl, return "(no cycles defined)".
    # if no cycle is matched, return NA.
    if(!team_id %in% unique(cycle_tbl$team_id)) return("(no cycles defined)")
    cycle_tbl_team <- cycle_tbl[cycle_tbl$team_id %in% team_id, ]
    cycle_tbl_team$in_cycle <- vectorized_between(date,
                                                  cycle_tbl_team$start_date,
                                                  cycle_tbl_team$end_date_x)
    if(any(cycle_tbl_team$in_cycle)) {
      return(unlist(cycle_tbl_team[first(which(cycle_tbl_team$in_cycle)), "cycle_name"]))
    } else {return(NA)}
  }
  vectorized_get_cycle_for_date <- Vectorize(get_cycle_for_date, c("date", "team_id"))


  # Try to map all responses to cycles. Unmapped responses get NA for cycle_name.
  data$StartDate_formatted <- data$StartDate %>%
    gsub(" .*", "", .) %>%
    lubridate::ymd()
  data$cycle_name <- vectorized_get_cycle_for_date(data$StartDate_formatted,
                                                   data$team_id,
                                                   cycle_tbl)

  # Tag rows that have pre-first-cycle data.
  first_start_date_for_teams <- cycle_tbl %>%
    group_by(team_id) %>%
    summarise(first_cycle_start_date = first(start_date))
  data <- merge(data,
                first_start_date_for_teams,
                by = "team_id",
                all.x = TRUE)
  data$cycle_name <- ifelse(!is.na(data$first_cycle_start_date) &
                              data$StartDate_formatted < data$first_cycle_start_date,
                            "(pre-first-cycle)",
                            data$cycle_name)

  # Sanity-check: ALL ROWS should be either tagged with a cycle, tagged as being
  # from a team with no cycles defined, or tagged as being pre-first-cycle. No NAs.
  if(any(is.na(data$cycle_name))) {
    logging$warning("WARNING: some rows in the data were NOT successfully tagged as being " %+%
                    "part of a cycle, before a first cycle, or from a team that created no cycles." %+%
                      " They are NA rows and will be cut. Consider investigating further.")
  }



  # Observe early data and uncycled data!
  early_data <- data[data$cycle_name %in% "(pre-first-cycle)",
                     c("team_id", "team_name", "class_id", "class_name",
                       "StartDate_formatted", "first_cycle_start_date")]
  uncycled_data <- data[data$cycle_name %in% "(no cycles defined)",
                        c("team_id", "team_name", "class_id", "class_name",
                          "StartDate_formatted", "first_cycle_start_date")]

  # Note: the vast majority of early data is from two teams as of 1-8-19. Dan will reach
  # out to their team captains to see if they can make some cycles.

  # Note: Fair number of data points from four teams with no cycles as of 1-8-19.
  # Dan will contact their captains to encourage them to set cycles for discrete
  # intervals of time when the team was collecting data.

  # Record the earliest recorded data for each of these teams, along with the team_id,
  # so you can warn them about missing data in their reports.
  earliest_data <- early_data %>%
    group_by(team_id) %>%
    summarise(earliest_response_date = min(StartDate_formatted))

  # Cut all early data points!
  data_w_earlies <- data
  data <- data[!data$cycle_name %in% "(pre-first-cycle)", ]

  # Also map on long cycle names for participation table reporting, merging on team-cycle combo
  data <- merge(data,
                cycle_tbl[, c("team_id", "cycle_name", "cycle_name_long")],
                by = c("team_id", "cycle_name"),
                all.x = TRUE,
                all.y = FALSE)


  logging$debug("########## FIX MISLABELED USERIDS ##########")
  # About 500 universal IDs (raw ID + team ID) have TWO participant IDs associated.
  # Same raw ID on the same team, but two participant IDs. This should be impossible.
  # According to Chris, and supported by Dan's experimentation, this bug is restricted to Josh's team.
  # Josh's team started early, and students were originally added to "Triton" organization, then to "Team_...".
  # This resulted in the same raw ID getting assigned two different participant IDs,
  # all under "Team_..." in the Qualtrics data. The same people show up twice!
  # To fix this, we recode participant IDs in the Qualtrics data to be the temporally FIRST participant ID
  # that was seen to be associated with that universal ID. This merges two "students" into one.
  # Note: students might now have multiple demographic records associated with one participant ID.
  # But the next section of the metascript only propagates the FIRST demographic response. Should be fine.

  # Get the first participant id seen for each universal ID
  universal_id_to_first_participant_id <- data %>%
    arrange(universal_id, week_start) %>%
    group_by(universal_id) %>%
    summarise(first_participant_id = first(participant_id))
  # Merge in
  data <- merge(data,
              universal_id_to_first_participant_id,
              by = "universal_id",
              all.x = TRUE,
              all.y = FALSE)
  # Overwrite and clean up
  data$participant_id <- data$first_participant_id
  rm(universal_id_to_first_participant_id)


  logging$debug("########## FILL IN MISSING DEMOGRAPHICS ##########")
  logging$debug("Memory Usage: ", top_mem_objects(environment()))

  ##  Link over time by userID so that students don't have to keep filling it out
  demographic_items <- items$question_code[items$demographics]
  # Make sure that we only filter on demographic items that are actually
  # present in the data. The items csv may have items that aren't in the data,
  # and that's fine.

  demographic_items_present <- demographic_items[demographic_items %in% names(data)]
  data <- data %>% rename(userID = participant_id)

  # Bug fix: when dropping pre-cycles records, we might be dropping demographic information too.
  # First records contain demographic information, while later records do not.
  # I will check if missing demographic information could be filled in using the excluded data.

  user_demographics_pre_cycles <- data_w_earlies[c("participant_id", "StartDate_formatted",
                                                   demographic_items_present)] %>%
    rename(userID = participant_id)

  # remove userIDs which are observed in the pre-cycle data only,
  # and then sort by time and remove all rows after the first one (when demogs were collected).
  # So each row represents the first measure for a user.
  present_userIDs <- unique(data$userID)
  user_demographics <- user_demographics_pre_cycles %>%
    dplyr::filter(userID %in% present_userIDs) %>%
    arrange(userID, StartDate_formatted) %>%
    group_by(userID) %>%
    mutate(ordinal = 1:length(userID)) %>%
    dplyr::filter(ordinal %in% 1)
  user_demographics$ordinal <- NULL

  # get rid of all-blank demographic rows (don't want to propagate these values!)
  user_demographics$all_blank <- apply(
      user_demographics[demographic_items_present],
      1,
      function(x) all(util.is_blank(x))
  )
  user_demographics <- user_demographics[!user_demographics$all_blank, ] %>%
    select(-all_blank)

  # now, user_demographics contains authoritative demographic information about demographics
  # for every user. So, we'll go ahead and merge it into the master dataset,
  # and retain the user_demographics values as authoritative over whatever is there now
  # be it NA or not

  data_wdem <- merge(
      data,
      user_demographics,
      by = "userID",
      all = TRUE,
      suffixes = c("_origXXX", "")
  ) %>%
    dplyr::select(-matches("_origXXX"))





  # make sure the merge didn't drop any rows! (it shouldn't, but you never know.)
  if(!nrow(data) == nrow(data_wdem)){
      stop("propagating demographic data resulted in added or dropped rows!")
  }

  data <- data_wdem
  rm(data_wdem)

  # We're migrating to using db tables as directly as possible. Restore these
  # table's original form.
  triton.participant <- util$prefix_columns(triton_participant_tbl, 'participant')
  neptune.participant <- neptune_participant_tbl %>%
    dplyr::select(-raw_id, -participant_id) %>%
    util$prefix_columns('participant')

  # Use a generic function to merge triton participant data to the "real"
  # participant id, stored in Neptune.
  # level: participant
  # cols:
  # * participant_id
  # * in_target_group
  merged_ppts <- summarize_copilot$merge_participants(
    triton.participant,
    neptune.participant
  )

  data <- data %>%
    dplyr::left_join(merged_ppts, by = c(userID = "participant_id")) %>%
    dplyr::mutate(
      in_target_group = ifelse(
        in_target_group %in% 1,
        "Target Grp.",
        "Not Target Grp."
      )
    )

  logging$debug("########## FILTER TESTING DATA ##########")

  testing_data <- dplyr::filter(data, testing %in% 'true')
  data <- dplyr::filter(data, !testing %in% 'true')

  if (nrow(testing_data) > 0) {
    logging$info("Dropped testing responses: " %+% nrow(testing_data))
  }

  logging$debug("########## TEAM TARGET MESSAGES ##########")

  target_msg_df <- data %>% group_by(team_id) %>%
    summarise(in_target_count = sum(in_target_group %in% "Target Grp.", na.rm = T))
  team_tbl <- merge(target_msg_df,
                    team_tbl,
                    by = "team_id",
                    all.x = T,
                    all.y = T)
  team_tbl$in_target_count[is.na(team_tbl$in_target_count)] <- 0
  team_tbl$target_msg <- NA
  team_tbl$target_msg <- ifelse(
    team_tbl$in_target_count == 0 & util.is_blank(team_tbl$target_group_name),
    "Your team has not defined a target group. To learn more and define a target group for your team, check out the website_xxx.",
    team_tbl$target_msg)
  team_tbl$target_msg <- ifelse(
    team_tbl$in_target_count == 0 & !util.is_blank(team_tbl$target_group_name),
    "Your team’s target group is “group_name_XXXX”, but no students have been marked as in this group.  To mark students, update your classroom roster at website_yyy.",
    team_tbl$target_msg)
  team_tbl$target_msg <- ifelse(
    team_tbl$in_target_count != 0 & util.is_blank(team_tbl$target_group_name),
    "Your team has not defined a target group, but there are students marked on classroom rosters as being in a target group. To learn more and define a target group for your team, check out the website_xxx.",
    team_tbl$target_msg)
  team_tbl$target_msg <- ifelse(
    team_tbl$in_target_count != 0 & !util.is_blank(team_tbl$target_group_name),
    "Your team’s target group is “group_name_XXXX”.",
    team_tbl$target_msg)

  # replace placeholders with correct strings
  f <- function(pattern,replacement,input) {gsub(pattern,replacement,input)}
  gsub_vect <- Vectorize(f, SIMPLIFY = FALSE)
  team_tbl$target_msg <- gsub_vect("group_name_XXXX",team_tbl$target_group_name, team_tbl$target_msg) %>% unlist %>% unname
  team_tbl$target_msg <- gsub_vect("website_xxx",'<a href="https://www.perts.net/engage/faq#what-is-target-group">Copilot FAQ</a>', team_tbl$target_msg) %>% unlist %>% unname
  team_tbl$target_msg <- gsub_vect("website_yyy",'<a href="https://www.copilot.perts.net">copilot.perts.net</a>', team_tbl$target_msg) %>% unlist %>% unname

  rm(target_msg_df)



  logging$debug("########## DEFINE PRIVACY & DISAGGREGATION FIELDS ##########")
  ##  Define fields used for disaggregation and how to group values if MIN_CELL
  ##  size is not met.




  logging$debug("########## IDENTIFY THE PRESENT_METRICS ##########")
  logging$debug("Memory Usage: ", top_mem_objects(environment()))
  ##  The present_metrics are the questions that will be reported on.
  ##  To qualify, data must be present and corresponding driver_desc must exist.
  ##  A second check will be run at each subsetting level to ensure that
  ##  reporting would not violate MIN_CELL.

  all_drivers  <- items$driver %>% unique
  all_drivers <- all_drivers[!util.is_blank(all_drivers)]
  all_drivers <- setdiff(all_drivers, "NA")
  all_metrics  <- items$question_code[items$metric_for_reports] %>% unique
  data_all_metrics <- data[,names(data) %in% all_metrics]

  n_answers_by_column <- util.apply_columns( data_all_metrics, function(col){
      sum( !is.na(col) )
  })

  # calculate all present metrics
  present_metrics <- names(n_answers_by_column)[n_answers_by_column > MIN_CELL]
  # drop metrics that are actually subsetting fields (demographics)
  present_metrics <- present_metrics[present_metrics %in% all_metrics]

  # check driver_desc for duplicated drivers
  if(any(duplicated(names(driver_desc)))){
    stop("the object driver_desc found in engagement_helpers.R has duplicate driver labels. Reports cannot be generated.")
  }

  # drop drivers that don't appear in driver_desc, and warn
  undescribed_drivers <- all_drivers[!all_drivers %in% names(driver_desc)]
  if(length(undescribed_drivers) > 0){
      all_drivers <- setdiff(all_drivers, undescribed_drivers)
      logging$warning(
        "The following drivers do not match drivers in driver_desc (an object",
        "sourced in from" %+% paste0(undescribed_drivers, collapse = ", ") %+% ".",
        "These drivers will NOT appear in the reports unless you either",
        "(1) add a description to driver_desc, or (2) change the driver label ",
        "in the items .csv to match one of the existing drivers in driver_desc."
      )
  }


  logging$debug("########## HANDLE DUPLICATE AND BLANK USERS ##########")
  ## Whenever users are duplicated within team/cycle/reporting unit, retain the LAST response

  data_dups_handled <- data %>%
      dplyr::filter(!util.is_blank(userID)) %>%
      dplyr::group_by(class_id, userID, cycle_name) %>%
      arrange(StartDate) %>%
      mutate(
          id_instance = 1:length(userID),
          id_instances = n()
      ) %>%
      dplyr::filter(id_instances == 1 | id_instance == id_instances) %>%
      as.data.frame


  logging$debug("########## CONVERT CATEGORICAL DATA TO STRINGS ##########")

  data_cols_not_in_items <- c()
  data_cat <- data_dups_handled # data_cat - categorical data

  # @SG note: this checks whether each variable is categorical
  # (per its designation in the `items` file).
  # If an item is labeled as "categorical" in `items`, then
  # the item_options field in `items` is used to recode the
  # values in the data. In `items`, the recoded values are
  # contained in a single cell and delimited with a ';'.

  for(col in names(data_dups_handled)){
      item_index <- which(items$question_code %in% col)
      if(length(item_index) > 1){
          logging$warning("Too many matches for", col)
      } else if(length(item_index) == 0){
          data_cols_not_in_items <- c(data_cols_not_in_items, col)
      } else if(items[item_index,]$categorical){
          item_options <- strsplit(items[item_index,]$response_options,";")[[1]]
          data_cat[,col] <- as.character(data_cat[,col])
          data_cat[,col] <-
              util.recode(data_cat[,col], 1:length(item_options), item_options)
      }
  }

  data_cat <- util.apply_columns(data_cat, util.trim) %>%
      util.apply_columns(util.as_numeric_if_number)

  logging$debug("########## AGGREGATE RACE CHECKBOX CATEGORIES ##########")

  ## Clean checkbox categories for easier working:

  # Setup
  race_chk_qs <- grep("race_chk", items$question_code, value = TRUE)
  race_chk_data <- data_cat[, race_chk_qs]
  # If there's anything in the race checkbox data except 1 or NA, throw an error
  unique_vals_in_race_chk_data <- unique(unlist(race_chk_data))
  if(length(setdiff(unique_vals_in_race_chk_data, c(1, NA))) > 0) {
    stop("Error - something in the race checkbox data is not 1 or NA.")
  }
  # Turn them into nice Booleans and rename them
  fix_boolean <- function(vec) {ifelse(is.na(vec), 0, vec) %>% as.logical()}
  race_chk_qs_nice_names <- grep("race_chk_", items$variable, value = TRUE)
  data_cat[, race_chk_qs_nice_names] <- util.apply_columns(data_cat[, race_chk_qs], fix_boolean)


  ## Reduce cleaned checkboxes to a 7-checkbox system:

  data_cat$race_partially_NatAm <- data_cat$race_chk_NatAmAlaskan
  data_cat$race_partially_Asian <- data_cat$race_chk_EastAsian | data_cat$race_chk_SoutheastAsian |
    data_cat$race_chk_SouthAsian | data_cat$race_chk_OtherAsian
  data_cat$race_partially_HispLat <- data_cat$race_chk_MexChicano | data_cat$race_chk_PuertoRican |
    data_cat$race_chk_CentAm | data_cat$race_chk_OtherHispLat
  data_cat$race_partially_Black <- data_cat$race_chk_AfAmBlack | data_cat$race_chk_African |
    data_cat$race_chk_Carib | data_cat$race_chk_OtherBlack
  data_cat$race_partially_White <- data_cat$race_chk_Euro | data_cat$race_chk_MidEast |
    data_cat$race_chk_OtherWhite
  data_cat$race_partially_PacIsl <- data_cat$race_chk_PacIsl
  data_cat$race_partially_Other <- data_cat$race_chk_Other


  ## Reduce 7-checkbox system to 6-category conventional system, using assumption of
  ## assigning people to most marginalized group based on test scores
  data_cat$race6 <- NA
  data_cat$race6 <- ifelse(data_cat$race_partially_Other, "Other", data_cat$race6)
  data_cat$race6 <- ifelse(data_cat$race_partially_White, "White", data_cat$race6)
  data_cat$race6 <- ifelse(data_cat$race_partially_Asian | data_cat$race_partially_PacIsl, "Asian or PacIsl", data_cat$race6)
  data_cat$race6 <- ifelse(data_cat$race_partially_HispLat, "Hispanic or Latino", data_cat$race6)
  data_cat$race6 <- ifelse(data_cat$race_partially_NatAm, "Native American", data_cat$race6)
  data_cat$race6 <- ifelse(data_cat$race_partially_Black, "Black or African American", data_cat$race6)


  logging$debug("########## CREATE SUPERSETS OF DISAGGREGATION GROUPS ##########")
  ##  To prevent small groups from killing displays

  # race: make a new "race_cat" broken up into advantaged vs. not
  # First, combine race labels from different sources into one source for aggregation decisions.
  # Use checkbox question unless checkbox is NA and old question is not NA.
  data_cat$race_for_agg <- ifelse(is.na(data_cat$race6) & !is.na(data_cat$race_1), data_cat$race_1, data_cat$race6)
  # Define relatively advantaged race categories.
  # Note - need a range of overlapping options because race_for_agg draws from the race_1 system AND the race6 system.
  advantaged_races <- c("White","Asian or Asian American",
                        "Asian or PacIsl", "Native Hawaiian or Other Pacific Islander")
  disadvantaged_races <- c("Black or African American", "American Indian or Alaska Native", "Native American",
                           "Filipino", "Two or More Races/Ethnicities", "Hispanic or Latino")
  # Define 2-category system.
  data_cat$race_cat <- NA
  data_cat$race_cat[data_cat$race_for_agg %in% advantaged_races] <- "Struct. Adv."
  data_cat$race_cat[data_cat$race_for_agg %in% disadvantaged_races] <- "Struct. Disadv."

  # gender:
  # assuming that the gender question is gender_v2
  # Use the response options to recode gender
  gender_codes <- items[items$question_code %in% "gender_v2", "response_options"] %>%
    strsplit(., split = "; ") %>% unlist()
  data_cat$gender <- util.recode(data_cat$gender_v2,
                                 c(1:3),
                                 gender_codes)
  # make everyone who is not male or female NA for display purposes
  data_cat$gender[! data_cat$gender %in% c("Male", "Female")] <- NA

  # Language: compute ELL_status
  # If a student chooses "Another language" in ell_1 and is bellow 4 on ell_2, it is considered an ELL
  data_cat$ELL_status <- NA

  is_eng_speaker_1 <- data_cat$ell1_1 == "English"
  is_eng_speaker_2 <- (data_cat$ell1_1 == "Another language") &
    (data_cat$ell2_1 %in% c("Very well", "Extremely well"))
  is_eng_learner <- (data_cat$ell1_1 == "Another language") &
    (data_cat$ell2_1 %in% c("Not well at all", "Slightly well", "Medium well"))

  data_cat$ELL_status <- ifelse(is_eng_speaker_1, "Eng. Speaker", data_cat$ELL_status )
  data_cat$ELL_status <- ifelse(is_eng_speaker_2, "Eng. Speaker", data_cat$ELL_status)
  data_cat$ELL_status <- ifelse(is_eng_learner, "Eng. Learner", data_cat$ELL_status )
  #table(data_cat$ELL_status, data_cat$ell1_1, exclude = NULL)
  #table(data_cat$ELL_status, data_cat$ell2_1, exclude = NULL)

  logging$debug("########## SUBDIVIDE REPORTS  ##########")
  logging$debug("Memory Usage: ", top_mem_objects(environment()))
  ##  Make data frames listing info about teams and classes

  team_study_df <- data_cat[,c("team_id", "team_name", "Study_ID")]
  team_study_df <- unique(team_study_df)
  team_study_class_df_list <- list() # contains dfs of all Team x Study x Class

  # all observations must have a class name parameter
  data_cat$class_name[
      util.is_blank(data_cat$class_name)
  ] <- "Not reporting"


  # SG note: For each Team/Study combo, pull out the classes that were
  # included. Generates `team_study_class_df_list`, which is a list
  # of data.frames with team_id, Study_ID, and all unique classes
  # found for that team_id/Study_ID combination in the data.
  for( i in 1:nrow(team_study_df) ){
      # what classes were included in the Team x Study?
      classes <- data_cat$class_name[
          # team column row i
          data_cat$team_id %in% team_study_df$team_id[i] &
              # study column row i
              data_cat$Study_ID %in% team_study_df$Study_ID[i]
      ]
      unique_classes <- strsplit(classes,",") %>% unlist %>% unique
      team_study_class_df_list[[i]] <- expand.grid(
          team_id=team_study_df$team_id[i],
          team_name=team_study_df$team_name[i],
          Study_ID=team_study_df$Study_ID[i],
          class_name=unique_classes,
          stringsAsFactors = FALSE
      )
  }

  # Rbind them into a single data.frame with one row per Team/study/class
  # combination present in the data
  if(length(team_study_class_df_list) == 1){
    team_study_class_df <- team_study_class_df_list[[1]]
  } else{
    team_study_class_df <- util.rbind_many(team_study_class_df_list)
  }

  team_study_class_df <- team_study_class_df[!util.is_blank(team_study_class_df$class_name),]
  # remove demo and test teams
  if (EXCLUDE_TEAMS) {
    team_study_class_df <-
      team_study_class_df[
        !team_study_class_df$team_id %in% excluded_team_ids,]
  }

  if( SINGLE_RUN ){
      # just run the report on the first class
    team_study_class_df <- team_study_class_df[1,]

      # used for targeting subsets of interest
      target_subset_feature <- ""  # e.g., race
      target_subset_level   <- ""  # e.g., "Latino"
  }

  data_cat_not_imputed <- data_cat #keep it for printing participation table


  # Save load point.
  if (should_save_rds) {
    saveRDS(as.list(environment()), rds_paths$pre_imputation)
    logging$info("Crypt folder found, saving rds to ", rds_paths$pre_imputation)
  }

  ############################################################################
  ######################## LOAD POINT: pre_imputation ########################
  ############################################################################

  # Uncomment to debug here
  # list2env(readRDS(rds_paths$pre_imputation), globalenv())
  # stop(paste0("Stopped for debugging, environment loaded from ", rds_paths$pre_imputation))



  logging$debug("########## DATA IMPUTATION ##########")


  # The workflow for data imputation is as follows:

  ## Save a copy of the pre-imputation data
  ## Set up important variables and cut nonsense rows (blank rows, same-cycle duplicates)
  ## Add all possible user-time rows, and flag them as such
  ## Remove rows that should not be kept for imputation:
  ### rows that are before anyone ever started
  ### entire students who have been missing longer than MAX_CYCLES_MISSING
  ## impute NAs downward to fill in vars of interest within subjects
  ## Save a copy of the post-imputation data


  logging$debug("########## Save a copy of the pre-imputation data")

  # If the script is not in debug-mode or otherwise restricting the data set,
  # then save a copy of the non-imputed data.
  if(!is.null(TEAMS_LIST)) {
    debugging_output$unimputed <- data_cat
  }

  logging$debug("########## Save an auxilary data frame for k-anonymization")
  # if there is monuted Rserve crypt, save the current data frame as csv
  if (should_save_rds) {
    f_path <- paste0(crypt_path$root_name,"/rds/anonymization_input_df.rds")
    saveRDS(data_cat, f_path)
    f_path <- paste0(crypt_path$root_name,"/csv/anonymization_input_df.csv")
    utils::write.csv(data_cat, f_path, row.names = FALSE)
    logging$info("Crypt folder is found. Anonymization input is saved.")
  }

  logging$debug("########## Set up important variables and cut duplicates within cycle")

  # Choose columns you want to impute
  col_names_to_impute <- present_metrics[present_metrics %in% colnames(data_cat)]

  # Define combinatorial ID that uniquely identifies students nested within their groups:
  # Study, team, class, user
  data_cat$comb_id <-
    paste(data_cat$Study_ID,
          data_cat$team_id,
          data_cat$class_id,
          data_cat$userID,
          sep = "@")

  # cut nonsense rows: duplicates within the same cycle (keep the last record)
  data_cat <- data_cat %>% arrange(desc(EndDate))
  data_cat <- data_cat[!duplicated(data_cat[,c("comb_id", "cycle_name")]),]

  # cut nonsense rows: rows where the student logged in but never answered one LC item
  data_cat$at_least_one_metric <-
    apply(data_cat[, col_names_to_impute], 1, any_non_na)
  data_cat <- data_cat[data_cat$at_least_one_metric, ]

  # record which is the first cycle on record for each student, and merge that info back in
  first_cycle_df <- data_cat %>%
    dplyr::group_by(comb_id) %>%
    summarise(first_cycle_in_record = min(cycle_name))
  data_cat <- merge(
    data_cat,
    first_cycle_df,
    by = "comb_id",
    all.x = TRUE,
    all.y = FALSE)

  # tag the rows here as real originals
  data_cat$imputed_row <- FALSE


  logging$debug("########## Add all possible user-cycle rows, and flag them as such")

  # create df with all combinations of comb-id and POSSIBLE cycles for that comb-id
  # (Note: possible cycles depend on the team to which the comb-id belongs.)

  comb_id_to_team <- data_cat %>%
    group_by(comb_id) %>%
    summarise(team_id = first(team_id))

  all_comb_id_cycle_combos <- merge(comb_id_to_team,
                                    cycle_tbl[, c("team_id", "cycle_name")],
                                    by = "team_id",
                                    all = TRUE) %>%
    util.apply_columns(as.character) %>%
    select(-team_id) %>%
    dplyr::filter(!is.na(comb_id)) %>%  # Some teams created cycles but have no data
    dplyr::arrange(comb_id, cycle_name)

  # If no cycles are defined for a comb-id's team, then that comb_id gets a line with "(no cycles defined)".
  all_comb_id_cycle_combos$cycle_name[is.na(all_comb_id_cycle_combos$cycle_name)] <- "(no cycles defined)"

  # merge the real data with the all-combinations df to create blank rows for unobserved cycles,
  # flag the blank rows as not originals,
  # and merge it with first_cycle_df to add info about the first cycle of real data.
  data_cat_w_blanks <- merge(
    data_cat,
    all_comb_id_cycle_combos,
    by = c("comb_id", "cycle_name"),
    all = TRUE
  ) %>%
    mutate(imputed_row = ifelse(is.na(imputed_row), TRUE, imputed_row)) %>%
    select(-first_cycle_in_record) %>%
    merge(.,
          first_cycle_df,
          by = "comb_id",
          all.x = TRUE,
          all.y = FALSE)

  # Fill in missing identity values in the blank cycle rows using the comb_id
  str_split_matrix <- strsplit(data_cat_w_blanks$comb_id, "@") %>%
    unlist %>%
    matrix(ncol = 4, byrow = TRUE)
  data_cat_w_blanks[, c("Study_ID", "team_id", "class_id", "userID")] <- str_split_matrix


  # clean up
  rm(first_cycle_df, str_split_matrix, all_comb_id_cycle_combos)

  logging$debug("########## Remove rows that should not be kept for imputation:")

  logging$debug("########## Remove rows that are before anyone ever started")

  # Each student has a first cycle that they showed up - this is stored in first_cycle_in_record.
  # only keep rows where cycle_name is <= first_cycle_in_record.
  # This works because cycle names are strictly alphabetical.
  data_cat_trim_early_non_data <- data_cat_w_blanks[data_cat_w_blanks$first_cycle_in_record <= data_cat_w_blanks$cycle_name, ]

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
  students_missing_too_long <- data_cat_trim_early_non_data %>%
    arrange(comb_id, desc(cycle_name)) %>%
    dplyr::group_by(comb_id) %>%
    summarise(missing_too_long = num_entries_before_val(imputed_row, FALSE) > MAX_CYCLES_MISSING) %>%
    dplyr::filter(missing_too_long %in% TRUE)


  # drop missing students
  data_cat_trimmed <- data_cat_trim_early_non_data[!data_cat_trim_early_non_data$comb_id %in%
                                                       students_missing_too_long$comb_id, ]

  # clean up
  # rm(students_missing_too_long)

  logging$debug("########## impute NAs downward to fill in vars of interest within subjects")
  logging$debug("Memory Usage: ", top_mem_objects(environment()))

  #add SUBSET_TYPES to col_names_to_impute, otherwise the current imputation does not
  # impute demographic vars (saved in SUBSET_TYPES)
  col_names_to_impute <- c(col_names_to_impute, SUBSET_TYPES)
  # add team_id to cols_to_impute. In the previous version we did not have to do this
  # but now we have use team_id as part of the index, so we have to add the team_id manually
  col_names_to_impute <- c(col_names_to_impute, "team_id", "class_name", "team_name",
                           "code", "expected_n", "teacher_name", "teacher_email")

  # Within each comb_id, impute in all NA values below a non-NA value of col_names_to_impute.
  data_cat_imputed_down <- data_cat_trimmed %>%
    group_by(comb_id) %>%
    fill(one_of(col_names_to_impute), .direction = "down")

  # now do backwards imputation -
  # Within each comb_id, impute in all NA values ABOVE a non-NA value of col_names_to_impute.
  data_cat_imputed <- data_cat_imputed_down %>%
    group_by(comb_id) %>%
    fill(one_of(col_names_to_impute), .direction = "up") %>%
    as.data.frame()  # turns it back from tibble to df for consistency

  row.names(data_cat_imputed) <- NULL


  logging$debug("########## Imputation complete!")
  logging$debug("Memory Usage: ", top_mem_objects(environment()))

  # overwrite data_cat
  data_cat <- data_cat_imputed

  # If the script is not in debug-mode or otherwise restricting the data set,
  # then save a copy of the imputed data.
  if(!is.null(TEAMS_LIST)) {
    debugging_output$imputed <- data_cat
  }


  logging$debug("########## BUILD COMPLETE PARTICIPATION TABLE (LONG FORMAT) ##########")
  logging$debug("Memory Usage: ", top_mem_objects(environment()))
  # Inputs:
  #  data_cat [team_id x class_id x student_ID x cycle_name]
  #  data_cat_not_imputed [team_id x class_id x student_ID x cycle_name]
  # Outputs:
  #  part_data_long_exp [cycle_name]: Participation data, long format, not expanded (only shows defined cycles)

  # First, create a vector of ID var names for any reporting units that we want to aggregate.
  reporting_unit_id_var_id_names <- REPORTING_UNIT_ID_VAR_TYPES %+% "_id"

  #  We already have expected_ns for each class in the Triton table -
  #  group and unique/sum them to make expected_team_ns and expected_classroom_ns tables.

  # This is a little helper function to get the expected ns for each value of a given type
  # of reporting unit ID. It assumes that the data table argument has a column called "expected_n"
  # and another one that is the id_var.
  get_expected_ns_table <- function(id_var, data_table) {

    # sanity checks
    if(! "expected_n" %in% names(data_table)){
      stop("In get_expected_ns_table, data_table requires a column called 'expected_n'")
    }
    if(! id_var %in% names(data_table)){
      stop("In get_expected_ns_table, id_var column " %+% id_var %+% " not found in data_table")
    }

    my_table <- data_table %>%
      dplyr::group_by_at(vars(one_of(id_var))) %>%
      summarise(expected_n = sum(expected_n, na.rm = TRUE)) %>%
      as.data.frame()
    my_table$reporting_unit_id <- as.vector(my_table[, id_var])
    my_table[, id_var] <- NULL
    return(my_table)
  }

  # Use the helper function above to create one giant expected_ns table for all reporting units.
  expected_ns <- lapply(reporting_unit_id_var_id_names, get_expected_ns_table, triton_tbl) %>%
    util.rbind_intersection()

  # Melt all the data to get long participation data -
  # Each row is defined by a reporting_unit_id/cycle_name_long combination.
  part_data_long_exp <- data_cat_not_imputed %>%
    melt(id.vars = c("cycle_name_long"),
         measure.vars = reporting_unit_id_var_id_names,
         variable.name = "reporting_unit_type",
         value.name = "reporting_unit_id") %>%
    dplyr::group_by(reporting_unit_id, cycle_name_long) %>%
    summarise(n = n())

  #  Calculate percentages at scale for every cell and format to char as needed
  part_data_long_exp <- merge(part_data_long_exp,
                              expected_ns,
                              by = "reporting_unit_id",
                              all.x = TRUE)
  part_data_long_exp$n_formatted <- ((100 * part_data_long_exp$n) / part_data_long_exp$expected_n ) %>%
    round(., 0)
  part_data_long_exp$n_formatted <- part_data_long_exp$n %+% " (" %+% part_data_long_exp$n_formatted %+% "%)"
  part_data_long_exp <- dplyr::rename(part_data_long_exp, cycle_name = cycle_name_long)



    logging$debug("########## CREATING AGG_METRICS: MAKE SUPER-MELTED RECENT DATA ##########")
    logging$debug("Memory Usage: ", top_mem_objects(environment()))

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

    days_since_last_vist_per_team_df <- compute_days_since_last_visit(
        grouping_var = data_cat$team_id,
        date_var = lubridate::date(data_cat$StartDate),
        current_date = lubridate::date(REPORT_DATE)
        )

    recent_data_teams <-
      days_since_last_vist_per_team_df[days_since_last_vist_per_team_df$min_lag <= TIME_LAG_THRESHOLD, "grouping_var"]

    recent_cycled_teams_df <- cycle_tbl %>%
      mutate(days_since_last_cycle_mod = lubridate::date(REPORT_DATE) - lubridate::date(modified),
             recent_cycle_mod = days_since_last_cycle_mod <= 7) %>%
      group_by(team_id) %>%
      summarise(recent_cycle_mod = any(recent_cycle_mod)) %>%
      dplyr::filter(recent_cycle_mod %in% TRUE)

    # Controls which team and class reports are rendered. They must
    # 1. have responses in the past week
    # 2. have a scheduled cycle that includes the report date
    # NOTE: this assumes that cycle dates have already been extended.
    active_teams_strict <- intersect(
      recent_data_teams,
      recent_cycled_teams_df$team_id
    )

    # NOTE: this definition is old and may still be wrong. Notice
    # how team that EITHER have recent data OR are in recent cycled are in
    # active_teams.
    active_teams <- c(recent_data_teams, recent_cycled_teams_df$team_id)

    if (length(active_teams) == 0) {
      stop("Stopping script - no active teams to include in the reports.")
    }

    data_cat_active_teams <- data_cat[data_cat$team_id %in% active_teams, ]

    # Identify and merge in most recent cycles for each team
    most_recent_cycles_per_team <- data_cat_not_imputed %>%
      arrange(team_id, cycle_name) %>%
      dplyr::group_by(team_id) %>%
      summarise(most_recent_observed_cycle = last(cycle_name))
    data_cat_active_teams <-  merge(data_cat_active_teams, most_recent_cycles_per_team, by = "team_id", all.x = TRUE)

    # Melt all_metrics columns down into rows
    d_melted_active <- data_cat_active_teams %>%
      melt(id.vars = c(REPORTING_UNIT_ID_TYPES, "userID", "cycle_name", SUBSET_TYPES,
                       "most_recent_observed_cycle", "imputed_row", "team_name", "class_name"),
           measure.vars = all_metrics,
           variable.name = "metric") %>%
      rename(metric_value = value)

    # Add in metric-level information and calculate if the response is in the good range
    d_melted_active <- d_melted_active %>%
      merge(., items[,c("question_code","min_good","max_good")],
            by.x = "metric", by.y = "question_code",
            all.x = TRUE) %>%
      mutate(good_range = (metric_value >= min_good &
               metric_value <= max_good))

    # Now melt subsets down into rows
    d_super_melted_active <- d_melted_active %>%
      melt(id.vars = c(REPORTING_UNIT_ID_TYPES, "userID", "cycle_name", "metric", "metric_value",
                       "most_recent_observed_cycle", "good_range", "imputed_row",  "team_name", "class_name"),
           measure.vars = SUBSET_TYPES,
           variable.name = "subset_type") %>%
      rename(subset_value = value)

    # Some people have NA for some subset values (e.g. no gender recorded), even after imputation.
    # Since we have melted subset values down into the rows, and we are going to calculate
    # aggregate statistics for subset types and subset values, we need to remove the rows with
    # NA subset values. This will not affect the rest of each participant's data.
    d_super_melted_active <- d_super_melted_active[!is.na(d_super_melted_active$subset_value), ]

    # Filter super-melted data to most recent observed week per team
    d_super_melted_active_recent <- d_super_melted_active[d_super_melted_active$cycle_name %in%
                                                            d_super_melted_active$most_recent_observed_cycle, ]



    logging$debug("########## CREATING AGG_METRICS: GET P-VALS FOR RECENT DATA ##########")
    logging$debug("Memory Usage: ", top_mem_objects(environment()))

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
        summarise(p = suppressWarnings(p_chi_sq(good_range, subset_value)))
      names(ru_pval_data)[names(ru_pval_data) %in% r] <- "reporting_unit_id"
      pvals_dfs[[r]] <- ru_pval_data
    }

    all_pvals_df <- util.rbind_intersection(pvals_dfs)


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
                  se = se(good_range),
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
                  se = se(good_range),
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
    all_metric_results_df <- util.rbind_intersection(c(subset_metric_results_dfs, all_students_metric_results_dfs))

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

    # add text versons of the % good values
    agg_metrics$pct_good_text <- ifelse(is.nan(agg_metrics$pct_good), NA, round(agg_metrics$pct_good * 100, 2))%>%
        round(., 0) %>%
        as.character %+% "%" %>%
        ifelse(. == "NA%", "", .)

    # check that nothing weird happened with text-percentage blanks
    # all blank values in pct good correspond to blank pct_good_text values
    if(!all(util.is_blank(agg_metrics$pct_good_text[util.is_blank(agg_metrics$pct_good)]))){
        stop("In the newly-programmed text-percentages feature, some blank percent-good-text values " %+%
             "were spotted where non-blank values were present in the raw pct_good column. Investigate further.")
    }
    if(!all(util.is_blank(agg_metrics$pct_good[util.is_blank(agg_metrics$pct_good_text)]))){
        stop("In the newly-programmed text-percentages feature, some blank percent-good values " %+%
                 "were spotted where non-blank values were present in the pct_good_text field. Investigate further.")
    }

    # Also need to make a subset_label column
    agg_metrics$subset_label <- ifelse(agg_metrics$subset_value %in% "All Students", "All Students",
                                       agg_metrics$subset_value %+% " Students")

    # Save off an RDS file for debugging/dashboard
    # saveRDS(agg_metrics, 'metascript_agg_metrics.rds')

    logging$debug("########## BUG FIX ##########")
    # create different team_study_class_df based on triton_tbl
    # it seems that we can have different classes using the same name within a team
    # this currently breaks the script, so I will switch to different looping structure
    # @todo clean previous team_study_class_df
    team_study_class_df <- triton_tbl[,
                                               c("team_id",
                                                 "team_name",
                                                 "class_name",
                                                 "code",
                                                 "class_id"
                                               )]
    team_study_class_df$Study_ID <- "Study 1"
    # exclude teams, if needed
    if (EXCLUDE_TEAMS) {
      team_study_class_df <-
        team_study_class_df[
          !team_study_class_df$team_id %in% excluded_team_ids,]
    }

    # exclude teams for which we do not have any data
    unique_codes <- data_cat$code %>% unique
    team_study_class_df  <- team_study_class_df[team_study_class_df$code %in% unique_codes,]


    logging$debug("########## CREATE LOG FILES ##########")

    #create log files to track what is happening during rendering
    basic_log <- team_study_class_df[, c("team_id", "class_name", "Study_ID", "code", "class_id")]
    basic_log <- basic_log %>%
      rename(study_id = Study_ID)
    basic_log$`Eng. Speaker` <- NA
    basic_log$`Eng. Learner` <- NA
    basic_log$`Struct. Adv.` <- NA
    basic_log$`Struct. Disadv.` <- NA
    basic_log$Male <- NA
    basic_log$Female <- NA
    basic_log$`All Students` <- NA
    basic_log$file_name <- NA
    basic_log$file_present <- FALSE
    basic_log$error_msg <- NA # currently not working
    basic_log$team_only <- NA
    basic_log$most_recent_week_n <- NA

    detailed_log <- data.frame()


    logging$debug("########## CUT INACTIVE TEAMS FROM REPORTING ##########")
    logging$debug("Memory Usage: ", top_mem_objects(environment()))

    #
    # find all classes which are members for teams which violate the time lag threshold,
    ## and remove those classes from the DF of classes to run
    ## this is already computed in active_teams_strict above
    team_study_class_df <- team_study_class_df[team_study_class_df$team_id %in% active_teams_strict, ]

    # create control structure for team only reports
    team_study_df <- team_study_class_df[!duplicated(team_study_class_df$team_id),]




    logging$debug("########## RENDER ENGAGEMENT DIAGNOSTICS ##########")
    logging$debug("Memory Usage: ", top_mem_objects(environment()))


    # If team-only, pass in "TEAM" as name for all class-level reports
    if (TEAM_ONLY) {
      team_study_class_df$class_name <- "TEAM"
    }
    # Save current value of TEAM_ONLY because it will get modified in the Rmd
    orig_TEAM_ONLY <- TEAM_ONLY



    logging$debug("########## create list which will select which teams and classes will be included in the report generation")

    # If you have a value for "run_program", use it to derive requested_rus
    if(!is.null(run_program)) {

      # There should be a single value in program_tbl$label that matches run_program;
      # use that to get the corresponding program uid
      program_uid <- program_tbl[program_tbl$label %in% run_program, "uid"]
      if(!length(program_uid) %in% 1) {
        msg <- "Zero or >1 program IDs were found in the program_tbl matching the value in the argument run_program. Exiting."
        logging$error(msg)
        stop(msg)
      }

      # use the program uid to find rus (teams or classes)
      ORGS_LIST <- org_tbl[org_tbl$program_id %in% program_uid, "organization_id"]
      requested_teams <- team_tbl[team_tbl$program_id %in% program_uid, "team_id"]
      requested_classes <- class_tbl[class_tbl$team_id %in% requested_teams, "class_id"]
      requested_rus <- c(requested_teams, requested_classes)

    }

    if (is.null(requested_rus)) {
      msg <- "No reporting units were requested. Exiting."
      logging$error(msg)
      stop(msg)
    }

    ru_request <-  separate_reporting_units(requested_rus, team_study_class_df)
    # select classes
    team_study_class_df <- team_study_class_df[team_study_class_df$class_id %in% ru_request$requested_classes,]
    # select teams
    team_study_df <- team_study_df[team_study_df$team_id %in% ru_request$requested_teams,]

    # Stop the script if no reports are going to be generated
    if((length(ru_request$requested_classes) %in% 0 & length(ru_request$requested_teams) %in% 0)) {
      logging$info("There are not enough recent responses to create any new class- or team-level reports.
                    Exiting the metascript.")
      return(list(report_data_list = list()))
    }


    logging$debug("########## CSV SUMMARIES ##########")
    # create summaries to share with the rest of the team
    summaries_df <- create_active_rus_summary(
      REPORT_DATE = REPORT_DATE,
      data_cat_not_imputed,
      triton_tbl = triton_tbl,
      class_tbl = class_tbl
    )
    # for testing, save them as files, later post them to Neptune
    logging$debug("File dimensions:", dim(summaries_df$active_rus_summary_df))


    # Save load point.
    if (should_save_rds) {
      saveRDS(as.list(environment()), rds_paths$pre_class_reports)
      logging$info("Crypt folder found, saving rds to ", rds_paths$pre_class_reports)
    }

    ############################################################################
    #################### LOAD POINT: pre_class_reports #########################
    ############################################################################

    # # Uncomment to debug here
    # list2env(readRDS(rds_paths$pre_class_reports), globalenv())
    # stop(paste0("Stopped for debugging, environment loaded from ", rds_paths$pre_class_reports))


    logging$debug("########## CLASS-LEVEL REPORTS ##########")

    report_data_list = list()


    if (nrow(team_study_class_df) > 0) {
    for(i in 1:nrow(team_study_class_df)) {

        # Setup
        logging$debug("Starting class: ", i)
        report_name <- NA

        # Retrieve the original value of TEAM_ONLY from outside the loop
        TEAM_ONLY <- orig_TEAM_ONLY

        # Declare local variable names for this report
        team_name           <- team_study_class_df[i,"team_name"]
        study_id            <- team_study_class_df[i,"Study_ID"]
        class_id        <- team_study_class_df[i,"class_id"]
        code                <- team_study_class_df[i,"code"]
        class_name      <- team_study_class_df[i,"class_name"]
        team_id             <- team_study_class_df[i,"team_id"]
        target_msg <- team_tbl$target_msg[team_tbl$team_id == team_id]

        # if class was not found, replace with NONE
        if(length(class_id) == 0 )  class_id <- "NONE" %+% (sample(1:10000,1) %>% as.character)
        if(util.is_blank(class_id))  class_id <- "NONE" %+% (sample(1:10000,1) %>% as.character)


        # filter the big agg_metrics df to this particular report
        agg_metrics_small <- agg_metrics[agg_metrics$reporting_unit_id %in% c(team_id, class_id), ]

        # if class_id is TEAM (why??) rename
        if(class_id == "TEAM")  class_id <- "TEAM" %+% (sample(1:10000,1) %>% as.character)


        # Cut down the big participation table to the relevant reporting unit(s):
        # If the report is team-only, cut it to team, otherwise class and its team.
        if(TEAM_ONLY) {
            ru_ids_for_part_table <- c(team_id)
            ru_names_for_part_table <- c(team_name)
            } else {
            ru_ids_for_part_table <- c(team_id, class_id)
            ru_names_for_part_table <- c(team_name, class_name)
            }
        # make participation table for these RUs and fix column names
        participation_table_df <- part_data_long_exp %>%
          dplyr::filter(reporting_unit_id %in% ru_ids_for_part_table) %>%
          dplyr::select(-n, -expected_n) %>%
          dcast(cycle_name ~ reporting_unit_id, value.var = "n_formatted") %>%
          arrange(cycle_name)
        # add an order variable to confirm later cutting of this df in the Rmd
        participation_table_df$cycle_order <- 1:nrow(participation_table_df)
        # order the columns so team is always before classroom
        team_column_name <- names(participation_table_df)[grep("Team_", names(participation_table_df))]
        class_column_name <- names(participation_table_df)[grep("Classroom_", names(participation_table_df))]
        participation_table_df <- participation_table_df[, c("cycle_name", team_column_name,
                                                             class_column_name, "cycle_order")]
        names(participation_table_df) <- c("Cycle", ru_names_for_part_table, "cycle_order")


        # Assemble the open responses for this class and most recent cycle
        class_raw_data <- data_cat_not_imputed[data_cat_not_imputed$class_id %in% class_id, ]
        class_most_recent_cycle <- class_raw_data$cycle_name %>% unique %>% sort %>% last
        all_or_question_ids <- items[items$open_response, "question_code"]
        class_recent_open_responses <- class_raw_data[class_raw_data$cycle_name %in% class_most_recent_cycle,
                                                      all_or_question_ids]

        # Calculate fidelity information for this class (honesty and TUQ):
        temp <- list()
        for(q_name in c("teacher_use_qs", "honest_comfort")) {
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
        fidelity_tuq <- temp[["teacher_use_qs"]]
        fidelity_honest <- temp[["honest_comfort"]]


        # Set up report name and path
        report_name  <- class_id %+% "." %+% REPORT_DATE %+% ".html" %>% gsub(" ", "", .)
        if (TEAM_ONLY) { report_name  <- team_id %+% "." %+% REPORT_DATE %+% ".html" %>% gsub(" ", "", .) }
        #if (ANONYMOUS) { report_name = "anonymous.html" }
        report_path   <- report_name

        # Get ready to run!
        logging$info("Preparing to run report:", report_name)
        error_msg <- NA
        basic_log$file_name[basic_log$class_id == class_id] <- report_name

        # This step renders the .Rmd file. Note that the .Rmd file
        # is not configured to be renderable on its own, and relies on
        # variables from the metascript global namespace.
        if (!save_workspace_only) {
          tryCatch(
            {
              ls_out <- create_report(
                SUBSET_TYPES = SUBSET_TYPES,
                REPORT_DATE = REPORT_DATE,
                TEAM_ONLY = TEAM_ONLY,
                REPORTING_UNIT_ID_TYPES = REPORTING_UNIT_ID_TYPES,
                MAX_CYCLES_MISSING =  MAX_CYCLES_MISSING,
                ANONYMOUS = ANONYMOUS,
                MIN_CELL = MIN_CELL,
                study_id = study_id,
                team_id = team_id,
                class_id = class_id,
                data_cat = data_cat,
                team_name = team_name,
                class_name = class_name,
                agg_metrics_small = agg_metrics_small,
                participation_table_df = participation_table_df,
                class_recent_open_responses = class_recent_open_responses,
                all_or_question_ids = all_or_question_ids,
                items = items,
                fidelity_tuq = fidelity_tuq,
                fidelity_honest = fidelity_honest,
                earliest_data = earliest_data,
                target_msg = target_msg
              )
              ls_out$filename <- REPORT_DATE %+% ".html"
              ls_out$id <- ls_out$classroom_id
              report_data_list[[ls_out$classroom_id]] <- ls_out
              # ls_out_json <- toJSON(ls_out, pretty=TRUE, auto_unbox = TRUE)
              ####
              #ls_out %>%
              #write_lines(
              #  paste0(
              #    "/Users/rumen/Sites/analysis/engagement_diagnostic/example_output",
              #    report_name,
              #    ".json"
              #  )
              #)
              ####
            },
            error = function (err) {
              logging$error(report_name, ": ", err)
              return(err)
            }
          )
        }
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

    # # Uncomment to debug here
    # list2env(readRDS(rds_paths$pre_team_reports), globalenv())
    # stop(paste0("Stopped for debugging, environment loaded from ", rds_paths$pre_team_reports))

    if (is.null(TEAMS_LIST)) {
      TEAMS_LIST <- grep('^Team_', requested_rus, value = TRUE)
    }

    logging$debug("########## TEAM ONLY REPORTS ##########")

    # create team reports for each of the teams

    # create team-only for-loop control data frame
    if (nrow(team_study_df) > 0) {
      for(i in 1:nrow(team_study_df)) {

        # Setup
        logging$debug("Starting team: ", i)
        report_name <- NA
        TEAM_ONLY <- TRUE

        # Declare local variables (why declare class info when these are team reports?)
        team_name           <- team_study_df[i,"team_name"]
        study_id            <- team_study_df[i,"Study_ID"]
        class_id        <- team_study_df[i,"class_id"]
        code                <- team_study_df[i,"code"]
        class_name      <- team_study_df[i,"class_name"]
        team_id             <- team_study_df[i,"team_id"]
        target_msg <- team_tbl$target_msg[team_tbl$team_id == team_id]

        # filter the big agg_metrics df to this particular report:
        agg_metrics_small <- agg_metrics[agg_metrics$reporting_unit_id %in% c(team_id), ]

        # Set up report name and path
        report_name  <- team_id %+% "." %+% REPORT_DATE %+% ".html" %>%
          gsub(" ", "", .)
        #if (ANONYMOUS) {report_name = "anonymous.html"}

        # Cut down the big participation table to the team,
        # and make participation table and fix column names
        # In this case, it is the team.
        ru_ids_for_part_table <- c(team_id)
        ru_names_for_part_table <- c(team_name)
        participation_table_df <- part_data_long_exp %>%
          dplyr::filter(reporting_unit_id %in% ru_ids_for_part_table) %>%
          dplyr::select(-n, -expected_n) %>%
          dcast(cycle_name ~ reporting_unit_id, value.var = "n_formatted") %>%
          arrange(cycle_name)
        names(participation_table_df) <- c("Cycle", ru_names_for_part_table)


        # Calculate fidelity information for this team (honesty and TUQ):
        team_raw_data <- data_cat_not_imputed[data_cat_not_imputed$team_id %in% team_id, ]
        team_most_recent_cycle <- team_raw_data$cycle_name %>% unique %>% sort %>% last
        temp <- list()
        for(q_name in c("teacher_use_qs", "honest_comfort")) {
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
        fidelity_tuq <- temp[["teacher_use_qs"]]
        fidelity_honest <- temp[["honest_comfort"]]



        report_path   <- report_name

        # Get ready to run!
        logging$info("Preparing to run report:", report_name)
        error_msg <- NA

        # this step renders the .Rmd file. Note that the .Rmd file
        # is not configured to be renderable on its own, and relies on
        # variables from the metascript global namespace. The main
        # input to the .Rmd file is the object `data_cat` which is
        # the Qualtrics data that's had categorical variables recoded.
        basic_log$file_name[basic_log$class_id == class_id] <- report_name
        if (!save_workspace_only) {
          tryCatch(
            {
              ls_out <- create_report(
                SUBSET_TYPES = SUBSET_TYPES,
                REPORT_DATE = REPORT_DATE,
                TEAM_ONLY = TEAM_ONLY,
                REPORTING_UNIT_ID_TYPES = REPORTING_UNIT_ID_TYPES,
                MAX_CYCLES_MISSING =  MAX_CYCLES_MISSING,
                ANONYMOUS = ANONYMOUS,
                MIN_CELL = MIN_CELL,
                study_id = study_id,
                team_id = team_id,
                class_id = class_id,
                data_cat = data_cat,
                team_name = team_name,
                class_name = class_name,
                agg_metrics_small = agg_metrics_small,
                participation_table_df = participation_table_df,
                class_recent_open_responses = data.frame(), # I will pass an empty object here
                all_or_question_ids = NA,
                items = items,
                fidelity_tuq = fidelity_tuq,
                fidelity_honest = fidelity_honest,
                earliest_data = earliest_data,
                target_msg = target_msg
              )
              ls_out$filename <- REPORT_DATE %+% ".html"
              ls_out$id <- team_id
              report_data_list[[team_id]] <- ls_out
              # ls_out_json <- toJSON(ls_out, pretty=TRUE, auto_unbox = TRUE)
              #ls_out %>%
              #  write_lines(
              #    paste0(
              #      "/Users/rumen/Sites/analysis/engagement_diagnostic/example_output",
              #      report_name,
              #      ".json"
              #    )
              #  )
              ####
            },
            error = function (err) {
              logging$error(report_name, ": ", err)
              return(err)
            }
          )
        }
      }
    }

    logging$debug("########## ORGANIZATION REPORTS ##########")

    # Save load point.
    if (should_save_rds) {
      saveRDS(as.list(environment()), rds_paths$pre_org_reports)
      logging$info("Crypt folder found, saving rds to ", rds_paths$pre_org_reports)
    }

    ############################################################################
    ###################### LOAD POINT: pre_org_reports #########################
    ############################################################################

    # # Uncomment to debug here
    # list2env(readRDS(rds_paths$pre_org_reports), globalenv())
    # stop(paste0("Stopped for debugging, environment loaded from ", rds_paths$pre_org_reports))

    # Create an easy-to-use association table showing how all orgs, teams, and
    # classrooms are nested.
    org_team_class <- team_tbl %>%
      json_utils$expand_string_array_column(organization_ids) %>%
      dplyr::rename(organization_id = organization_ids) %>%
      dplyr::filter(organization_id %in% ORGS_LIST) %>%
      dplyr::left_join(class_tbl, 'team_id') %>%
      dplyr::select(organization_id, team_id, team_name, class_id, code)

    # We may want these in the future, but probably not before we let the
    # Qualtrics pipeline (EP and CCP) die off.
    logging$info(
      "Organization/Community reports disabled for the Qualtrics-based " %+%
      "pipeline. See scripts/copilot/metascript.R"
    )
    ORGS_LIST <- character() # disables for loop below

    # Recall that triton organizations are the same as copilot communities.
    for (org_id in ORGS_LIST) {
      org_name <- org_tbl[org_tbl$organization_id %in% org_id, "organization_name"]
      assoc <- dplyr::filter(org_team_class, organization_id %in% org_id)
      org_data_cat <- data_cat %>%
        dplyr::filter(class_id %in% assoc$class_id) %>%
        dplyr::rename(participant_id = userID)

      # The structure of this list must match the expectations of the Neptune
      # and Triton APIs. Don't change it without coordinating w/ Dev Team.
      report_data <- list()

      # plot_teams_by_week returns a list of lists of base_64 graphs
      # the first level groups by "question_code" (i.e., metric)
      # the second level has 1+ graphs. Each graph only has up to 4 teams.
      # For a full description, see comments above function.
      report_data$team_by_week_plots <- community_graphs$plot_teams_by_week(
        report_date = REPORT_DATE,
        organization_associations = assoc,
        items,
        org_data_cat,
        base_64=TRUE
      )

      # change_by_team_subgroup returns a list of lists
      ##  The 1st layer of the list is indexed on each question_code.
      ##  The 2nd layer of the list is indexed as
      ##  "label" for printable metric name or "deltas" for a delta data.frame.
      # For a full description, see comments above function.
      report_data$delta_table <- community_graphs$change_by_team_subgroup(
        report_date = REPORT_DATE,
        organization_associations = assoc,
        items,
        org_data_cat
      )

      report_data$id <- org_id
      report_data$organization_id <- org_id
      report_data$organization_name <- org_name
      report_data$report_date <- REPORT_DATE
      report_data$filename <- REPORT_DATE %+% ".html"

      report_data_list[[org_id]] <- report_data
    }

    logging$debug("########## RESTORE OPTIONS ##########")
    logging$debug("Memory Usage: ", top_mem_objects(environment()))

    options(stringsAsFactors = original_stringsAsFactors)

    if (save_workspace_only) {
      logging$debug("########## SAVE_WORKSPACE_ONLY ##########")
      logging$debug("not creating any reports")
      del_req_smr <- list(ru_summaries = list())
      del_req_email_msg <- NULL
    } else {

      # create relivered requested for the email message
      del_req_email_msg <- create_msg(REPORT_DATE,
                                      TIME_LAG_THRESHOLD,
                                      c(ORGS_LIST, requested_rus),
                                      data_cat_not_imputed,
                                      class_tbl,
                                      report_data_list)

      # create count summary for requested / delivered reports for the GAE logs
      del_req_smr <- create_req_deliv_summary(requested_rus, report_data_list)

      # log info for delivered / requested files
      msg <-  paste0(
        names(del_req_smr$deliv_req_smr_counts),
        "=",
        del_req_smr$deliv_req_smr_counts
      ) %>%
      paste0(., collapse = ", ")
      logging$debug("Counts of requested and delivered reports:", msg)

      del_req_smr$ru_summaries$part_tbl <- del_req_smr$ru_summaries$part_tbl %>% gsub("\n",";",.)
    }

    metascript_output_list = list(
      report_data_list = report_data_list,
      reports_details = del_req_smr$ru_summaries,
      active_rus_summary = summaries_df$active_rus_summary_df,
      team_summary_tbl = summaries_df$team_summary_tbl,
      del_req_email_msg = del_req_email_msg
    )



    # Save load point.
    if (should_save_rds) {
      saveRDS(as.list(environment()), rds_paths$return)
      logging$info("Crypt folder found, saving rds to ", rds_paths$return)
    }

    ############################################################################
    ########################## LOAD POINT: return ##############################
    ############################################################################

    # # Uncomment to debug here
    # list2env(readRDS(rds_paths$return), globalenv())
    # stop(paste0("Stopped for debugging, environment loaded from ", rds_paths$return))

  return(metascript_output_list)
}







