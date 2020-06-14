# Generate Copilot reports with a given report template (i.e. for one program).
#
# This file handles process-level concerns, like logging, emailing
# notifications, wrangling arguments, and sending results back over the network.
# Serious data manipulation is delegated to scripts/copilot/metascript.R.
#
# Shared by BELE-SET and C-SET Copilot programs.

json_utils <- import_module("json_utils")
logging <- import_module("logging")
perts_ids <- import_module("perts_ids")
util <- import_module("util")

handler_util <- import_module("modules/handler_util")
rserve_env <- import_module("modules/environment")

helpers <- import_module("scripts/copilot/saturn_engagement_helpers")
profiler <- import_module("profiler")
saturn_metascript <- import_module("scripts/copilot/saturn_metascript")$metascript

modules::import('dplyr', `%>%`)

`%+%` <- paste0

LOGGING_SHEET_ID <- '1SHpu5yQ9Xw8bBBrZ23aONR__ug-K7nHkURUDzsfBazw'

create_all <- function (
  auth_header = NA,
  platform_data = NA,
  qualtrics_service,
  saturn_service,
  sheets_service,
  emailer,
  script_params = NA,
  should_post = TRUE, # if FALSE, data is returned but not posted, no email
  neptune_report_template,
  survey_label
) {
  profiler$add_event('saturn_reports$create_all()')
  # !! These variables needed for load points.
  crypt_path <- util$find_crypt_paths(list(root_name = "rserve_data"))
  should_save_rds <- length(crypt_path$root_name) > 0  # if not found value will be character(0)
  rds_paths <- list(
    args = paste0(crypt_path$root_name,"/rds/saturn_reports_args.rds"),
    pre_sg_and_chris_changes = paste0(crypt_path$root_name,"/rds/pre_sg_and_chris_changes.rds"),
    summaries = paste0(crypt_path$root_name,"/rds/saturn_reports_pre_send_reports.rds")
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

  logging$info("###### SYSTEM INFORMATION: ######### \n")
  logging$info(Sys.info())

  reporting_unit_ids <- unlist(Map(
    function(unit) unit$id,
    script_params$reporting_units
  ))

  # If not report date is provided, set to next monday.
  report_date <- helpers$next_monday()
  if (!is.null(script_params$report_date)) {
    report_date <- script_params$report_date
  }

  # Find the right parameters to query Saturn: applicable participation codes
  # and date range.
  classroom_codes <- platform_data$triton$Classroom %>%
    dplyr::filter(uid %in% reporting_unit_ids) %>%
    dplyr::pull(code)

  team_entailed_codes <- platform_data$triton$Classroom %>%
    dplyr::filter(team_id %in% reporting_unit_ids) %>%
    dplyr::pull(code)

  # Save load point.
  if (should_save_rds) {
    saveRDS(as.list(environment()), rds_paths$pre_sg_and_chris_changes)
    logging$info("Crypt folder found, saving rds to ", rds_paths$pre_sg_and_chris_changes)
  }

  # # Uncomment to debug here
  # list2env(readRDS(rds_paths$pre_sg_and_chris_changes), globalenv())

  # now pull codes that were not necessarily requested but that are associated
  # with org_ids that were requested

  team_org_assoc <- platform_data$triton$Team %>%
    json_utils$expand_string_array_column(organization_ids) %>%
    dplyr::select(
      team_id = uid,
      organization_id = organization_ids
    ) %>%
    dplyr::filter(!util$is_blank(organization_id))

  if(any(duplicated(team_org_assoc))){
    stop("Duplicated combinations of team and organization ids were found in " %+%
           "the triton data. Reports not rendered.")
  }

  org_entailed_codes <- platform_data$triton$Classroom %>%
    # merge in team data with an inner join so that teams without orgs are
    # dropped (we don't need org-entailed codes for classes on teams with no
    # orgs).
    # we take the UNIQUE codes because teams and orgs have a many-to-many
    # relationship, so there could be classes on teams that correspond to multiple
    # orgs, and therefore those classes' codes would occur more than once
    merge(
      .,
      team_org_assoc,
      by = "team_id",
      all.x = FALSE,
      all.y = FALSE
    ) %>%
    # filter to the requested organization_ids. Because the ids are
    # prefixed, non-org ids like team ids will never match the values
    # in organization_id
    dplyr::filter(organization_id %in% reporting_unit_ids) %>%
    dplyr::pull(code) %>%
    unique()

  # Concatenate the requested and entailed codes, taking the unique set again
  # because classroom codes could be requested directly or be entailed by
  # requested team or orgs.
  codes <- unique(c(classroom_codes, team_entailed_codes, org_entailed_codes))

  logging$info("Limiting saturn query to codes:")
  logging$info(codes)
  # @todo: should this get set to some constant?
  start_date <- NULL
  end_date <- report_date
  logging$info(
    "Limiting saturn query to dates:",
    ifelse(is.null(start_date), "(no limit)", start_date),
    "to",
    end_date
  )

  # Get survey responses from Saturn.
  saturn_responses <- saturn_service$get_responses(
    survey_label,
    codes = codes,
    start_date = start_date,
    end_date = end_date
  )

  # @to-do move check of saturn data to HERE

  # Make sure we have everything we need, otherwise alert the dev.
  platform_data_present <- !is.na(platform_data) && !length(platform_data$triton) %in% 0
  saturn_data_present <- !nrow(saturn_responses) %in% 0
  if (!platform_data_present) {
    logging$info("cset.R$main received no platform data")
  }
  if (!saturn_data_present) {
    logging$info("cset.R$main received no saturn data")
  }
  if (!platform_data_present || !saturn_data_present) {
    logging$info("cset.R$main did not receive necessary data, stopping.")
    return()
  }

  # Do some useful initial conversions.
  imitation_qualtrics_data <- saturn_responses %>%
    dplyr::rename(
      ResponseID = firestore_id,
      StartDate = created,
      EndDate = modified
    ) %>%
    dplyr::mutate(
      Finished = ifelse(progress == 100, 1, 0),
      testing = if ("testing" %in% names(.)) testing else NA
    )

  # note that the saturn_metascript.R assumes the items df has been filtered to
  # program. If this changes, then important assumptions made by the metascript
  # will be violated.
  items <- utils::read.csv('config/copilot_items.csv', stringsAsFactors = FALSE) %>%
    dplyr::filter(saturn_uses == TRUE)
  items <- items[items[[survey_label]] == TRUE, ]

  drivers <- utils::read.csv('config/copilot_drivers.csv', stringsAsFactors = FALSE)
  drivers <- drivers[drivers[[survey_label]] == TRUE, ]

  subsets <- utils::read.csv(
    'config/copilot_subset_config.csv',
    stringsAsFactors = FALSE
  )
  subsets <- subsets[subsets[[survey_label]] == TRUE, ]

  tbls = helpers$create_triton_tables(platform_data)

  ram_log <- peakRAM::peakRAM(
    metascript_output <- saturn_metascript(
      auth_header,
      survey_label,
      neptune_report_template,
      should_post,
      triton_tbl  = tbls$triton_tbl,
      org_tbl = tbls$org_tbl,
      user_tbl = tbls$user_tbl,
      class_tbl = tbls$class_tbl,
      team_tbl = tbls$team_tbl,
      cycle_tbl = tbls$cycle_tbl,
      program_tbl = tbls$program_tbl,
      triton_participant_tbl = tbls$participant_tbl,
      neptune_participant_tbl = platform_data$neptune$Participant,
      saturn_data_input = imitation_qualtrics_data,
      items = items,
      drivers = drivers,
      subsets = subsets,
      reporting_units = script_params$reporting_units,
      reporting_unit_ids = reporting_unit_ids,
      # Optional:
      REPORT_DATE = report_date,
      emailer = emailer,
      run_program = script_params$run_program,
      save_workspace_only = script_params$save_workspace_only
    )
  )
  msg_list <- list(
    peak_ram = ram_log$Peak_RAM_Used_MiB,
    total_ram = ram_log$Total_RAM_Used_MiB,
    elapsed_time = round(as.numeric(ram_log$Elapsed_Time_sec),2),
    ram = helpers$get_RAM()
  )
  msg_list[["ram_msg"]] <- msg_list$ram$ram_msg
  msg_list[["ram_available"]] <- msg_list$ram$ram_available
  msg_list[["ram_load_peak"]] <- round(as.numeric(msg_list$peak_ram)
                                       /as.numeric(msg_list$ram_available),2)*100

  msg <- paste0("Peak RAM used: ",
                msg_list$peak_ram,
                "MiB, Total RAM used:",
                msg_list$total_ram,
                "MiB.",
                "Elapsed Time: ",
                msg_list$elapsed_time,
                " seconds.")
  logging$info("####### RAM LOAD from peakRAM ########: ", msg)


  # Save load point.
  if (should_save_rds) {
    saveRDS(as.list(environment()), rds_paths$summaries)
    logging$info("Crypt folder found, saving rds to ", rds_paths$summaries)
  }

  ############################################################################
  ###################### LOAD POINT: summaries ########################
  ############################################################################

  # list2env(readRDS(rds_paths$summaries), globalenv())

  if (!should_post) {
    return(metascript_output)
  }

  logging$debug("########## Save summaries to logging sheet ##########")

  if(is.null(sheets_service)) {
    logging$info("Sheets service is null - not writing to any sheets.")
  } else {

    sheets_service$add_tab(LOGGING_SHEET_ID, "Run_Log")
    sheets_service$append(
      LOGGING_SHEET_ID,
      data.frame(runs = helpers$pacific_time()),
      range_a1 = 'Run_Log!A1'
    )
  }

  logging$debug("########## Send email to developers ##########")



  logging$info("####### RAM MESSAGE: ")
  logging$info(msg_list$ram_msg)


  to <- handler_util$default_email_recipients
  subject <- paste0("RServe completed: ", survey_label)
  body_text <- paste0("
      ", survey_label, " metascript elapsed time: ",msg_list$elapsed_time, " seconds.

      ", ifelse(
        'email_msg' %in% names(metascript_output),
        metascript_output$email_msg %+% "\n\n      ",
        ""
      ),
      "Peak RAM: ", msg_list$peak_ram, " MiB ", msg_list$ram_load_peak, "% RAM load.

      Summaries have been written to the [RServe Logging Sheet][1].

      [1]: https://docs.google.com/spreadsheets/d/", LOGGING_SHEET_ID, "/

      IDs processed:
  \n")

  possible_errors <- c()
  if (msg_list[["ram_load_peak"]] > 50) {
    possible_errors <- c(possible_errors, "Memory load more than 50%!")
  }
  if (msg_list[["elapsed_time"]] > 1200) {
    possible_errors <- c(possible_errors, "Metascript runtime more than 20 minutes!")
  }
  if (length(possible_errors) == 0) {
    possible_errors <- " No possible errors detected."
  } else {
    possible_errors <- paste0(possible_errors, collapse = "\n")
  }

  added_msg <- c()
  if(!is.null(metascript_output$email_msg)) {
    added_msg <- metascript_output$email_msg
  }

  body_text <- paste0(body_text, '\n Potential Errors:\n',possible_errors, '\n')

  body_text <- paste0(body_text, '\n RAM Details:\n', msg_list$ram_msg,
                      '\n Operating System:\n', msg_list$ram$os, '\n')
  body_text <- paste0(body_text, '\n ', added_msg, '\n')

  if(is.null(emailer)) {
    logging$info("Emailer is null - not emailing.")
  } else {
    emailer(to, subject, body_text)
  }

  get_copilot_link <- function (report_data) {
    report_kind <- perts_ids$get_kind(report_data$id)
    copilot <- 'https://copilot.perts.net'
    if (report_kind == 'Organization') {
      collection <- 'organizations'
    } else if (report_kind == 'Team') {
      collection <- 'teams'
    } else {
      return(NA)
    }
    return(paste(
      copilot,
      collection,
      perts_ids$get_short_uid(report_data$id),
      'reports',
      sep = '/'
    ))
  }
  viewing_links <- metascript_output$report_data_list %>%
    Map(get_copilot_link, .) %>%
    Filter(Negate(is.na), .) %>%
    unlist()

  logging$info("######### REPORT VIEWING LINKS #########")
  cat("\n\n")
  cat(paste(viewing_links, collapse = "\n"))
  cat("\n\n")

  logging$debug("########## Successfully completed saturn_reports.R ##########")
}
