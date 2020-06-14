# Generate Copilot reports with a given report template (i.e. for one program).
#
# This file handles process-level concerns, like logging, emailing
# notifications, wrangling arguments, and sending results back over the network.
# Serious data manipulation is delegated to scripts/copilot/metascript.R.
#
# Shared by EP and CCP Copilot programs.


github_base_path <- "https://raw.githubusercontent.com/PERTS/gymnast/master/"
source(paste0(github_base_path,"R/util_legacy.R"), local = TRUE)

logging <- import_module("logging")

handler_util <- import_module("modules/handler_util")
rserve_env <- import_module("modules/environment")

helpers <- import_module("scripts/copilot/engagement_helpers")
metascript <- import_module("scripts/copilot/metascript")$metascript

`%>%` <- dplyr::`%>%`

# 2017_2018_old: 'SV_6SF6JDqAKyGpTnf'
# 2018_2019: 'SV_2lskJRtnLKwHdhX'
# training: 'SV_1NZ8M7cjGFoA8OF'
QUALTRICS_SURVEY_ID <- 'SV_7VhdOnQLdNXw81D'  # 2019-20
# to edit: https://docs.google.com/spreadsheets/d/1VKJNCTLVBzqvGpq9QR_dHCEWA8Nqw5O50uG5IXKwMrc/edit#gid=0

LOGGING_SHEET_ID <- '1SHpu5yQ9Xw8bBBrZ23aONR__ug-K7nHkURUDzsfBazw'

create_all <- function (
  auth_header = NA,
  platform_data = NA,
  qualtrics_service,
  saturn_service,  # not used
  sheets_service,
  emailer,
  script_params = NA,
  should_post = TRUE, # if FALSE, data is returned but not posted, no email
  neptune_report_template
) {
  # !! These variables needed for load points.
  crypt_path <- util.find_crypt_paths(list(root_name = "rserve_data"))
  should_save_rds <- length(crypt_path$root_name) > 0  # if not found value will be character(0)
  rds_paths <- list(
    args = paste0(crypt_path$root_name,"/rds/program_reports_args.rds"),
    pre_send_reports = paste0(crypt_path$root_name,"/rds/program_reports_pre_send_reports.rds")
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

  logging$info("###### SYSTEM INFORMATION: ######### \n")
  logging$info(Sys.info())

  reporting_unit_ids <- unlist(Map(
    function(unit) unit$id,
    script_params$reporting_units
  ))

  if (is.null(script_params$use_fake_qualtrics_responses)) {
    qualtrics_responses <- qualtrics_service$get_responses(QUALTRICS_SURVEY_ID)
  } else {
    logging$info("using fake qualtrics responses \n")
    classrooms_df <- platform_data$triton$Classroom %>%
      dplyr::filter(
        # Classrooms specifically requested or related to requested teams.
        uid %in% reporting_unit_ids | team_id %in% reporting_unit_ids
      )
    qualtrics_responses <- qualtrics_service$fake_responses(
      classrooms_df,
      start_date=helpers$last_monday(),
      end_date=Sys.Date()
    )
  }

  platform_data_present <- !is.na(platform_data) && !length(platform_data$triton) %in% 0
  qualtrics_data_present <- !nrow(qualtrics_responses) %in% 0

  if (!platform_data_present) {
    logging$info("ep.R$main received no platform data")
  }
  if (!qualtrics_data_present) {
    logging$info("ep.R$main received no qualtrics data")
  }

  if (!platform_data_present || !qualtrics_data_present) {
    logging$info("ep.R$main did not receive necessary data, stopping.")
    return()
  }

  items <- utils::read.csv("config/copilot_items.csv", stringsAsFactors = FALSE)
  tbls = helpers$create_triton_tables(platform_data)

  report_date <- helpers$next_monday() # if not report date is provided, set to next monday
  if (!is.null(script_params$report_date)) {
    report_date <- script_params$report_date
  }
  ram_log <- peakRAM::peakRAM(
    metascript_output <- metascript(
      triton_tbl  = tbls$triton_tbl,
      org_tbl = tbls$org_tbl,
      user_tbl = tbls$user_tbl,
      class_tbl = tbls$class_tbl,
      team_tbl = tbls$team_tbl,
      cycle_tbl = tbls$cycle_tbl,
      program_tbl = tbls$program_tbl,
      triton_participant_tbl = tbls$participant_tbl,
      neptune_participant_tbl = platform_data$neptune$Participant,
      qualtrics_data_input = qualtrics_responses,
      items = items,
      REPORT_DATE = report_date,
      ANONYMOUS = FALSE,
      TEAM_ONLY = FALSE,
      requested_rus = reporting_unit_ids,
      run_program = script_params$run_program,
      save_workspace_only = script_params$save_workspace_only
    )
  )
  msg_list <- list(
    mem_used = round(pryr::mem_used() / (1e6), 1),
    peak_ram = ram_log$Peak_RAM_Used_MiB,
    total_ram = ram_log$Total_RAM_Used_MiB,
    elapsed_time = round(as.numeric(ram_log$Elapsed_Time_sec),2),
    ram = helpers$get_RAM()
  )
  msg_list[["ram_msg"]] <- msg_list$ram$ram_msg
  msg_list[["ram_available"]] <- msg_list$ram$ram_available
  msg_list[["ram_load_peak"]] <- round(as.numeric(msg_list$peak_ram)
                                       /as.numeric(msg_list$ram_available),2)*100
  msg_list[["ram_load_mem"]] <- round(as.numeric(msg_list$mem_used)
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

  msg <- paste0("Total memory used: ", msg_list$mem_used, "Mb.")
  logging$info("####### RAM LOAD from msg_list$mem_used ########: ", msg)

  # Save load point.
  if (should_save_rds) {
    saveRDS(as.list(environment()), rds_paths$pre_send_reports)
    logging$info("Crypt folder found, saving rds to ", rds_paths$pre_send_reports)
  }

  ############################################################################
  ###################### LOAD POINT: pre_send_reports ########################
  ############################################################################

  # # Uncomment to debug here
  # list2env(readRDS(rds_paths$pre_send_reports), globalenv())

  report_data_list <- metascript_output$report_data_list
  summaries_list <- metascript_output[!names(metascript_output) %in% "report_data_list"]

  if (!should_post) {
    return(report_data_list)
  }

  for (report_data in report_data_list) {
    # Find the matching reporting unit from Triton.
    unit <- Find(
      function (ru) ru$id == report_data$id,
      script_params$reporting_units
    )

    # POST a dataset to Neptune.
    dataset_payload = list(
      content_type = 'application/json', # either application/json or text/csv
      filename = report_data$filename, # any filename, eg. 'my_report.html'
      data = report_data # csv or json text as a unitary character vector
    )
    dataset <- handler_util$post_to_platform(
      # Any appropriate endpoint on triton or neptune, should be specified in
      # the initial request.
      unit$post_url,
      auth_header, # RSA-encrypted jwt, signed by platform's private key.
      dataset_payload
    )

    # POST a reference to this dataset to a report on Triton/Copilot.
    report_payload = list(
      dataset_id = dataset$uid,
      # template = ??  # not set here; depends on the type of reporting unit
      # preview = FALSE,  # NOTE: use this if injecting reports into Copilot from your local machine!
      filename = report_data$filename, # current convention is "YYYY-MM-DD.html"
      issue_date = report_date
    )
    if (grepl('^Team_', report_data$id)) {
      report_payload$team_id <- report_data$id
      # Team reports and Classroom reports use the same template.
      report_payload$template <- neptune_report_template
    } else if (grepl('^Classroom_', report_data$id)) {
      report_payload$classroom_id <- report_data$id
      # Team reports and Classroom reports use the same template.
      report_payload$template <- neptune_report_template
    } else if (grepl('^Organization_', report_data$id)) {
      report_payload$organization_id <- report_data$id
      # Organization/Community reports use a different template.
      report_payload$template <- paste0(neptune_report_template, '_organization')
    }

    report <- handler_util$post_to_platform(
      unit$post_report_url,
      auth_header,
      report_payload
    )
  }

  logging$debug("########## Save summaries to logging sheet ##########")

  if(is.null(sheets_service)) {
    logging$info("Sheets service is null - not writing to any sheets.")
  } else if (length(summaries_list) > 0) {
    sheets_service$add_tab(LOGGING_SHEET_ID, "Run_Log")
    sheets_service$append(
      LOGGING_SHEET_ID,
      data.frame(runs = helpers$pacific_time()),
      range_a1 = 'Run_Log!A1'
    )

    sumaries_ls_overwrite = list(
      reports_details = summaries_list$reports_details
    )
    sumaries_ls_append = list(
      team_summary = summaries_list$team_summary_tbl,
      active_rus_summary = summaries_list$active_rus_summary
    )
    # overwriter summaries
    for (summary_name in names(sumaries_ls_overwrite)) {
      try({ # use try since there might conflict with s.o. concurrently using google sheets
        Sys.sleep(1.5)
        sheets_service$add_tab(LOGGING_SHEET_ID, summary_name) # create if missing
        sheets_service$clear_tab(LOGGING_SHEET_ID, summary_name) # clean space
        sheets_service$overwrite(
          LOGGING_SHEET_ID,
          as.data.frame(sumaries_ls_overwrite[summary_name][[1]]),
          range_begin = paste0(summary_name, '!A1')
        )
        logging$debug(paste0("########## Summary sheet ",summary_name , " overwritten ##########" ))
      })
    }

    # append summaries
    for (summary_name in names(sumaries_ls_append)) {
      try({
        Sys.sleep(1.5)
        sheets_service$add_tab(LOGGING_SHEET_ID, summary_name) # create if missing
        sheets_service$append(
          LOGGING_SHEET_ID,
          as.data.frame(sumaries_ls_append[summary_name][[1]]),
          range_a1 = paste0(summary_name, '!A1')
        )
        logging$debug(paste0("########## Summary sheet ",summary_name , " appended ##########" ))
      })
    }

    logging$debug("########## Send email to developers ##########")
    logging$info("####### RAM MESSAGE: ")
    logging$info(msg_list$ram_msg)

    to <- handler_util$default_email_recipients
    subject <- paste0("RServe completed: ", neptune_report_template)
    body_text <- paste0("
        ", neptune_report_template, " metascript elapsed time: ",msg_list$elapsed_time, " seconds.",
        "

        Peak RAM: ", msg_list$peak_ram, " MiB ", msg_list$ram_load_peak,
        "% RAM load.",
        "

        Memory used: ", msg_list$mem_used, " Mb ",
        msg_list$ram_load_mem,
        "% RAM load.",
        "

        Summaries have been written to the [RServe Logging Sheet][1].

        [1]: https://docs.google.com/spreadsheets/d/", LOGGING_SHEET_ID, "/
    \n")

    possible_errors <- c()
    if (msg_list[["ram_load_peak"]] > 50 | msg_list[["ram_load_mem"]] > 50) {
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


    body_text <- paste0(body_text, '\n',summaries_list$del_req_email_msg, '\n')
    body_text <- paste0(body_text, '\n Potential Errors:\n',possible_errors, '\n')

    body_text <- paste0(body_text, '\n RAM Details:\n', msg_list$ram_msg,
                        '\n Operating System:\n', msg_list$ram$os, '\n')
    body_text <- paste0(body_text, '\n ', added_msg, '\n')

    if(is.null(emailer)) {
      logging$info("Emailer is null - not emailing.")
    } else {
      emailer(to, subject, body_text)
    }
  }


  logging$debug("########## POST summaries to Neptune ##########")
  try({
    for (summary_name in names(summaries_list)) {
      if (summary_name == "del_req_email_msg"){next}
      smr_data <- summaries_list[[summary_name]] %>%
        as.data.frame() %>% helpers$paste_tbl(., sep = "\t", include_names = TRUE)
      file_name <- paste0(summary_name, "-", report_date, ".txt")
      logging$debug("########## POSTing summary to Neptune ##########: ", file_name)

      # POST a dataset to Neptune.
      dataset_payload = list(
        content_type = 'text/csv', # text/csv
        filename = file_name, # whatever
        data = smr_data # the actual csv text (writelines) # unitary character vector
      )
      dataset <- handler_util$post_to_platform(
        paste0(rserve_env$neptune_domain(),"/api/datasets"), # note the lack of parent_id,
        # it restrains the access to superadmins
        auth_header, # real header
        dataset_payload
      )
    }
    logging$debug("########## POSTed summaries to Neptune ##########")
  })

  logging$debug("########## Successfully completed ep.R ##########")
}
