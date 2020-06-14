# todo add text file, report_generation_summary (one row per ordered / delievered report with basic summaries, ngraphs, etc.)

# this files contains functions for creating summaries which will be shared with
# the team via google-docs and google-keys

modules::import('lubridate')
modules::import('tidyr')
modules::import('jsonlite')
modules::import('readr')
modules::import('stats')
modules::import('dplyr')
modules::import('stringr')
modules::import('reshape2')
modules::import('ggplot2')

github_base_path <- "https://raw.githubusercontent.com/PERTS/gymnast/master/"
source(paste0(github_base_path,"R/util_legacy.R"), local = TRUE)
source(paste0(github_base_path,"R/util_qualtrics_cleaning.R"), local = TRUE)

helpers <- import_module('scripts/copilot/engagement_helpers.R')
list2env(helpers, environment())

logging <- import_module("logging")

`%>%` <- dplyr::`%>%`

create_active_rus_summary <- function(
  REPORT_DATE,
  data_cat_not_imputed,
  triton_tbl,
  class_tbl
) {
  logging$debug("########### CREATE ACTIVE RUS SUMMARIES ##################")
  logging$debug(REPORT_DATE)
  one_week_ago_start <- (lubridate::date(REPORT_DATE) - 8) %>% as.character()
  three_weeks_ago_start <- (lubridate::date(REPORT_DATE) - 8 - 2*7) %>% as.character()


  filtered_data <- data_cat_not_imputed %>%
    filter(!is.na(userID) & !is.na(class_id) & !is.na(week_start))


  one_week_span_df <- data_cat_not_imputed[filtered_data$week_start >= one_week_ago_start,]
  three_weeks_span_df <- data_cat_not_imputed[filtered_data$week_start >= three_weeks_ago_start,]



  active_rus_summary_df <- data.frame()

  #total number of participating teams
  active_rus_summary_df[1,"report_date"] <- REPORT_DATE
  active_rus_summary_df[2,"report_date"] <- REPORT_DATE
  active_rus_summary_df[1,"time_span"] <- "1 week"
  active_rus_summary_df[2,"time_span"] <- "3 weeks"
  active_rus_summary_df[1,"active_teams"] <- length(unique(one_week_span_df$team_id))
  active_rus_summary_df[2,"active_teams"] <- length(unique(three_weeks_span_df$team_id))



  active_rus_summary_df[1,"active_class"] <- length(unique(one_week_span_df$class_id))
  active_rus_summary_df[2,"active_class"] <- length(unique(three_weeks_span_df$class_id))

  active_rus_summary_df[1,"active_particip"] <- paste0(one_week_span_df$userID, one_week_span_df$class_id) %>% unique %>% length
  active_rus_summary_df[2,"active_particip"] <- paste0(three_weeks_span_df$userID, three_weeks_span_df$class_id) %>% unique %>% length


  active_rus_summary_df[1,"exp_class"] <- triton_tbl$class_id[triton_tbl$team_id %in% unique(one_week_span_df$team_id)] %>% unique %>% length
  active_rus_summary_df[2,"exp_class"] <- triton_tbl$class_id[triton_tbl$team_id %in% unique(three_weeks_span_df$team_id)] %>% unique %>% length

  active_rus_summary_df[1,"expected_n"] <- triton_tbl$expected_n[triton_tbl$team_id %in% unique(one_week_span_df$team_id)] %>% sum
  active_rus_summary_df[2,"expected_n"] <- triton_tbl$expected_n[triton_tbl$team_id %in% unique(three_weeks_span_df$team_id)] %>% sum



  #####

  # create table for Lauren which has team names, expected students and participating students
  # for the last week only
  active_team_id_1week <- unique(one_week_span_df$team_id)


  one_week_span_df$userIDclassroomID <- paste0(one_week_span_df$userID, one_week_span_df$class_id)

  team_summary_tbl_1week <- one_week_span_df %>% group_by(team_id) %>%
    summarise(
      team_name = first(team_name),
      active_class = length(unique(class_id)),
      active_n = n()
    ) %>% as.data.frame()

  team_summary_tbl_1week$time_span <- "1 week"

  team_summary_tbl_3week <- three_weeks_span_df %>% group_by(team_id) %>%
    summarise(
      team_name = first(team_name),
      active_class = length(unique(class_id)),
      active_n = n()
    ) %>% as.data.frame()

  team_summary_tbl_3week$time_span <- "3 weeks"
  team_summary_tbl <- rbind(team_summary_tbl_1week, team_summary_tbl_3week)

  triton_tbl_smr <- triton_tbl %>% group_by(team_id) %>%
    summarise(
      expected_class = length(unique(class_id)),
      expected_n = as.integer(sum(expected_n))
    )

  team_summary_tbl <- merge(
    team_summary_tbl,
    triton_tbl_smr,
    by = "team_id"
  ) %>% arrange(time_span)

  ##############################################################################
  ##### Participation Summaries - dashboard
  # https://docs.google.com/document/d/1EqS_5dTac4FBYr7eKCBvsjJIu6QhlFkN0rGeT_rlKQU/edit#

  pc_summary_txt <- ""

  # all analyses should be run only for classes active in 2018, or registered in 2018

  # Classes registered in 2018
  filtering_vector <- (as.Date(class_tbl$created) %>% year) >= 2018
  classes_registered_in_2018 <- class_tbl$uid[filtering_vector]

  # classes active in 2018
  filtering_vector <- (as.Date(data_cat_not_imputed$StartDate) %>% year) >= 2018
  classes_active_in_2018 <- data_cat_not_imputed$class_id[filtering_vector] %>% unique


  classes_in_2018 <- c(classes_registered_in_2018, classes_active_in_2018) %>% unique()

  #create filtered copies of the data and the sql table
  data_cat_not_imputed_filtered <- data_cat_not_imputed[data_cat_not_imputed$class_id %in% classes_in_2018,]
  triton_tbl_filtered <- triton_tbl[triton_tbl$class_id %in% classes_in_2018,]



  # Of users who have a class, how many surveyed students? (Have we made a product that’s compelling enough for people to want to try it?)

  percent_served_class_tbl <-
    data_cat_not_imputed_filtered %>% group_by(class_id) %>%
    summarise(serveyed = length(unique(userID))) %>%
    merge(.,
          triton_tbl_filtered,
          by = "class_id",
          all = T) %>%
    mutate(
      serveyed = ifelse(is.na(serveyed),0,serveyed),
      perc_serv = round((serveyed/as.numeric(expected_n)),2),
      eighty_or_more = ifelse(perc_serv >= .8, 1, 0)
    )
  q1 <- mean( percent_served_class_tbl$eighty_or_more) # 66% of all classrooms

  # Of users who surveyed students once (above), how many surveyed a second time? (Have we made a product that’s compelling enough that people want to keep using it?) (1)


  percent_served_twice_class_tbl <-
    data_cat_not_imputed_filtered %>% group_by(class_id, week_start) %>%
    summarise(serveyed = length(unique(userID))) %>%
    merge(.,
          triton_tbl_filtered,
          by = "class_id",
          all = T) %>%
    mutate(
      serveyed = ifelse(is.na(serveyed),0,serveyed),
      perc_serv = round((serveyed/as.numeric(expected_n)),2),
      eighty_or_more = ifelse(perc_serv >= .8, 1, 0)
    ) %>%
    group_by(class_id) %>%
    summarise(
      active_weeks = n(),
      perc_serv_aver = mean(perc_serv, na.rm = T),
      eighty_or_more_sum = sum(eighty_or_more, na.rm = T)
    )

  q2 <- sum (percent_served_twice_class_tbl$eighty_or_more_sum >= 2) /sum(percent_served_twice_class_tbl$eighty_or_more_sum >= 1)
  # 48% of all classrooms who surveyed students once also surveyed them a second time.

  # Of users who surveyed students once, how many have surveyed three times? (deep engagement) (2)

  q3 <- sum (percent_served_twice_class_tbl$eighty_or_more_sum >= 3) /sum(percent_served_twice_class_tbl$eighty_or_more_sum >= 1)
  # 24% of all classrooms who surveyed students once also surveyed them a second time


  # What % of teams focused on each LC? Important for revealing if certain LCs are weak sauce.
  data_cat_not_imputed_filtered$learning_conditions %>% unique
  lc_df <- data_cat_not_imputed_filtered
  lc_df$feedback_for_growth <- grepl("feedback-for-growth", lc_df$learning_conditions)
  lc_df$meaningful_work <- grepl("meaningful-work", lc_df$learning_conditions)
  lc_df$teacher_caring <- grepl("teacher-caring", lc_df$learning_conditions)

  lc_summary_tbl <-
    lc_df %>% group_by(team_id) %>%
    summarise(
      feedback_for_growth_average = mean(feedback_for_growth, na.rm = T),
      meaningful_work_average = mean(meaningful_work, na.rm = T),
      teacher_caring_average = mean(teacher_caring, na.rm = T)
    )

  lc_summary_tbl %>% summary

  # feedback_for_growth_average: 0.7695
  # meaningful_work_average: 0.7659
  # teacher_caring_average: 0.9569


  #How do the student questions about engagement with survey look? (esp. Dave’s proposed question of “did you rush through this”) (3)
  # this is Q71


  #Do you feel comfortable answering these questions honestly?
  # 1 yes; 2 no
  lc_df$honest_comfort %>% table()
  mean(lc_df$honest_comfort, na.rm= T)
  #My teacher will try to use my answers to this survey to make class better.
  # 1 Strongly disagree, 5 Strongly agree

  # it seems there is issue with recoding; There are only 5 options, but the sacle is 1 to 6, with 3 missing
  lc_df$teacher_use_qs <- util.recode(lc_df$teacher_use_qs, c(4,5,6), c(3,4,5))
  lc_df$teacher_use_qs %>% table()
  mean(lc_df$teacher_use_qs, na.rm= T)

  #I rushed through this survey and didn't think about my answers.
  lc_df$rushing %>% table() # No data yet

  # prepare class-level aggregated lc_df
  lc_df_class <- lc_df %>% group_by(class_id) %>%
    summarise(honest_comfort = mean(honest_comfort, na.rm = T),
              teacher_use_qs = mean(teacher_use_qs, na.rm = T),
              rushing = mean(rushing, na.rm = T)
    )

  # Start writing the dashboard analyses to an output file

  ##############################################################################
  pc_summary_txt <- paste0(
    "\n",
    "Report Date:" , REPORT_DATE, "\n",
    "\n",
    "#########################################################################\n",
    "######################## Dashboard Summary Last Week  ###################\n",
    "\n",
    "q1. Of users who have a class, how many surveyed students? (Have we made a product that’s compelling enough for people to want to try it?\n",
    "\tPercentage: ", round((q1*100),2), "%\n",
    "\n",
    "q2. Of users who surveyed students once (above), how many surveyed a second time? (Have we made a product that’s compelling enough that people want to keep using it?)\n",
    "\tPercentage: ", round((q2*100),2), "%\n",
    "\n",
    "q3. Of users who surveyed students once, how many have surveyed three times? (deep engagement)\n",
    "\tPercentage: ", round((q3*100),2), "%\n",
    "\n",
    "q4. What % of teams focused on each LC? Important for revealing if certain LCs are weak sauce.\n",
    paste0("\tfeedback_for_growth_average: ",
         lc_summary_tbl$feedback_for_growth_average %>% mean(na.rm = T) %>%
           '*'(.,100) %>%
           round(.,2),
         "%\n"),
    paste0("\tmeaningful_work_average: ",
         lc_summary_tbl$meaningful_work_average %>% mean(na.rm = T) %>%
           '*'(.,100) %>%
           round(.,2),
         "%\n"),
    paste0("\tteacher_caring_average: ",
         lc_summary_tbl$teacher_caring_average %>% mean(na.rm = T) %>%
           '*'(.,100) %>%
           round(.,2),
         "%\n"),
    "\n",
    "q5. How do the student questions about engagement with survey look?\n",
    "\n",
    "q5.1. Do you feel comfortable answering these questions honestly?\n",
    "\tScale: 1 yes, 2 no; Mean = ", round(mean(lc_df$honest_comfort, na.rm= T),2),"\n",
    "Classroom-level summary:\n",
    "FEATURE NOT ACTIVE",
    #paste0(paste0(lc_df_class$class_id,": ", round(lc_df_class$honest_comfort,2)), colapse = "\n"),
    "\n",
    "\n",
    "q5.2. My teacher will try to use my answers to this survey to make class better.\n",
    "\tScale: 1 Strongly disagree, 5 Strongly agree; Mean = ", round(mean(lc_df$teacher_use_qs, na.rm= T),2),"\n",
    "Classroom-level summary:\n",
    "FEATURE NOT ACTIVE",
    #paste0(paste0(lc_df_class$class_id,": ", round(lc_df_class$teacher_use_qs,2)), colapse = "\n"),
    "\n",
    "\n",
    "q5.2. I rushed through this survey and didn't think about my answers.\n",
    "\tScale: 1 Strongly disagree, 5 Strongly agree; Mean = ", round(mean(lc_df$rushing, na.rm= T),2),"\n",
    "Classroom-level summary:\n",
    "FEATURE NOT ACTIVE",
    #paste0(paste0(lc_df_class$class_id,": ", round(lc_df_class$rushing,2)), colapse = "\n"),
    "\n",
    "#########################################################################\n",
    "\n"
  )

  ##############################################################################

  team_summary_tbl$report_date <- REPORT_DATE
  team_summary_tbl$created <- pacific_time()
  active_rus_summary_df$created <- pacific_time()
  output_list <- list(
    active_rus_summary_df = active_rus_summary_df,
    team_summary_tbl = team_summary_tbl,
    pc_summary_txt = pc_summary_txt
  )
  return(output_list)
}

create_msg <- function(
  REPORT_DATE,
  TIME_LAG_THRESHOLD,
  requested_rus,
  data_cat_not_imputed,
  class_tbl,
  report_data_list
) {
  # creates list of values whcih will be used to create a text for the
  # email message
  oldest_start_date <- (as.Date(REPORT_DATE) - 9)
  recent_data <- data_cat_not_imputed[
    as.Date(data_cat_not_imputed$day_start) >= oldest_start_date,]


  output_ls = list(
    requested_classes = sum(grepl("Classroom",requested_rus)),
    requested_teams = sum(grepl("Team",requested_rus)),
    requested_orgs = sum(grepl("Organization",requested_rus)),
    active_classes = length(unique(recent_data$class_id)),
    active_teams = length(unique(recent_data$team_id)),
    class_reports_created = sum(grepl("Classroom", names(report_data_list))),
    team_reports_created = sum(grepl("Team", names(report_data_list)))
  )
  current_time <-
    Sys.time() %>% as.character %>%   as.POSIXct() %>%
    format(., tz="America/Los_Angeles",usetz=TRUE)
  output_message <- paste0(
    "########### Requested and delivered reports summary ######################\n",
    "Current time: ", current_time, "\n",
    "Report Date:  ", REPORT_DATE, "\n",
    "Classrooms in program: ", output_ls$requested_classes, "\n",
    "Teams in program: ", output_ls$requested_teams, "\n",
    "Communities in program: ", output_ls$requested_orgs, "\n",
    "Recently active classrooms in Qualtrics: ", output_ls$active_classes, "\n",
    "Recently active teams in Qualtrics: ", output_ls$active_teams, "\n",
    "Created classroom reports: ", output_ls$class_reports_created, "\n",
    "Created team reports: ", output_ls$team_reports_created, "\n",
    "#########################################################################\n"
  )
  return(output_message)
}

### <testing code> ###
#metascript_args <- readRDS('/Users/rumen/Sites/analysis/rserve/metascript_args.rds')
#requested_rus <- metascript_args$requested_rus
#report_data_list  <-  readRDS('/Users/rumen/Sites/analysis/rserve/metascript_output.rds')
#report_data_list_element <- report_data_list$Classroom_6NqkU5RvE8qAAF8H
### <testing code> ###

flatten_output_list <- function(output_list_element) {
  # takes a single ru from the metascript output list and creates a brief summary
  names_to_include <- c(
    "page_title",
    "report_date",
    "team_name",
    "team_id",
    "classroom_name",
    "classroom_id",
    "zero_length_table",
    "max_weeks_missing",
    #"participation_table_data",
    "fidelity_tuq",
    "fidelity_honest",
    "ru_expected_n",
    "filename",
    "id"
  )
  out_ls <- output_list_element[names(output_list_element) %in% names_to_include]
  # flatten participation table
  out_ls$part_tbl <- paste_tbl(output_list_element$participation_table_data, " ")

  #"open_responses"
  out_ls$open_responses_flat_count <-
    (output_list_element$open_responses %>% unlist %>% length) # this is approxiamte count only, not precise count
  # learning_conditions
  buffer_list <- output_list_element$learning_conditions %>% unlist
  out_ls$conditions_labels <- buffer_list[names(buffer_list) %in% "label"] %>% paste0(., collapse = ", ")
  out_ls$conditions_active <- buffer_list[names(buffer_list) %in% "active"] %>% paste0(., collapse = ", ")
  out_ls$charts_labes <- active_conditions <- grep("chart_64", names(buffer_list), value = TRUE) %>% paste0(., collapse = ", ")
  out_ls$chart_nchars <- active_conditions <- buffer_list[grep("chart_64", names(buffer_list))] %>% nchar %>% paste0(., collapse = ", ")
  return(out_ls)
}


create_req_deliv_summary <- function(requested_rus, report_data_list) {
  # creates a table with requested / delivered rus, and
  # aggregated counts for requested / delivered teams and classromms
  logging$debug("########### CREATE REQUESTED-DELIVERED RUS SUMMARIES ##################")

  deliv_req_smr <- list()
  deliv_req_smr_counts <- list()
  ru_summaries <- data.frame()
  if ( is.null(requested_rus) | length(requested_rus) == 0) {
    logging$debug("Cannot create create_req_deliv_summary, zero length lists provided as argument.")
  }
  if ( !is.null(requested_rus) | length(requested_rus) != 0) {
    deliv_req_smr[["requested_classrooms"]] <- requested_rus[grep("Classroom",
                                                               requested_rus)]
    deliv_req_smr[["requested_teams"]] <- requested_rus[grep("Team", requested_rus)]
    deliv_req_smr[["delivered_classrooms"]] <- names(report_data_list)[grep("Classroom", names(report_data_list))]
    deliv_req_smr[["delivered_teams"]] <- names(report_data_list)[grep("Team", names(report_data_list))]
    deliv_req_smr[["non_delivered_classrooms"]] <- setdiff(
      deliv_req_smr[["requested_classrooms"]],
      deliv_req_smr[["delivered_classrooms"]]
    )
    deliv_req_smr[["non_delivered_teams"]] <- setdiff(
      deliv_req_smr[["requested_teams"]],
      deliv_req_smr[["delivered_teams"]]
    )
    deliv_req_smr[["over_delivered_classrooms"]] <- setdiff(
      deliv_req_smr[["delivered_classrooms"]],
      deliv_req_smr[["requested_classrooms"]]
    )
    deliv_req_smr[["over_delivered_teams"]] <- setdiff(
      deliv_req_smr[["delivered_teams"]],
      deliv_req_smr[["requested_teams"]]
    )
    deliv_req_smr_counts <- lapply(deliv_req_smr,function(x) length(x))
  }
  # create individual summaries
  for (ru in names(report_data_list)) {
    current_df <- flatten_output_list(report_data_list[[ru]]) %>% as.data.frame(., stringsAsFactors = FALSE)
    ru_summaries <- dplyr::bind_rows(ru_summaries, current_df)
  }
  return(list(deliv_req_smr = deliv_req_smr,
              ru_summaries = ru_summaries,
              deliv_req_smr_counts = deliv_req_smr_counts))
}
