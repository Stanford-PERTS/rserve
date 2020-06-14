modules::import(
  'dplyr',
  `%>%`,
  'arrange',
  'case_when',
  'distinct',
  'filter',
  'group_by',
  'inner_join',
  'last',
  'left_join',
  'mutate',
  'mutate_at',
  'mutate_if',
  'n',
  'one_of',
  'pull',
  'rename',
  'rowwise',
  'select',
  'summarise',
  'ungroup'
)
modules::import('lubridate', 'as_date', 'weeks')
modules::import('ggplot2')
modules::import('scales', 'percent_format')
modules::import('reshape2', 'dcast', 'melt')
modules::import('tidyr', 'fill')
modules::import('stats', 'rnorm')

graphing <- import_module("graphing")
helpers <- import_module("scripts/copilot/saturn_engagement_helpers")
logging <- import_module("logging")
util <- import_module("util")

`%+%` <- paste0

###############################################################
###
###   Helper Functions
###
###############################################################


html_pct_deltas <- function(vec){
  # NAs become "--"
  # numbers become percentage point strings with + or -
  if(! is.numeric(vec) ){ return(vec) }
  vec_round <- round(vec,2) * 100
  vec_str <- ifelse(vec_round > 0,
                    "+" %+% vec_round %+% "%",
                    vec_round %+% "%")
  ifelse(is.na(vec_str),"–",vec_str)
}


###############################################################
###
###
###   impute_to_week returns a melted tibble with columns:
###   team_id, participant_id, week_start, question_code, value, value_imputed
###   where each user has inputed data for a question_code if that variable was
###   collected at all on their team in that given week.
###   Inputation proceeds down and then up.
###   (ie, if it was an "active week" for the team x question_code)
###
###   see test__impute_to_week()
###
###   Note: impute_to_week should possibly reside elsewhere
###   because it performs a data transformation that could
###   prove useful in other contexts. For now, this is the only
###   place using it.
###
###############################################################


impute_to_week <- function(org_data_cat, metrics){
  # Each report_metric (question_code) must be imputed to WEEK separately
  # otherwise there would be extra "dots" for weeks that were not collected
  # including after data collection stopped.
  melt_ids <- c("team_id", "participant_id", "week_start")

  # integrity: if metrics are factors, turn them into characters
  metrics <- as.character(metrics)

  # integrity: Remove metrics that are not present & numeric.
  metrics_filtered <- metrics[metrics %in% names(org_data_cat)]
  if( length(metrics_filtered) == 0 ){
      warning("None of the metrics to report exist in the org_data")
      return(NULL)
  }

  numeric_metrics_bool <- util$apply_columns(
                            as.data.frame(org_data_cat[,metrics_filtered]),
                            is.numeric) %>% unlist()
  numeric_metrics <- metrics_filtered[numeric_metrics_bool]

  # responses_melted is a tibble of present, reportable responses.
  # Those are the only responses that need to be imputed.
  responses_melted <- org_data_cat %>%
    select(one_of(melt_ids, numeric_metrics)) %>%
    filter( ! is.na(week_start) ) %>%
    melt(id.vars=melt_ids) %>%
    filter( ! is.na(value) ) %>%
    rename( question_code = variable )

  # retain only the most recent response if a participant
  # has multiple responses in a given week
  responses_melted_last <- responses_melted %>%
    group_by(team_id, participant_id, week_start, question_code) %>%
    summarise(value = last(value)) %>%
    ungroup()

  # below confirms that metrics are not already imputed.
  # i.e., different counts of responses
  # responses_melted %>%
  #     group_by(team_id,question_code,week_start) %>%
  #     summarise(n = n())

  # In what weeks did each team actively collect data?
  # If a student is missing data from a question_code in a week in which
  # their team was collecting data, data will need to be imputed for that
  # question_code x week combination for that student.
  active_weeks <- responses_melted_last %>%
    select(team_id, week_start, question_code) %>%
    distinct()
  user_on_teams <- org_data_cat %>%
    select(team_id, participant_id) %>%
    distinct()

  # for imputation, you need every combination of active week for a user x var
  imputation_base <- inner_join(active_weeks, user_on_teams, by="team_id")

  # rmv = responses, melted, variables
  rmv <- left_join(imputation_base, responses_melted_last,
                   by=c("team_id","participant_id","week_start","question_code"))

  # rmvi = responses, melted, variables
  rmvi <- rmv %>%
            group_by(team_id, participant_id, question_code) %>%
            arrange(week_start) %>%
            mutate( value_imputed = value ) %>%
            fill( value_imputed, .direction="downup") %>%
            ungroup() %>%
            arrange(participant_id, week_start) %>%
            mutate_if(is.factor, as.character)

  return(rmvi)
}


###############################################################
###
###   default_subgroups
###   Returns a data.frame of features and corresponding
###   subgroup values for which separate summaries should be
###   generated.
###   @Sarah, this is designed to fail gracefully. It should
###   work in BELE-SET or C-SET or other programs, depending
###   on what target groups are there. You can update labels
###   here if you want them to be reflected in the delta table.
###
###
###############################################################

default_subgroups <- function(program_id=NA){
  # this can be adapted to return different subgroups for different programs
  data.frame(
    feature = c("race_cat","gender_cat","target_group_cat","financial_stress_cat"),
    value   = c("Struct. Disadv.","Female", "Target Grp.","High Fin. Str."),
    label   = c("Str. Dis. Race","Female","Target Grp.","Fin. Stressed")
  ) %>% mutate_if( is.factor, as.character)
}

###############################################################
###
###   validate_items stops execution
###   with an error message if items data frame is missing
###   required columns or the is_good" range is not set
###   for reportable items.
###
###############################################################

validate_items <- function(items){
  required_cols <- c("question_code",
                     "variable",
                     "metric_for_reports",
                     "min_good",
                     "max_good")

  present_col_bool <- required_cols %in% names(items)
  if( ! all( present_col_bool ) ) {
    "Error: Items is missing required column(s): " %+%
      paste0( required_cols[ ! present_col_bool ] , collapse=", ") %>%
      stop()
  }

  reportable_metrics_without_good_range <-
    items %>%
    filter(metric_for_reports, is.na(min_good) | is.na(max_good) ) %>%
    pull(question_code) %>%
    unique()

  if( length(reportable_metrics_without_good_range) != 0 ){
    "Error: Missing good range on metric_for_reports: " %+%
      paste0( reportable_metrics_without_good_range , collapse=", ") %>%
      stop()
  }

  factor_columns <- util$apply_columns( items, is.factor ) %>% unlist()
  if( any(factor_columns) ){
    "Error: There are factor data types in the items data.frame: " %+%
      paste0( names(items)[factor_columns] , collapse=", ") %>%
      stop()
  }
}


###############################################################
###
###   test__well_formed_items
###
###   Generates a well-formed items data frame for simulation
###   purposes.
###   @Sarah, you will need to update the items table
###   to follow these conventions. The composites are being
###   treated like items, so they will need a row in this
###   table too.
###
###############################################################

test__well_formed_items <- function(){
  data.frame(
    question_code = c("belonging_mean","faculty_mindset_mean","or_garbage"),
    variable = c("Sense of Belonging","Faculty Mindset","Open Response"),
    metric_for_reports = c(TRUE,TRUE,FALSE),
    min_good = rep(6, 3),
    max_good = rep(7, 3)
  ) %>% mutate_if( is.factor, as.character )
}


########################################################################
#####
#####   test__sim_org_data
#####
#####   Simulates organization data corresponding to multiple teams
#####   and multiple weeks in the specified range. If subgroups is
#####   supplied, each participant gets a 50% chance of being assigned
#####   to each subgroup. E.g., if subgroups are "target_group~yes"
#####   and "gender~female", participants have prob=.5 of being
#####   "gender~female" or "gender~not-female" and, independently, of
#####   "target_group~yes" and "target_group~not-yes".
#####
#####   Odd Behavior Notices:
#####   * If active_weeks < week_span, it is possible the present data
#####     will span fewer weeks than week_span because of missingness.
#####   * If present_responses < 1, it is possible some users could be
#####     entirely missing from data because they were randomly missing
#####     in every sampled week.
#####
########################################################################

test__sim_org_data <- function(
  items=NULL, # df with question_code,
  # metric_for_reports (incl. for compatibility with items)
  # full range assumed (1-7)
  subgroups=data.frame(),
  n_teams=2,
  participants_per_team=8,
  first_week="2019-08-26",
  week_span=4,   # span of org data in weeks
  active_weeks=2, # weeks given team was active
  present_responses=.8, # some responses should be missing
  response_mean=5.8, # for generated data
  response_floor=1,
  response_ceiling=7
){

  if( is.null(items) ) items <- test__well_formed_items()

  validate_items(items)

  # potential_weeks are weeks in the week_span when data could be collected
  start_date <- strptime(first_week, "%Y-%m-%d" ) %>% as_date()
  potential_weeks <- start_date + weeks(0:(week_span-1))

  team_ids   <- 1:n_teams

  # all possible indexes witout filtering to active week or present response
  all_team_indexes <- expand.grid(team_id=team_ids,
                                  week_start=as.character(potential_weeks)
  )
  all_team_indexes$team_name <- paste0("Team_",all_team_indexes$team_id)

  participant_ids    <- 1:(n_teams * participants_per_team)
  team_users <- data.frame(
    team_id = rep(team_ids, participants_per_team),
    participant_id  = participant_ids
  )

  all_indexes <- inner_join(all_team_indexes, team_users, by="team_id")

  # sample active_weeks for a given team
  team_active_weeks <-
    all_indexes %>%
    select(team_id, week_start) %>%
    distinct() %>%
    group_by(team_id) %>%
    mutate(active=week_start %in% sample(week_start,active_weeks)) %>%
    filter(active) %>%
    select(-active) %>%
    ungroup()

  active_indexes <- inner_join(team_active_weeks, all_indexes,
                               by=c("week_start","team_id"))

  # sample present_responses for a given user in a given week
  responses_to_sample <- floor(participants_per_team * present_responses)
  user_present_responses <-
    active_indexes %>%
    group_by(team_id, week_start) %>%
    mutate(present = participant_id %in% sample(participant_id,responses_to_sample)) %>%
    filter(present) %>%
    select(-present) %>%
    ungroup()

  # confirm expected number of users were sampled each week
  responses_in_week <- user_present_responses %>%
    group_by(team_id, week_start) %>%
    summarise(present_users = length(unique(participant_id))) %>%
    pull(present_users) %>%
    unique()
  if( responses_in_week != responses_to_sample ){
    stop("Error: Responses in a week should equal responses to sample.")
  }

  # generate response data
  metrics <- items %>%
    filter(metric_for_reports) %>%
    pull(question_code) %>%
    as.character()
  user_present_responses[,metrics] <- NA

  # create variance (random effects) for team
  V_team  <- .8
  V_error <- 1
  n_obs   <- nrow(user_present_responses)

  for(metric in metrics){
    # generate random effects
    team_re <- rnorm( n_teams, 0, sqrt(V_team) )
    eps <- rnorm( n_obs , response_mean, sqrt(V_error))

    # put it all together
    simulated_metric <-
      team_re[user_present_responses$team_id] +
      eps

    user_present_responses[,metric] <- case_when(
      simulated_metric > response_ceiling ~ response_ceiling,
      simulated_metric < response_floor ~ response_floor,
      TRUE ~ simulated_metric
    )
  }

  # If subgroups were passed in, randomly assign each present user to be a
  # member or a non-member of each subgroup.
  if(nrow(subgroups) > 0){
    # make a version of subgroups with non-member values for each feature
    subgroups_with_nonmembers <- subgroups %>%
      rename( target = value ) %>%
      mutate( non_target = paste0("not-", target) ) %>%
      mutate_if(is.factor, as.character) %>%
      as.data.frame()
    # for each user, randomly pick the target or non-target of each feature
    users_with_subgroups <- user_present_responses %>%
      select(participant_id) %>%
      distinct() %>%
      merge(subgroups_with_nonmembers) %>% # merge sans "by" = cartesian prod.
      rowwise() %>%
      mutate(chosen = sample(c(target,non_target),size=1)) %>%
      select(participant_id, feature, chosen) %>%
      dcast(participant_id ~ feature, value.var="chosen")
    # join the subgroup information to the present responses
    user_present_responses <-
      inner_join(user_present_responses,
                        users_with_subgroups,
                        by="participant_id")
  }

  user_present_responses
}

###   The diganostic code below can help you visualize the output of
###   test__sim_org_data

# sim_items <- test__well_formed_items()
# simulated <- test__sim_org_data(n_teams = 8,
#                                 week_span = 8,
#                                 active_weeks = 5,
#                                 items=sim_items)
# ggplot(simulated,
#           aes(week_start, belonging_mean, group=team_name, color=team_name) ) +
#         geom_path(stat="summary", fun.y="mean") +
#         geom_point(stat="summary", fun.y="mean")



############################################################
###
###   change_first_to_last
###
###   Calculates the % point change in pct_good from 1st to
###   last week for each question_code by team_id.
###   delta_col_label is how to rename the column of deltas.
###   Otherwise it is simply called "delta".
###
############################################################

change_first_to_last <- function(df, delta_col_label=NA){

  # debugging helpers immediately below
  # df <- imputed_responses
  # delta_col_label <- NA

  # check that all columns are present
  required_cols <- c("week_start","question_code",
                     "participant_id","is_good", "team_id")
  present_col_bool <- required_cols %in% names(df)
  if( ! all( present_col_bool ) ) {
    "Error change_first_to_last df is missing required col(s): " %+%
      paste0( required_cols[ ! present_col_bool ] , collapse=", ") %>%
      stop()
  }

  # calculate pct good for each team x question
  pct_good_by_team_x_question_position <- df %>%
    group_by(team_id, question_code, week_start) %>%
    summarise(pct_good = mean(is_good, na.rm=TRUE ),
                     n=n()) %>%
    # remove k<5 entries for privacy
    filter( n > 4 ) %>%
    # demark the first and last week for a given question x team
    group_by(team_id, question_code) %>%
    mutate( first_week = min(week_start),
                   last_week = max(week_start) ) %>%
    rowwise() %>%
    mutate( is_first = week_start == first_week,
                   is_last = week_start == last_week) %>%
    # return only rows corresponding to the first and last question
    select(team_id, question_code, pct_good, is_first, is_last) %>%
    filter( is_first | is_last ) %>%
    ungroup()

  # if filtering out k<5 could have resulted in no data to report
  if(nrow(pct_good_by_team_x_question_position) == 0) return(list())

  # break apart the first and last observations; then join to calculate change
  lasts <- pct_good_by_team_x_question_position %>%
    filter(is_last) %>%
    select(team_id, question_code, pct_good) %>%
    rename(last_pct_good = pct_good)
  firsts <- pct_good_by_team_x_question_position %>%
    filter(is_first) %>%
    select(team_id, question_code, pct_good) %>%
    rename(first_pct_good = pct_good)

  pct_good_by_team_x_question_wide <-
    inner_join(lasts, firsts, by=c("team_id","question_code") ) %>%
    mutate( delta = last_pct_good - first_pct_good ) %>%
    select( team_id, question_code, delta )

  if( ! is.na(delta_col_label) ){
    pct_good_by_team_x_question_wide[,delta_col_label] <-
      pct_good_by_team_x_question_wide$delta
    pct_good_by_team_x_question_wide <- pct_good_by_team_x_question_wide %>%
      select( -delta )
  }
  pct_good_by_team_x_question_wide
}



########################################################################
#####
#####   Primary (non-helper) Functions start here.
#####
#####   plot_teams_by_week returns a list of lists of base_64 graphs or
#####   plots if base_64==FALSE.
#####   The 1st level groups by "question_code" (i.e., metric).
#####   The 2nd level has 1+ graphs depending on how many teams are
#####   in the community. Each graph only has up to 4 teams.
#####
#####   All metrics from "items" are graphed if (1) there is k>4 data
#####   on a team and (2) items$metric_for_reports is TRUE.
#####   Values get imputed. See impute_to_week for details.
#####
#####
########################################################################

plot_teams_by_week <- function (
  report_date = REPORT_DATE,
  organization_associations = assoc,
  items,
  org_data_cat,
  base_64=TRUE  # if TRUE, apply plot_to_base_64 to each plot
) {

  #### debugging helper:
  # If using the simulated data generated in the "diagnostic" section below,
  # uncomment the lines below to pretend those variables got passed in.
  # org_data_cat <- simulated
  # organization_associations <- sim_org_assc
  # report_date <- report_date
  # items <- items
  # report_date <- "2020-11-01"

  validate_items(items)

  variable_label_map <- items %>%
    select(question_code, variable) %>%
    distinct()

  # what metrics are eligible for reporting?
  report_metrics <- items %>%
    filter(metric_for_reports) %>%
    pull(question_code) %>%
    unique()

  # if there are no data to graph, return an empty list
  if(nrow(org_data_cat) == 0) return(list())

  # remove data collected after the report date
  org_data_cat <- org_data_cat %>%
    mutate(week_start = as.character(week_start) ) %>%
    filter(week_start <= report_date)

  # impute response data for a given team's active weeks for each metric
  imputed_responses <- impute_to_week(org_data_cat, report_metrics)
  # even if org_data_cat has rows, there might not be any reportable
  # metrics. imputed_responses returns NULL if reportable data aren't present.
  # If there aren't imputed data, return an empty list
  if( is.null(imputed_responses) ) return(list())

  # calculate the pct_good (after joining max_good & min_good from items)
  reportable_item_ranges <- items %>%
    filter(metric_for_reports) %>%
    select(question_code,min_good,max_good) %>%
    mutate_if(is.factor, as.character)
  imputed_responses <- inner_join(imputed_responses,
                                  reportable_item_ranges,
                                  by="question_code")

  imputed_responses$is_good <- ifelse(
    imputed_responses$value_imputed >= imputed_responses$min_good &
    imputed_responses$value_imputed <= imputed_responses$max_good,
    1,0)

  # calculate percent responses in the good range by team & week
  team_metric_wk <- imputed_responses %>%
    group_by(team_id, week_start, question_code) %>%
    summarise( pct_good = mean(is_good), n = n() ) %>%
    ungroup()

  # merge in team names
  team_name_map <- organization_associations %>%
    select(team_id, team_name) %>% distinct()
  team_metric_wk <-
    inner_join(team_metric_wk, team_name_map, by="team_id")


  ## get ready for graphing

  # make a date version of week_start so that x-axis can be proper time
  # rather than a discrete set of strings (resulting in uneven breaks)
  string_date_format <- "%Y-%m-%d"
  team_metric_wk$week_start_date <-
    strptime(team_metric_wk$week_start, string_date_format ) %>%
    as_date()

  # metric_graph_ls will hold the graph objects
  metric_graph_ls <- list()

  plot_title_size <- 12
  panel_margin <- .2

  present_metrics <- team_metric_wk$question_code %>% unique()

  for(present_metric in present_metrics){
    # extract the label for use in the graph title
    present_metric_label <- variable_label_map %>%
      filter(question_code %in% present_metric) %>%
      pull(variable)

    # remove teams with k<5
    present_metric_wks <- team_metric_wk %>%
      filter(question_code %in% present_metric, n > 4) %>%
      ungroup()

    # if a metric does not have present data, nothing to graph
    if(nrow(present_metric_wks) == 0) next

    # for interpretability, only up to 4 teams per graph
    present_metric_wks <-
      present_metric_wks %>%
        mutate(graph_group=ceiling(util$ordinal(team_id)/4))

    graph_groups <- unique(present_metric_wks$graph_group)
    for(graph_group_i in graph_groups){

      present_metric_wks_to_graph <- present_metric_wks %>%
        filter(graph_group_i == graph_group)

      metric_graph_ls[[present_metric]][[graph_group_i]] <-
          ggplot( present_metric_wks_to_graph,
                  aes(week_start_date, pct_good,
                      group=team_name)) +
            scale_color_brewer(palette = "Set1") +
            geom_path( aes(color=team_name), stat="identity") +
            geom_point( aes(color=team_name, shape=team_name),
                        stat="identity", size=3.5) +
            xlab("") +
            ylab("") +
            ggtitle("") + #ggtitle(present_metric_label) +
            scale_x_date(date_labels = "%m/%d") +
            scale_y_continuous(labels = percent_format(accuracy = 2L)) +
            theme(
              legend.position="bottom",
              legend.direction = "vertical",
              # legend.key = element_blank(),
              plot.title = element_text(
                colour="black", size=plot_title_size, face="bold", hjust=0),
              panel.background    = element_blank() ,
              #   make the major gridlines light gray and thin
              panel.grid.major.y  = element_line( size=graphing$line_size, colour="#757575" ),
              #   suppress the vertical grid lines
              # panel.grid.major.x  = element_blank() ,
              #   suppress the minor grid lines
              panel.grid.minor    = element_blank() ,
              #   adjust the axis ticks
              axis.ticks = element_line( size=graphing$line_size , colour="black" ),
              axis.title = element_text(face="bold"),
              axis.title.x  =
                element_text( vjust=-.5, color="black", angle=0, size=graphing$text_size ) ,
              axis.title.y =
                element_text( vjust=.2, color="black", angle=90, size=graphing$text_size ),
              axis.text.x =
                element_text(face="bold", angle=0, size=graphing$text_size),
              axis.text.y =
                element_text( vjust=0, color="black", angle=0, size=graphing$text_size ) ,
              plot.margin = unit(c(0,0,0,0), "cm"),
              panel.spacing = unit(panel_margin + .2, "in"),
              legend.title = element_blank()
            )

      if(base_64){
        metric_graph_ls[[present_metric]][[graph_group_i]] <-
          helpers$plot_to_base_64(metric_graph_ls[[present_metric]][[graph_group_i]])
      }
    }
  }

  metric_graph_ls

}



########################################################################
#####
#####
#####   change_by_team_subgroup returns a list of lists.
#####
#####   The 1st layer of the list is indexed on each question_code.
#####   The 2nd layer of the list is indexed as
#####   "label" for printable metric name or "deltas" for the delta data.frame.
#####   The delta data.frame shows the percentage point change from the
#####   first observation to the last observation for each team.
#####   The deltas are shown overall and for requested or default subgroups.
#####
#####
########################################################################


change_by_team_subgroup <- function(
  report_date = REPORT_DATE,
  organization_associations = assoc,
  items,
  org_data_cat,
  subgroups="default" # if subgroups is NULL it calls default_subgroups()
) {

  #### debugging helper:
  #### If using the simulated data generated in the "diagnostic" section below,
  #### uncomment the lines below to pretend those variables got passed in.
  # org_data_cat <- simulated
  # organization_associations <- sim_org_assc
  # report_date <- report_date
  # items <- items
  # subgroups <- subgroups

  validate_items(items)

  # if there are no data to graph, return an empty list
  if(nrow(org_data_cat) == 0) return(list())

  variable_label_map <- items %>%
    select(question_code, variable) %>%
    distinct()

  # what metrics are eligible for reporting?
  report_metrics <- items %>%
    filter(metric_for_reports) %>%
    pull(question_code) %>%
    unique()

  # remove data collected after the report date
  org_data_cat <- org_data_cat %>%
    mutate(week_start = as.character(week_start) ) %>%
    filter(week_start <= report_date) %>%
    as.data.frame()

  # impute response data for a given team's active weeks for each metric
  imputed_responses <- impute_to_week(org_data_cat, report_metrics)
  if( is.null(imputed_responses) ) return(list())

  # join in good ranges to determine where's "good"
  reportable_item_ranges <- items %>%
    filter(metric_for_reports) %>%
    select(question_code,min_good,max_good)
  imputed_responses <- inner_join(imputed_responses,
                                  reportable_item_ranges,
                                  by="question_code")

  imputed_responses$is_good <- ifelse(
    imputed_responses$value_imputed >= imputed_responses$min_good &
      imputed_responses$value_imputed <= imputed_responses$max_good,
    1,0)

  # these are the deltas at a team level (overall, not subgroup)
  deltas <- change_first_to_last(imputed_responses,"Overall")
  if( identical(deltas, list()) ){ return(list()) }

  # if subgroups weren't passed in, assume we want default subgroups
  if( identical(subgroups,"default") ){
    subgroups <- default_subgroups()
  }

  # for each present subgroup, append the first to last deltas
  if( nrow(subgroups) > 0 ){
    present_subgroups <- subgroups %>%
      filter( feature %in% names(org_data_cat) ) %>%
      mutate_if( is.factor, as.character ) %>%
      as.data.frame()

    for(i in sequence(nrow(present_subgroups))) {
      sub_feature <- subgroups[i,"feature"]
      sub_value <- subgroups[i,"value"]
      sub_label <- subgroups[i,"label"]

      # what users belong to the subgroup?
      subgroup_rows <- org_data_cat[,sub_feature] %in% sub_value
      subgroup_participants <- org_data_cat[subgroup_rows,"participant_id"] %>%
        unique()

      # subset imputed responses to those from this subgroup
      subgroup_responses <- imputed_responses %>%
        filter( participant_id %in% subgroup_participants)

      # if there are no subgroup_responses, skip the loop
      if( nrow(subgroup_responses) == 0 ) next

      subgroup_deltas <- change_first_to_last(subgroup_responses,sub_label)

      if( identical(subgroup_deltas,list()) ) next

      deltas <- left_join(
        deltas,
        subgroup_deltas,
        by=c("team_id","question_code")
      )
    }
  }

  # append team name
  team_name_map <- organization_associations %>%
    select(team_id, team_name) %>% distinct()

  labeled_deltas <- deltas %>%
    left_join( team_name_map, by="team_id" ) %>%
    # The columns of this data frame will become display text in the
    # corresponding table in the report (except team_id), so give this column
    # a more human-readable name.
    rename(Project = team_name)

  # prettify the labeled_deltas so they can be displayed without additional
  # reformatting
  vars_to_mutate <-
    names(labeled_deltas)[! names(labeled_deltas) %in% c("team_id","team_name")]
  labeled_deltas <- labeled_deltas %>%
    mutate_at( vars_to_mutate ,
               html_pct_deltas )

  # turn the labeled_deltas data.frame into a list of lists
  # the 1st layer of the list is indexed on each question_code
  # the 2nd layer of the list is indexed as
  #   "label" for printable name or "deltas" for the delta data.frame
  present_question_codes <- labeled_deltas$question_code %>% unique()
  deltas_list <- list()
  for( present_question in present_question_codes ){
    deltas_list[[present_question]][["deltas"]] <-
      labeled_deltas %>%
        filter(question_code %in% present_question) %>%
        select( -question_code )

    deltas_list[[present_question]][["label"]] <-
      util$recode(present_question, items$question_code, items$question_text )
  }
  deltas_list
}



########################################################################
#####
#####   WARNING! Problem Demonstration
#####
#####   @todo @Sarah: REMOVE this section on RServe integration.
#####
#####   Description: The version of "items" available to Dave at coding time
#####   did not have min_good and max_good, which are necessary for
#####   these functions. The code below DOES work with real "items"
#####   iff the min_good and max_good are set manually (per below).
#####   To see the problem: call validate_items(items) without setting
#####   min_good and max_good.
#####
########################################################################


# items$min_good <- 6
# items$max_good <- 7
# validate_items(items) # it should work
#
# plots <- plot_teams_by_week(
#   report_date = report_date,
#   organization_associations = organization_associations,
#   items,
#   org_data_cat,
#   base_64=FALSE
# )
#
# plots[[2]][[1]]



########################################################################
#####
#####   Demos & Diagnostics (for human learning & debugging)
#####
#####   The code below facilitates debugging and demostrates the
#####   features of the functions with simulated data.
#####   These are NOT automated unit tests, but it would probably be
#####   worth automating some of them. Dave ran out of time before
#####   he could do covert them to robust unit tests :-(
#####
########################################################################


# items <- test__well_formed_items()
#
# # Define the subgroups for which data should be summarized.
# # If a feature or value is missing from a dataset, it's gracefully skipped.
# subgroups <- default_subgroups()
# simulated <- test__sim_org_data(items=items,
#                                 subgroups=subgroups,
#                                 n_teams=6,
#                                 week_span = 3,
#                                 active_weeks = 2,
#                                 participants_per_team = 8
# )
#
# sim_org_assc <- simulated %>%
#   dplyr::select(team_id, team_name)
#
# graphs <- plot_teams_by_week(
#             report_date = "2020-11-01",  # in RServe, set to REPORT_DATE
#             organization_associations = sim_org_assc,
#             items=items,
#             org_data_cat=simulated,
#             base_64=FALSE
#             )
#
# graphs[[2]][[1]]  # displays plot for 2nd metric, 1st group of teams
#
# change_by_team_subgroup(
#   report_date = "2020-01-01",
#   organization_associations = sim_org_assc,
#   items=items,
#   org_data_cat=simulated
# )
#
#
## if no data get passed in, plot_teams_by_week should return empty list
# graphs <- plot_teams_by_week(
#   report_date = "2020-11-01",  # in RServe, set to REPORT_DATE
#   organization_associations = sim_org_assc,
#   items=items,
#   org_data_cat=data.frame(),
#   base_64=FALSE
# )
#
# change_by_team_subgroup(
#   report_date = "2020-01-01",
#   organization_associations = sim_org_assc,
#   items=items,
#   org_data_cat=data.frame()
# )

### @todo(chris): DP left this code in global scope, clean it up later
###<DP code>

# ## if data get passed in, but none are REPORTABLE data,
# # plot_teams_by_week should return empty list
# metrics_for_reports <- items %>%
#   filter(metric_for_reports) %>%
#   pull(question_code)
# no_reportable_data <- simulated[,! names(simulated) %in% metrics_for_reports]

# graphs <- plot_teams_by_week(
#   report_date = "2020-11-01",  # in RServe, set to REPORT_DATE
#   organization_associations = sim_org_assc,
#   items=items,
#   org_data_cat=no_reportable_data,
#   base_64=FALSE
# )
# change_by_team_subgroup(
#   report_date = "2020-01-01",
#   organization_associations = sim_org_assc,
#   items=items,
#   org_data_cat=no_reportable_data
# )

###</DP code>



########################################################################
#####
#####   Unit Testing Functions (run every time RServe runs)
#####
#####   Ideally called automatically once per RServe run...
#####   Notes: (1) Uses CSV file. (2) Assume getwd() is root of RServe.
#####
#####   @todo for @Chris: Move to preffered testing folder and
#####   integrate with testing package (not urgent).
#####
########################################################################


test__impute_to_week_filling <- function(){
  # uses community_graphs_unit_test_support.csv
  # to compare what impute_to_week produces compared to expectations
  test__imputation_org_data <-
    read.csv("scripts/copilot/community_graphs_unit_test_support.csv",
             stringsAsFactors = FALSE)
  report_metrics <- "question_code"
  imputation_returned <-
    impute_to_week(test__imputation_org_data, report_metrics)

  test__joined <- left_join( test__imputation_org_data,
                             imputation_returned,
                             by=c("team_id","participant_id","week_start"))
  if(any(test__joined$value_inputed != test__joined$expected)){
    stop("test__impute_to_week_filling Failed!")
  }
  return("test__impute_to_week_filling Passed!")
}

### Test call is commented out because it does not fit the RServe
### unit test paradigm, yet.
# test__impute_to_week_filling()







