modules::import('lubridate')
modules::import('reshape2')
modules::import('tidyr')
modules::import('jsonlite')
modules::import('readr')
modules::import('tools')
modules::import('dplyr')
modules::import('stats')
modules::import('scales')
modules::import('stringr', 'str_trim')
modules::import('ggplot2')

source('scripts/copilot/engagement_helpers.R', local = TRUE)

graphing <- import_module("graphing")
logging <- import_module("logging")
perts_ids <- import_module("perts_ids")
util <- import_module("util")


create_report <- function(
  SUBSET_TYPES,
  REPORT_DATE,
  TEAM_ONLY,
  REPORTING_UNIT_ID_TYPES,
  MAX_CYCLES_MISSING,
  ANONYMOUS,
  MIN_CELL,
  study_id,
  team_id,
  class_id,
  data_cat,
  team_name ,
  class_name,
  agg_metrics_small,
  participation_table_df,
  class_recent_open_responses,
  all_or_question_ids,
  items,
  fidelity_tuq,
  fidelity_honest,
  earliest_data,
  target_msg
) {
  # Uncomment this if you want to simulate some data in a separte csv file
  # agg_metrics_small <- read.csv("agg_metrics_small_try.csv", stringsAsFactors = FALSE)

  # Places we may want to save data (if a crypt is mounted)
  crypt_path <- util$find_crypt_paths(list(root_name = "rserve_data"))
  should_save_rds <- length(crypt_path$root_name) > 0  # if not found value will be character(0)
  rds_paths <- list(
    args = paste0(crypt_path$root_name,"/rds/create_report.args.rds"),
    return = paste0(crypt_path$root_name,"/rds/create_report.return.rds")
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

  EXPECTED_DRIVERS <- items$driver %>% na.omit() %>% unique()

  # I get an error that the two fidelity arguments are not used. I will assign them to bogus variable.
  bogus <- fidelity_tuq
  bogus <- fidelity_honest

  logging$debug("########## SET OPTIONS ##########")

  original_scipen <- getOption('scipen')
  original_stringsAsFactors <- getOption('stringsAsFactors')
  options(scipen = 999) # removes scientific notation
  options(stringsAsFactors = FALSE)

  ####   Set parameters   #####
  ##  (this is independent of subsetting and of agg_metrics, so it can be kept in the Rmd)
  ##

  max_table_width <- 8
  # this controls how wide the participation summary tables can be (e.g. number of cycles)
  # we may need something like this for the line graphs too

  max_time_chart_width <-16
  # controls how many cycles are included in the time chart

  # all subset values
  all_subset_values_orig <- c(
    #"Eng. Speaker",
    #"Eng. Learner",
    "Not Target Grp.",
    "Target Grp.",
    "Struct. Adv.",
    "Struct. Disadv.",
    "Male",
    "Female",
    "All Students"
  )

  # Clean up names for presentation
  title_for_full_dataset <- team_name %+% " " %+% study_id
  class_title <- format_reporting_units(class_name)
  team_title <- format_reporting_units(team_name)

  ## This report must be called from the metascript.R file.
  # expected variables from metascript
  # NOTE: this will change after the code update.
  expected_variables <- c("study_id","team_id","class_id","data_cat",
                          "SUBSET_TYPES")
  nothing <- lapply(expected_variables, function(var){
    if(!exists(var)){
      stop(var %+% " was not passed in!")
    }
  })


  #####   Anonymize report if desired (replace team and unit names with fake ones) #####
  ##
  ##
  team_name_unhashed <- team_name
  class_name_unhashed <- class_name
  if (ANONYMOUS) {
    # anonymize agg_metrics data
    agg_metrics_small$team_name <- "Dream Team"
    agg_metrics_small$class_name <- "Mr. Smith Math 1"
    agg_metrics_small$reporting_unit_name <-ifelse(agg_metrics_small$reporting_unit_type %in% "class_id",
                                                   "Mr. Smith Math 1",
                                                   "Dream Team")
    # anonymize participation table data
    if(length(participation_table_df) == 3) {
      names(participation_table_df) <- c("Cycle", "Dream Team", "Mr. Smith Math 1")
    }
    if(length(participation_table_df) == 2) {
      names(participation_table_df) <- c("Cycle", "Dream Team")
    }

    # anonymize tags that we are using in this script (for matching)
    team_name <- "Dream Team"
    class_name <- "Mr. Smith Math 1"
  }

  # if not team-only and there are not enough students, create a team-only report (copied from old code)
  if(!TEAM_ONLY) {
    min_users_per_cycle <- agg_metrics_small[agg_metrics_small$reporting_unit_type %in% "class_id" &
                                            agg_metrics_small$subset_type %in% "All Students", "n"] %>%
      min(na.rm = TRUE)
    if (min_users_per_cycle < MIN_CELL) {
      TEAM_ONLY <- TRUE
      class_title_full <- ""
      agg_metrics_small <- agg_metrics_small[!agg_metrics_small$reporting_unit_type %in% "class_id", ]
    }
  }



  #####   Protect privacy of small subsets  #####
  ##  Note: removing just the offending offending cell not good enough because
  ##  let's say n=30, total=72%, Latino=74%. If only one non-Latino,
  ##      you know they're in the "bad" range.
  ##  pixelate groups with no variance (add se even if it does not exist)

  # pixelate se and pct_good:
  # if no se or NA se, impute smallest non-zero se.
  agg_metrics_small$se[agg_metrics_small$se == 0] <- min(agg_metrics_small$se[agg_metrics_small$se > 0], na.rm = TRUE)
  agg_metrics_small$se[is.na(agg_metrics_small$se) & ! is.na(agg_metrics_small$pct_good)] <-
    min(agg_metrics_small$se[agg_metrics_small$se > 0 ], na.rm=TRUE)
  # if no pct_good, impute one person's worth of it.
  # here we're replacing "0" with the smallest possible proportion of "good" responses
  # so as not to implicate a whole group of students for having uniformly
  # "bad" responses.
  agg_metrics_small$pct_good[agg_metrics_small$pct_good %in% 0] <-
    1 / agg_metrics_small$n[agg_metrics_small$pct_good %in% 0]


  # find all the rows corresponding to small n's
  small_rows <- agg_metrics_small[agg_metrics_small$n < MIN_CELL,]
  # currently this line does not include groups which are missing.
  # their sample size is not 0, they are just missing. This leads to
  # the problem that the counterparts of the n of 0 groups are not being
  # excluded

  # A non-elegant solution here (it will be better to do it on the level
  # of melt and cast) :

  # I will remove those categories which have only one level (e.g. only male, or only White)
  # I will refer to those as large_rows (because their n is high)
  # I will also remove those rows with n smaller than the minimum allowed
  # Those are referred to as small rows (because n is small)

  # I assume that there are only two levels per category here
  large_rows <- agg_metrics_small %>%
    dplyr::group_by(
      reporting_unit_name, cycle_name, subset_type, metric) %>%
    summarise(n_levels = n()) %>%
    dplyr::filter(subset_type != "All Students") %>%
    mutate(delete_small_subset = TRUE)

  # check if the assumption is correct
  if (max(large_rows$n_levels, na.rm = TRUE) > 2) {
    msg <-
      "The assumption for binary comparisons does not hold: " %+%
      team_name %+% ", " %+% class_name
    stop(msg)
  }

  # select only those categories which have only one level
  large_subsets <-
    large_rows[large_rows$n_levels %in% 1,]  %>%
    dplyr::select(- n_levels) %>%
    unique()

  small_subsets <- small_rows[,c("reporting_unit_name", "cycle_name","subset_type","metric")] %>%
    unique() %>%
    mutate(delete_small_subset = TRUE)

  small_subsets <- bind_rows(small_subsets, large_subsets)
  small_subsets <- small_subsets[!duplicated(small_subsets),]
  # remove duplicates (e.g. those who have only one level, such as Female, and n = 2)

  # delete values from all rows matching any subset feature/metric/reporting_unit_name/cycle_name
  # combos that had small n's. That way, you're not just deleting e.g., the
  # one gender value with < 5 responses; you're deleting gender as a category
  # from the report for that reporting_unit_name/metric/day.
  agg_metrics_small <- merge(
    agg_metrics_small,
    small_subsets,
    by = c("reporting_unit_name", "cycle_name","subset_type","metric"),
    all = TRUE
  ) %>%
    mutate(delete_small_subset = !is.na(delete_small_subset))

  agg_metrics_small[ agg_metrics_small$delete_small_subset, c("pct_good","se") ] <- NA

  # Clean up
  agg_metrics_small$delete_small_subset <- NULL


  #####   Clean up the participation table   #####
  ##
  ##

  # Assume that the 1st column of the table is the cycle date, and the rest are response rates,
  # and look for the first row of response rates that has something non-zero.
  # (This feels hacky and contingent, but I want to get something going.)

  # Sanity-check the table column names first:
  if(!names(participation_table_df)[1] %in% "Cycle"){
    stop("Rendering participation table failed because column 1 of participation_table_df was not named 'Cycle'.")
  }
  # Sanity-check the ordering of rows in the participation table:
  if(any(participation_table_df$cycle_order != 1:nrow(participation_table_df))){
    stop("Rendering participation table failed because participation_table_df rows don't seem to be in order.")
  }
  # rows are ordered fine, so you can cut "cycle_order"
  participation_table_df$cycle_order <- NULL


  # If the table is still too long to display, shorten it to the right max length backwards from today
  if(nrow(participation_table_df) > max_table_width) {
    participation_table_df <- participation_table_df[(nrow(participation_table_df) - max_table_width + 1) :
                                                       nrow(participation_table_df), ]
  }

  # If any values in the rightmost column of the participation table are NA,
  # replace them with "0 (0%)". These are assumed to be missing class data.
  participation_table_df[, ncol(participation_table_df)] <- ifelse(
    is.na(participation_table_df[, ncol(participation_table_df)]),
    "0 (0%)",
    participation_table_df[, ncol(participation_table_df)]
  )

  zero_length_table <- nrow(participation_table_df) == 0  # This might not be used, but it's in YAML

  #####   Set flags if there are pre-first-cycle data or no cycles at all  #####
  ##
  ##

  # If there are pre-first-cycle data, note the earliest date of data
  date_of_earliest_uncycled_data <- NA
  if(team_id %in% earliest_data$team_id) {
    date_of_earliest_uncycled_data <- earliest_data[earliest_data$team_id %in% team_id,
                                                    "earliest_response_date"] %>%
                                      as.numeric() %>%
                                      as.Date(origin = "1970-01-01")
  }

  # If there are no cycles, note this
  no_cycles_defined <- FALSE
  if("(no cycles defined)" %in% unlist(participation_table_df)) {
    no_cycles_defined <- TRUE
  }


  #####   Clean up the open responses #####
  ##
  ##

  # Convert to character just in case,
  # remove funky characters that R might not know how to print,
  # remove leading or trailing whitespace,
  # convert empty strings to NA
  empty_string_to_NA <- function(x) {
    ifelse(x %in% "", NA, x)
  }
  trim_if_over_5k <- function(x) {
    ifelse(!is.na(x) & nchar(x) > 5000,
           substr(x, 1, 5000) %+% "[---RESPONSE TRIMMED FOR SPACE---]",
           x)
  }
  class_recent_open_responses_cleaned <- class_recent_open_responses %>%
    util$apply_columns(as.character) %>%
    util$apply_columns(util$strip_non_ascii) %>%
    util$apply_columns(str_trim) %>%
    util$apply_columns(empty_string_to_NA) %>%
    util$apply_columns(trim_if_over_5k)

  # If this report is team-only, wipe the open-response data so that
  # nothing is displayed but all objects are created and the code runs.
  if(TEAM_ONLY) {
    class_recent_open_responses_cleaned <- util$apply_columns(class_recent_open_responses_cleaned,
                                                              function(x) {x <- NA})
  }

  # helper function for creating a list for a single OR question. This list has "q_id", "intro_text", and "responses."
  create_q_list <- function(or_data, q_id) {
    q_list <- list()
    q_list$q_id <- q_id
    q_list$intro_text <- items[items$question_code %in% q_id, "question_text"]
    q_list$responses <- na.omit(or_data[, q_id])
    return(q_list)
  }

  # assemble master list of open response question lists, using helper function above
  open_responses_list <- list()
  if (!TEAM_ONLY) {
    for(q_id in all_or_question_ids) {
      open_responses_list[[q_id]] <- create_q_list(class_recent_open_responses_cleaned, q_id)
    }
  }



  # The object used going forward is called ag_metric_with_p
  ag_metric_with_p <- agg_metrics_small



  #####   Append driver information   #####
  #

  # @ to=do, move this to the items-generator code in Dan's script
  # (i.e., should start with an items csv that has all the
  # required columns, rather than fixing it here!)
  items_renamed <- rename(items, metric=question_code)

  agm <- merge(
    ag_metric_with_p,
    items_renamed,
    by="metric"
  )

  agm$grand_mean <- ifelse(agm$subset_value %in% "All Students","All Students","Subset")

  # Issues with the metascript make it difficult to tell which drivers are actually in the data,
  # because NA columns for all questions were inserted earlier.
  # HACKY TEMPORARY SOLUTION: Identify drivers that have 100% missing data for mean_value in agm.
  # They have NEVER been in the dataset for this class/team.
  # TODO: Rewrite metascript and report-generation code to not insert NA columns (around line 191 in metascript)
  # drivers_in_data <- agm$driver %>% unique  <-- this was old code
  nan_mean_val_drivers <- agm %>%
    group_by(driver) %>%
    summarise(prop_nan_mean = sum(is.nan(mean_value)) / n())
  drivers_in_data <- nan_mean_val_drivers[nan_mean_val_drivers$prop_nan_mean < 1, "driver"] %>%
    unlist(use.names = FALSE)
  undescribed_drivers <- drivers_in_data[!drivers_in_data %in% EXPECTED_DRIVERS]
  present_drivers <- drivers_in_data[drivers_in_data %in% EXPECTED_DRIVERS]

  agm$reporting_unit_name <- factor(agm$reporting_unit_name, c(team_name, class_name))

  # add selective position of the star for the graphs
  gg_sign_star_str <-
    'geom_text(
  aes(y = max_pct_good, label = sign_star),
  stat="summary",
  fun.y="mean",
  vjust = 0.3, #positive goes down in flipped bar graph (relative to bars)
  hjust = 0.3, #positive goes left in flipped bar graph (relative to bars)
  size = graphing$text_size/2,
  color = "black",
  fontface="bold",
  angle = 270
  )'
  #for hjust # more negative goes further right

  triage_columns <- c("driver","reporting_unit_name","pct_good","question_text","cycle_name")
  pre_triage <- agm[order(agm$cycle_name, decreasing = TRUE), triage_columns]
  pre_triage <-
    pre_triage[ ! duplicated(pre_triage[,c("question_text","cycle_name")]), ]

  ## Print out the driver graphs
  org_focus_populations <- c("Female")
  #From Rumen: I'm not removing the old code for focus population since we
  # might want to add them again later
  #focus_populations <- c("All Students", org_focus_populations)
  focus_populations <- c("All Students")
  plot_title_size <- 12
  panel_margin <- .2

  # set some factor levels for good graphing
  all_subset_values <- agm$subset_value %>% unique
  all_subset_values_ordered <- c("All Students", setdiff(all_subset_values, "All Students"))
  agm$subset_value <- factor(agm$subset_value, all_subset_values_ordered)
  agm$grand_mean <- factor(agm$grand_mean, c("All Students", "Subset"))


  #new_basic_log <- data.frame()

  # These will hold the temporary file names of ggplot charts, so they can be
  # passed to the template during knitting. See the YAML block at the end of
  # this file.
  cross_section_charts <- list()
  cross_section_charts_base_64 <- list()
  timeline_charts <- list()
  timeline_charts_base_64 <- list()

  # we need to keep track of those drivers for which we did not create any graph objects
  # later we will subtract them from the present_drivers, so the html template will not
  # get confused about what is truly present
  absent_drivers <- c()

  # also want to keep track of messages to display on the report of pct-improvement
  pct_improve_strings <- list()

  ##################################################################
  ##################################################################

  # BIG LOOP:
  # Loop through each driver and generate graphs
  for(driver in present_drivers){
    # format the question and reporting_unit_name labels


    # driver_df is the subset of agm where the driver matches
    # the driver we're iterating over, and the pct_good value is not NA
    driver_df <- agm[agm$driver %in% driver &
                       !is.na(agm$pct_good),]
    if (nrow(driver_df) == 0){
      logging$warning("No data to display for driver:", driver)
      absent_drivers <- c(absent_drivers, driver)
      next
    }
    # create human readable labels for graph axes and panels
    driver_df$dates_measured_phrase <- driver_df$cycle_name
    driver_df$dates_measured_phrase_full <- driver_df$cycle_name
    driver_df$question_text_wrapped <- lapply(
      driver_df$question_text,
      function(x) wrap_text(x, width = 15) #original width was 25
    ) %>%
      unlist()
    driver_df$reporting_unit_name_wrapped <- driver_df$reporting_unit_name %>%
      format_reporting_units %>%
      lapply(function(x) wrap_text(x, width = 15)) %>%
      unlist

    # combine cycles/questions with date ranges for time and bar graphs, respectively
    driver_df$question_with_dates_wrapped <- driver_df$question_text %+% "\n" %>%
      lapply(function(x) wrap_text(x, width = 19)) %>% unlist #original width was 25
    driver_df$reporting_unit_name_with_dates_wrapped <-
      driver_df$reporting_unit_name %+% "\n" %+% driver_df$dates_measured_phrase %>%
      lapply(function(x) wrap_text(x, width = 10)) %>% unlist


    # create factor levels to ensure that team results always
    # get faceted above class results
    factor_reporting_unit_name_levels <- function(reporting_unit_name_col,
                                                  reporting_unit_label_col){
      levs <- data.frame(reporting_unit_name_col, reporting_unit_label_col) %>%
        unique
      team_levs <- levs[levs[,1] %in% team_name, 2] %>% as.character
      class_levs <- levs[levs[,1] %in% class_name, 2] %>% as.character
      factored_var <- factor(reporting_unit_label_col, c(team_levs, class_levs))
      return(factored_var)
    }

    driver_df$reporting_unit_name_wrapped <- factor_reporting_unit_name_levels(
      driver_df$reporting_unit_name,
      driver_df$reporting_unit_name_wrapped
    )


    # make the most recent graph by population for each question
    recent <- driver_df %>%
      dplyr::group_by(question_text) %>%
      summarise( cycle_name=max(cycle_name) )

    # I want to define the most recent data as the most
    # recent cycle WITH VALID RESPONSES.
    # driver_df_most_recent <- merge(driver_df, recent, by = c("question_text", "wave"), all = FALSE)
    # From Rumen: I will override the selection of wave by question. Dave clarified that in the new design
    # all questions will appear in each time period.
    driver_df_most_recent <- driver_df [ driver_df$cycle_name %in% max(driver_df$cycle_name),]

    ###################################
    # add gray bars with NA when students are missing
    # we will need to restructure the input database used by ggplot
    # by inserting empty rows for the missing categories

    # take Cartesian product
    grid_df <- expand.grid(
      "metric" =  unique(driver_df_most_recent$metric),
      "reporting_unit_name" = unique(driver_df_most_recent$reporting_unit_name),
      "subset_value" = all_subset_values_orig)

    driver_df_most_recent$imputed_row <- FALSE

    grid_df <- merge(
      grid_df,
      driver_df_most_recent,
      by = c("metric", "reporting_unit_name", "subset_value"),
      all = TRUE)

    grid_df$imputed_row[is.na(grid_df$imputed_row)] <- TRUE

    grid_df <- replace_missing(
      in_df = grid_df,
      shared_col = "metric",
      cols_to_fill_in = c(
        "question_with_dates_wrapped",
        "question_text_wrapped",
        "question_text",
        "cycle_name")
    )

    grid_df <- replace_missing(
      in_df = grid_df,
      shared_col = "reporting_unit_name",
      cols_to_fill_in = c(
        "reporting_unit_name_wrapped",
        "reporting_unit_name_with_dates_wrapped",
        "dates_measured_phrase_full")
    )

    grid_df <- replace_missing(
      in_df = grid_df,
      shared_col = "subset_value",
      cols_to_fill_in = c(
        "subset_type")
    )

    # add pct good, grand mean, text to display
    grid_df$grand_mean[grid_df$imputed_row] <- "All Students"

    grid_df$comb_id <- paste(
      grid_df$metric,
      grid_df$reporting_unit_name,
      grid_df$grand_mean,
      sep = "_"
    )

    # replace missing pct_good with those from the overall mean
    # for each corresponding group and subgroup
    grid_df <- replace_missing(
      in_df = grid_df,
      shared_col = "comb_id",
      cols_to_fill_in = c("pct_good"),
      diverse_vals = TRUE
    )



    grid_df$text_bar <- (100*grid_df$pct_good) %>%
      round(.,0) %>%
      paste(.,"%",sep = "")

    # supress printing percentages lower than 10%, there is no room in the bars
    grid_df$text_bar[grid_df$pct_good < .10] <- ""

    grid_df$text_bar[grid_df$imputed_row] <-"n/a"


    levels(grid_df$grand_mean) <- c(levels(grid_df$grand_mean),"Imputed")
    grid_df$grand_mean[grid_df$imputed_row] <- "Imputed"


    # Masking present values
    # For categories where only one level is still present as non-missing (e.g. Female),
    # replace its values with the the values from the counterpart level (e.g. Male)
    # Previously, we have removed counterparts levels when n < 5. After imputation, however,
    # it is possible to have both cells n>5, yet one of the groups have only NAs, resulting in
    # displaying only one of the levels. Since we decided never to have a situation where
    # only one level is displayed, here I'm maksing the row with existing data.

    grid_df$comb_id <- paste(
      grid_df$metric,
      grid_df$reporting_unit_name,
      grid_df$subset_type,
      sep = "_"
    )

    count_nas <- grid_df %>%
      dplyr::group_by(comb_id) %>%
      summarise(
        is_na = sum(is.na(n)),
        is_not_na = sum(!is.na(n))
      )
    # chose those subset_features, where one level is na, while the other is not na
    rows_to_change <- count_nas[count_nas$is_na == 1 & count_nas$is_not_na == 1,"comb_id"] %>%
      unlist %>% unname

    # the values in the vars below will be used to replace the present row values
    # with the missing row values (yes, the present will become missing)
    vars_to_replace <- c("text_bar", "pct_good", "grand_mean")

    for (row_ in rows_to_change) {
      present_row_df <- grid_df[grid_df$comb_id == row_ & !is.na(grid_df$n),]
      missing_row_df <- grid_df[grid_df$comb_id == row_ & is.na(grid_df$n),]
      present_row_df[, vars_to_replace] <- missing_row_df[, vars_to_replace]
      grid_df[grid_df$comb_id == row_ & !is.na(grid_df$n),] <- present_row_df
    }

    grid_df$comb_id <-NULL
    # end of masking present values
    ######################################


    driver_df_most_recent <- grid_df
    grid_df <- NULL
    ######################################
    # add significance stars

    # for each pair (e.g. male, female) it takes the highest value
    # which later is used to determine height of the text entry
    max_df <- driver_df_most_recent %>%
      dplyr::group_by(reporting_unit_name, metric, subset_type) %>%
      summarise(
        max_pct_good = max(pct_good, na.rm = TRUE)
      )

    driver_df_most_recent <- merge(
      driver_df_most_recent,
      max_df,
      by = c("reporting_unit_name", "metric", "subset_type"),
      all.x = TRUE,
      all.y = FALSE
    )

    driver_df_most_recent$p_aux <- driver_df_most_recent$p
    driver_df_most_recent$p_aux[
      is.na(driver_df_most_recent$p_aux)] <- 1.0

    driver_df_most_recent$sign_star <- ''
    driver_df_most_recent$sign_star[
      driver_df_most_recent$p_aux <.05] <- '  *'
    driver_df_most_recent$sign_star[
      driver_df_most_recent$p_aux <.01] <- ' **'
    driver_df_most_recent$sign_star[
      driver_df_most_recent$p_aux <.001] <- '***'

    excluded_labels <- c("All Students", "Male", "Struct. Adv.","Eng. Speaker", "Not Target Grp.")
    driver_df_most_recent$sign_star[
      driver_df_most_recent$subset_value %in% excluded_labels] <- ''

    driver_df_most_recent$p_aux <- NULL
    max_df <- NULL




    #######################################

    # make sure no extra panels got added
    unique_recent_question_levels <-
      driver_df_most_recent$question_with_dates_wrapped %>% unique
    unique_questions <- driver_df$metric %>% unique %>% as.character()
    if(length(unique_recent_question_levels) > length(unique_questions)){
      logging$warning(
        driver %+% " driver in " %+% report_name %+% " plotted",
        paste0(gsub("\n", " ", unique_recent_question_levels), collapse = "............ "),
        "as panels, which is too many. There should only be",
        length(unique_questions)
      )
    }

    # order the appearance of the separate levels for gender and race
    driver_df_most_recent$subset_value <- driver_df_most_recent$subset_value %>%
      factor(
        levels = all_subset_values_orig
      )

    # order questions by their code in the items table
    buff_df <- driver_df_most_recent[,c("metric", "question_text_wrapped")]
    buff_df <- buff_df[!duplicated(buff_df),] %>% arrange(metric)
    ordered_questions <- buff_df$question_text_wrapped %>% unique()
    buff_df <- NULL

    driver_df_most_recent$question_text_wrapped <-
      driver_df_most_recent$question_text_wrapped %>%
      factor(
        levels = ordered_questions
      )




    # set graph colors
    colors_ <- c("#0a5894","#3faeeb", "#d0d0d0")
    shapes <- c(1,4) #(14,2)

    # if both demographic categories are "n/a", set the colors to two only
    # (i.e., if present_gm_levels below is just c("All Students", "Imputed"))
    present_gm_levels <- driver_df_most_recent$grand_mean %>% as.character %>% unique
    if (identical(sort(present_gm_levels), c("All Students", "Imputed"))) {
      colors_ <- c("#0a5894","#d0d0d0", "#d0d0d0")
    }

    # If bars are too short, they visually cut off the display number.
    # So, if a pct-good is below 20%, bump it up to 20% so you can see the number in the graph.
    # (The number displayed in reports & underlying data won't change, only the bar length.)
    driver_df_most_recent$pct_good[driver_df_most_recent$pct_good < .2] <- .2

    driver_cross_section <-
      ggplot(driver_df_most_recent,
             aes( subset_value, pct_good, fill=grand_mean, color=grand_mean )
      ) +
      # scale_x_discrete( limits=subset_no_all ) +
      geom_bar(
        stat="summary",
        fun.y="mean",
        width = 0.75
      ) +
      scale_fill_manual(
        breaks=c("All","Subset"),
        label=c("Control","Treatment"), # override condition names
        values= colors_ ,
        guide="none"
      ) +
      scale_colour_manual(
        guide="none", # removes the color guide
        values= colors_
      ) +
      geom_text(
        aes(label = text_bar),
        stat="summary",
        fun.y="mean",
        vjust = 0.3, #positive goes down in flipped bar graph (relative to bars)
        hjust = 1.05, #positive goes left in flipped bar graph (relative to bars)
        size = graphing$text_size/3,
        color = "white",
        fontface="bold") +
      # add sign stars
      eval(parse(text=gg_sign_star_str)) +
      facet_grid( question_text_wrapped ~ reporting_unit_name_wrapped) +
      scale_y_continuous(
        breaks = c(0,0.5,1.0),
        labels=percent, expand = c(.1, 0)) +
      theme(
        legend.key=element_rect(color="black", size=graphing$text_size),
        #axis.text=element_text(face="bold", size=graphing$text_size ),
        axis.text.x=element_text(
          face="bold",
          size=graphing$text_size,
          angle=270,
          hjust=0),
        axis.title=element_text(face="bold"),
        panel.background    = element_blank() ,
        #   make the major gridlines light gray and thin
        panel.grid.major.y  = element_line( size=graphing$line_size, colour="#C0C0C0" ),
        #   suppress the vertical grid lines
        panel.grid.major.x  = element_blank() ,
        #   suppress the minor grid lines
        panel.grid.minor    = element_blank() ,
        #   adjust the axis ticks
        axis.ticks    = element_line( size=graphing$line_size , colour="black" ),
        #   move the y-axis over to the left
        axis.title.x  =
          element_text( vjust=-.5 ,
                        face="bold",
                        color="black",
                        angle=0,
                        size=graphing$text_size ) ,
        axis.text =
          element_text( vjust=0 , face="bold",
                        color="black", angle=0,
                        size=graphing$text_size ) ,
        axis.title.y =
          element_text( vjust=.2 , color="black", angle=90, size=graphing$text_size ) ,
        plot.title = element_text(
          colour="black",
          size=plot_title_size,
          face="bold",
          hjust=0,
          vjust = 0),
        plot.margin = unit(c(0,0,0,0), "cm"),
        panel.spacing = unit(panel_margin, "in")
      ) + #theme(strip.switch.pad.grid = unit(5, "cm")) +
      theme(strip.text.y = element_text(angle=0)) +
      xlab("") + ylab("") +
      coord_cartesian(ylim=c(0,1)) + coord_flip() +
      expand_limits(y = c(0,1) )  # <--- This scales pct-good bars to 100% max

    # Save plot to a temporary file so we can pass it to the template.
    # Record the file name we use for later reference.
    cross_section_charts[[driver]] <- driver %+% '_cross_section.png'
    ggsave(cross_section_charts[[driver]],width = 5, height = 5, dpi = 150, units = "in")



    # save to temporary folder and read from there in order to convert to base64
    # https://stackoverflow.com/questions/33409363/convert-r-image-to-base-64
    # https://www.rdocumentation.org/packages/RCurl/versions/1.95-4.11/topics/base64

    # Update: the Rcurl base64 encoding has problems with json conversions,
    # I'm switching  to jsonlite.
    temp_png <- NULL # make sure it doesn't exist from previous operation
    temp_png <- tempfile(pattern = "file", tmpdir = tempdir(), fileext = ".png")
    ggsave(temp_png, width = 5, height = 5, dpi = 150, units = "in")
    cross_section_charts_base_64 [[driver]]  <-
      readBin(temp_png, "raw", file.info(temp_png)[1, "size"]) %>%
      jsonlite::base64_enc()
      #RCurl::base64Encode(., mode = "txt")


    # only print the plot over time IFF there is more than one
    # cycle's worth of non-NA data!
    n_cycles <- driver_df[driver_df$subset_value %in% focus_populations &
                           !is.na(driver_df$pct_good), "cycle_name" ] %>%
      unique %>%
      length

    # @to-do: make the facets work when only 1 facet of data exists


    #############
    #############
    if(n_cycles > 1){
      # filter out any questions within the driver that have only
      # one cycle of data available
      exclude_qs <- recent$question_text[recent$cycle_name %in% 1]
      driver_df_time <- driver_df[!driver_df$question_text %in% exclude_qs, ]

      # define the order of cycles
      levels_ <- driver_df_time$dates_measured_phrase %>% unique %>%
        sort
      driver_df_time$dates_measured_phrase <-
        factor(driver_df_time$dates_measured_phrase,
               levels = levels_)

      # if the graph has too many time points, display only the most recent cycles
      driver_df_time$cycle_ord <- driver_df_time$cycle_name %>%
        compute_ordinal() %>%
        as.numeric

      if (max(driver_df_time$cycle_ord) > max_time_chart_width) {
        cut_off_cycle <- max(driver_df_time$cycle_ord) - max_time_chart_width
        driver_df_time <-
          driver_df_time[driver_df_time$cycle_ord > cut_off_cycle, ]
      }
      driver_df_time$cycle_ord <- NULL


      # order questions by their code in the items table
      driver_df_time$question_text_wrapped <-
        driver_df_time$question_text_wrapped %>%
        factor(
          levels = ordered_questions # this is the same order used in the bar graphs
        )


      # Create offsets for the text percentages

      # For most data points, the text percentages can
      # appear underneath the points on the line graph. But this won't work in cases
      # where the values are within the offset of zero (it will fall off the bottom
      # of the graph). So in those cases, place the text percentages above the line.
      offset_amount <- .15 # set the offset
      driver_df_time$pct_text_offset <- offset_amount * -1 # most percentages appear below the point
      driver_df_time$pct_text_offset[driver_df_time$pct_good <= offset_amount] <- offset_amount # the rest appear above

      df <- driver_df_time[driver_df_time$subset_value %in% focus_populations,]


      driver_time <-
        ggplot( df,
                aes( dates_measured_phrase,
                     pct_good,
                     group=subset_value,
                     shape=subset_value,
                     linetype=subset_value )
        ) +
        geom_path(
          stat="summary", fun.y="mean", color="#0a5894"
        ) +
        geom_point(aes(size = (1-pct_imputed)/9),
                   stat="summary", fun.y="mean", color="#0a5894"
        )  +
        scale_size_area(limits = c(0, 1)) +
        facet_grid( question_text_wrapped ~ reporting_unit_name_wrapped) +
        geom_text(aes(x = dates_measured_phrase, y = pct_good + pct_text_offset, label = pct_good_text), size = graphing$text_size/3) +
        scale_y_continuous( labels=percent) +
        theme(
          # legend.key=element_rect(color="black",size=.5),
          #axis.text=element_text(face="bold", size=graphing$text_size ),
          legend.position="bottom",
          legend.key = element_blank(),
          plot.title = element_text(
            colour="black", size=plot_title_size, face="bold", hjust=0),
          panel.background    = element_blank() ,
          #   make the major gridlines light gray and thin
          panel.grid.major.y  = element_line( size=graphing$line_size, colour="#757575" ),
          #   suppress the vertical grid lines
          panel.grid.major.x  = element_blank() ,
          #   suppress the minor grid lines
          panel.grid.minor    = element_blank() ,
          #   adjust the axis ticks
          axis.ticks    = element_line( size=graphing$line_size , colour="black" ),
          #   move the y-axis over to the left
          axis.title = element_text(face="bold"),
          axis.title.x  =
            element_text( vjust=-.5, color="black", angle=0, size=graphing$text_size ) ,
          axis.title.y =
            element_text( vjust=.2, color="black", angle=90, size=graphing$text_size ),
          axis.text.x =
            element_text(face="bold", angle=270, size=graphing$text_size),
          axis.text.y =
            element_text( vjust=0, color="black", angle=0, size=graphing$text_size ) ,
          plot.margin = unit(c(0,0,0,0), "cm"),
          panel.spacing = unit(panel_margin + .2, "in")
        ) +
        xlab("") + ylab("") +
        scale_x_discrete() +
        # scale_x_datetime(breaks = date_breaks(time_label_spacing), labels = date_format("%m/%d")) +
        # scale_x_date(breaks = seq(min(df$week_start_dt),max(df$week_start_dt), by = time_label_spacing), labels = date_format("%m/%d")) +
        scale_shape( guide_legend(title = "Focus Population") ) +
        scale_linetype( guide="none" ) +
        ggtitle("") +
        coord_cartesian(ylim=c(0,1)) +
        theme(strip.text.y = element_text(angle=0)) +
        theme(legend.position = "none") # I use this to suppress all legends

      # Save plot to a temporary file so we can pass it to the template.
      # Record the file name we use for later reference.
      timeline_charts[[driver]] <- driver %+% '_timeline.png'
      ggsave(timeline_charts[[driver]],width = 5, height = 5.7, dpi = 150, units = "in")

      #save to base64
      temp_png <- NULL
      temp_png <- tempfile(pattern = "file", tmpdir = tempdir(), fileext = ".png")
      ggsave(temp_png, width = 5, height = 5.7, dpi = 150, units = "in")
      timeline_charts_base_64 [[driver]]  <-
        readBin(temp_png, "raw", file.info(temp_png)[1, "size"]) %>%
        jsonlite::base64_enc()
        #RCurl::base64Encode(., mode = "txt")


      # Also! Since there are multiple cycles of data,
      # record an absolute-improvement message for this driver,
      # as long as it's a class-level report.
      if(!TEAM_ONLY) {

        # Get the class-level first pct-goods and last pct-goods for each question
        delta_pct_goods <- df %>%
          filter(reporting_unit_type %in% "class_id") %>%
          arrange(metric, cycle_name) %>%
          group_by(metric) %>%
          summarise(delta_pct_good = last(pct_good) - first(pct_good))

        # Avg together and compare to get abs number, rounded up
        avg_delta_pct_good <- round(100*mean(delta_pct_goods$delta_pct_good, na.rm = TRUE), 0)

        # Define appropriate added message based on avg_delta_pct_good
        added_message <- case_when(avg_delta_pct_good >= 1 & avg_delta_pct_good < 5 ~
                                     "Keep it up!",
                                   avg_delta_pct_good >= 5 & avg_delta_pct_good < 10 ~
                                     "Great progress!",
                                   avg_delta_pct_good >= 10 ~
                                     "Amazing progress!",
                                   TRUE ~ "")


        # Get exact net # of students increased-minus-decreased by:
        # filter to the relevant class
        # melt so that each row is a question response
        # filter to questions in the given LC
        # arrange by student and time
        # summarise to get first-last delta for each question in the LC for each student
        # averaging them to get student-level deltas for the LC
        # counting the number of students with positive deltas minus negative deltas
        my_class_id <- class_id
        q_codes <- unique(df$metric) %>% as.character()
        delta_counts <- data_cat %>%
          filter(class_id %in% my_class_id) %>%
          select(userID, cycle_name, q_codes) %>%
          melt(id.vars = c("userID", "cycle_name")) %>%
          arrange(userID, variable, cycle_name) %>%
          group_by(userID, variable) %>%
          summarise(first = first(value),
                    last = last(value),
                    delta = last(value) - first(value)) %>%
          summarise(mean_delta = mean(delta, na.rm = TRUE)) %>%
          summarise(num_students_pos_delta = sum(mean_delta > 0, na.rm = TRUE),
                    num_students_neg_delta = sum(mean_delta < 0, na.rm = TRUE))
        net_students_pos_delta <- delta_counts$num_students_pos_delta[1] -
          delta_counts$num_students_neg_delta[1]

        # Choose ending string based on driver, with an emergency backup value.
        ending_string <- case_when(driver %in% "teacher_caring" ~
                                     " more students now see you as more caring.",
                                   driver %in% "feedback_for_growth" ~
                                     " more students now see this class as more supportive of their growth.",
                                   driver %in% "meaningful_work" ~
                                     " more students now see this class as more meaningful.",
                                   TRUE ~ " more students now see this class as more engaging.")

        # If the net number of improved students is 1, change the ending_string from plural to singular
        if(net_students_pos_delta %in% 1) {
          ending_string <- gsub("students", "student", ending_string)
          ending_string <- gsub(" see ", " sees ", ending_string)
        }

        # Save NULL if either key number is too low, otherwise save the relevant info
        if(avg_delta_pct_good <= 0 | net_students_pos_delta <= 0) {
          pct_improve_strings[[driver]] <- NULL
        } else {
          pct_improve_strings[[driver]] <- list(avg_delta_pct_good,
                                                added_message,
                                                net_students_pos_delta,
                                                ending_string)
        }

      }


    } else {
      # Don't display a timeline chart in this case.
      timeline_charts[[driver]] <- NA
    }

    date_range <- driver_df_most_recent$dates_measured_phrase_full %>% unique()
    if (length(date_range) > 1) {
      # if date range has multiple values, warn, and take the first one
      # you can consider stopping the scirpt too
      logging$warning("Date range has more than one value" %+% paste(date_range, sep = " "))
      date_range <- date_range[1]
    }

    #update log files
    #buff_df <- driver_df_most_recent[, c("reporting_unit_name", "subset_value", "n")]
    #buff_df <- buff_df[!duplicated(buff_df),] %>%
    #  dcast(reporting_unit_name ~ subset_value )
    #new_basic_log <- bind_rows(
    #  new_basic_log,
    #  buff_df
    #)


    #new_basic_log$team <- team_id
    #new_detailed_log$team <- team_id
    #detailed_log <- bind_rows(detailed_log, new_detailed_log)
    #basic_log <- bind_rows(basic_log, new_basic_log)
    #basic_log$team_only[basic_log$code == code] <- TEAM_ONLY


  } # End of big driver loop!

  #update basic_log
  #new_basic_log <- new_basic_log[!duplicated(new_basic_log),]
  #colnames <- setdiff(names(new_basic_log), "reporting_unit_name")

  # allow team level data to be updated too, if for some reason we save team level information
  #for (reporting_unit_name in new_basic_log$reporting_unit_name){
  #  basic_log[basic_log$class_name ==  reporting_unit_name & basic_log$code == code, colnames] <-
  #    new_basic_log[new_basic_log$reporting_unit_name ==  reporting_unit_name, colnames]
  #}
  #new_basic_log <- data.frame()
  #basic_log$most_recent_week_n[basic_log$code == code]  <-
  #  participation_table_df[nrow(participation_table_df), ncol(participation_table_df)]


  # original css: https://raw.githubusercontent.com/PERTS/gymnast/master/Rmd/rmd_styles.css
  # return the original form of TEAM_ONLY, in case it has been changed in the Rmd
  #TEAM_ONLY <- orig_TEAM_ONLY

  # if unit and team names were changed, undo it

  if (ANONYMOUS) {
    data_cat$team_name[data_cat$team_name %in% team_name] <- team_name_unhashed
    data_cat$class_name[data_cat$class_name %in% class_name] <-
      class_name_unhashed
    data_cat_not_imputed$team_name[data_cat_not_imputed$team_name %in% team_name] <- team_name_unhashed
    data_cat_not_imputed$class_name[data_cat_not_imputed$class_name %in% class_name] <-
      class_name_unhashed

    team_name <- team_name_unhashed
    class_name <- class_name_unhashed
  }

  # adjust text if team only report
  page_title <- class_name
  if (TEAM_ONLY) {
    class_title <- ""
    page_title <- team_name
  }

  # subtract absent_drivers from present_drivers to get the truly present drivers
  # otherwise the html template will get confused and will expect more objects than we
  # currently have

  actually_present_drivers <- setdiff(present_drivers, absent_drivers)

  # if you want to record the objects passed to the html template uncomment this section
  # source(RMD_BASE_PATH %+% "/html_tmpl_object_log.R")


  # create a list of objects which will be passed as json
  # export all outputs here pass them in a list (probably save as file, or experiment with json)
  drivers <- list(
    feedback_for_growth = "fg",
    teacher_caring = "tc",
    meaningful_work = "mw",
    cultural_competence = "cc",
    student_voice = "sv",
    classroom_belonging = "cb",
    social_belonging = "sb",
    trust_fairness = "tf",
    institutional_gms = "igms",
    stem_efficacy = "steme",
    identity_threat = "it"
  )

  create_sublist <- function(driver_name) {
    my_list <- list(
      label = driver_name,
      active = driver_name %in% actually_present_drivers,
      timeline_active = !is.null(timeline_charts[[driver_name]]),
      timeline_chart_64 = timeline_charts_base_64[[driver_name]],
      bar_chart_64 = cross_section_charts_base_64[[driver_name]]
    )
    my_list$pct_improve_string_list <- pct_improve_strings[[driver_name]]
    return(my_list)
  }

  learning_conditions <- lapply(EXPECTED_DRIVERS, create_sublist)

  if (TEAM_ONLY) {
    ru_expected_n <- NA
  } else {
    class_data <- data_cat[data_cat$class_id %in% class_id, ]
    ru_expected_n <- class_data %>%
      dplyr::group_by(class_id) %>%
      dplyr::summarise(expected_n = max(expected_n)) %>%
      dplyr::select(expected_n) %>%
      unlist()
  }

  output_list <- list(
    page_title = page_title,
    report_date = paste0(
      months(lubridate::date(REPORT_DATE)),
      " ",
      day(lubridate::date(REPORT_DATE)),
      ", ",
      year(lubridate::date(REPORT_DATE))
    ),
    team_name =  team_title,
    team_id = perts_ids$get_short_uid(team_id), # from engagement_helpers.R
    classroom_name = class_title,
    classroom_id = class_id,
    classroom_id_url = gsub("Classroom_", "", class_id),
    zero_length_table = zero_length_table,
    max_cycles_missing = MAX_CYCLES_MISSING,
    participation_table_columns = names(participation_table_df),
    participation_table_data = as.matrix(participation_table_df),
    fidelity_tuq = fidelity_tuq,
    fidelity_honest = fidelity_honest,
    learning_conditions = learning_conditions,
    # list where the names are the driver abbreviations, and the values are
    # character vectors of responses.
    open_responses = Map(
      function (or) or$responses,
      open_responses_list
    ),
    ru_expected_n = ru_expected_n,
    date_of_earliest_uncycled_data = date_of_earliest_uncycled_data,
    no_cycles_defined = no_cycles_defined,
    target_msg = target_msg
  )

  # adjust the output list, so single elements will not be autounboxed automatically
  # otherwise the json conversion messes up the open responses
  element_names <- names(output_list$open_responses)
  output_list$open_responses <- wrap_asis(output_list$open_responses, element_names)


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



  logging$debug("########## RESTORE OPTIONS ##########")

  options(scipen = original_scipen)
  options(stringsAsFactors = original_stringsAsFactors)

  return(output_list)
}

