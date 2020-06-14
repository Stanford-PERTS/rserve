# CM has approved these imports:

modules::import("ggplot2")
modules::import("stringr", "str_trim")
modules::import("stats", "na.omit")

graphing <- import_module("graphing")
helpers <- import_module("scripts/copilot/saturn_engagement_helpers")
logging <- import_module("logging")
perts_ids <- import_module("perts_ids")
profiler <- import_module("profiler")
sanitize_display <- import_module("modules/sanitize_display")
util <- import_module("util")

`%+%` <- paste0

##############

# CM intends to clean up these imports:

modules::import('lubridate')
modules::import('reshape2')
modules::import('tidyr')
modules::import('jsonlite')
modules::import('readr')
modules::import('tools')
modules::import('dplyr')
modules::import('stats')
modules::import('scales')


create_report <- function(
  SUBSET_TYPES,
  PRESENT_SUBSET_COLS,
  REPORT_DATE,
  TEAM_ONLY,
  REPORTING_UNIT_ID_TYPES,
  MAX_CYCLES_MISSING,
  MIN_CELL,
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
  drivers,
  subsets,
  fidelity_tuq,
  fidelity_honest,
  earliest_data,
  target_msg,
  tg_is_on
) {

  profiler$add_event('start', 'create_saturn_report')

  # !! These variables needed for load points.
  crypt_path <- util$find_crypt_paths(list(root_name = "rserve_data"))
  # if not found value will be character(0)
  should_save_rds <- length(crypt_path$root_name) > 0
  rds_paths <- list(
    args = paste0(crypt_path$root_name,"/rds/saturn_create_report_args.rds"),
    return = paste0(crypt_path$root_name,"/rds/saturn_create_report_return.rds")
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

  EXPECTED_DRIVERS <- items$driver %>% na.omit() %>% unique()

  # Modify subset config depending on whether target groups are on. Target groups
  # should not appear in the config when it's off.
  if(!tg_is_on){
    subsets <- subsets[!subsets$subset_type %in% "target group", ]
  }

  # <HACK>
  # If the team name and classroom name are the same, a later operation that
  # turns a vector of combined names into a factor gets confused, because it
  # expects to find two unique values but only finds one.
  #
  # My hack is to, in this circumstance, change the class name to make sure
  # it's different.
  if (all(agg_metrics_small$team_name == agg_metrics_small$class_name)) {
    team_name <- paste0("(", team_name, ")")
    agg_metrics_small$team_name <- team_name
    agg_metrics_small$reporting_unit_name <- ifelse(
      perts_ids$get_kind(agg_metrics_small$reporting_unit_id) == 'Classroom',
      agg_metrics_small$team_name,
      agg_metrics_small$class_name
    )
  }
  # </HACK>

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

  # Clean up names for presentation
  title_for_full_dataset <- team_name
  class_title <- helpers$format_reporting_units(class_name)
  team_title <- helpers$format_reporting_units(team_name)

  #####   Anonymize report if desired (replace team and unit names with fake ones) #####
  ##
  ##
  team_name_unhashed <- team_name
  class_name_unhashed <- class_name

  # if not team-only and there are not enough students, create a team-only
  # report (copied from old code)
  if(!TEAM_ONLY) {
    min_users_per_cycle <- agg_metrics_small[
      agg_metrics_small$reporting_unit_type %in% "class_id" &
        agg_metrics_small$subset_type %in% "All Students", "n"
      ] %>%
      min(na.rm = TRUE)
    if (min_users_per_cycle < MIN_CELL) {
      TEAM_ONLY <- TRUE
      class_title_full <- ""
      agg_metrics_small <- agg_metrics_small[
        !agg_metrics_small$reporting_unit_type %in% "class_id",
      ]
    }
  }


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
    participation_table_df <- participation_table_df[
      (nrow(participation_table_df) - max_table_width + 1) : nrow(participation_table_df),
    ]
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
    class_recent_open_responses_cleaned <- util$apply_columns(
      class_recent_open_responses_cleaned,
      function(x) {x <- NA}
    )
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

  ############################################################################
  #####   Append driver information & format data for display in ggplot  #####

  # @ to=do, move this to the items-generator code in Dan's script
  # (i.e., should start with an items csv that has all the
  # required columns, rather than fixing it here!
  agm <- merge(
    agg_metrics_small,
    rename(items, metric=question_code),
    by="metric"
  )
  if(nrow(agm) != nrow(agg_metrics_small)){
    stop("appending driver information resulted in dropped rows from agm. " %+%
           "Skipping.")
  }

  ###########################################################################
  ########## Anonymize & pixellate ##########################################
  ###########################################################################
  profiler$add_event('anon & pixellate', 'create_saturn_report')

  #####   Protect privacy of small subsets  #####
  ##  Note: removing just the offending offending cell not good enough because
  ##  let's say n=30, total=72%, Latino=74%. If only one non-Latino,
  ##      you know they're in the "bad" range.
  ##  pixelate groups with no variance (add se even if it does not exist)
  agm_anon <- agm %>%
    dplyr::mutate(
      subset_type = as.character(subset_type),
      metric = as.character(metric)
    ) %>%
    sanitize_display$expand_subsets_agm_df(
      .,
      # because the suffix _cat got added while recoding demographic cols into
      # their labels in the response data, we need to append the _cat suffix
      # to the desired config as well, otherwise merging won't work. (Eventually
      # we should stop appending that suffix)
      desired_subset_config = subsets[c("subset_type", "subset_value")] %>%
        dplyr::mutate(subset_type = paste0(subset_type, "_cat"))
    ) %>%
    # resulting object will be ddf_most_recent_pixellated
    sanitize_display$pixellate_agm_df() %>%
    # we also anonymize wherever cell sizes are < MIN_CELL
    sanitize_display$anonymize_agm_df(min_cell = MIN_CELL) %>%
    sanitize_display$display_anon_agm()

  equal_to_overall <- agm_anon$pct_good == agm_anon$pct_good_all
  small_size <- agm_anon$n < MIN_CELL

  # Note: there SHOULD NOT be NaN values in agm_anon$pct_good, but until we
  # figure out why, the na.rm below will prevent this check from stopping the
  # script too much.
  if(any(small_size & ! equal_to_overall, na.rm = TRUE)){
    stop("anonymization failed to mask data from small samples. " %+%
           "This is a concern for privacy. Skipping.")
  }

  # make sure there are no subset_types with only one value
  # (except for "all students")
  sub_vals_per_type <- agm_anon %>%
    dplyr::filter(grand_mean %in% "Subset") %>%
    dplyr::group_by(reporting_unit_id, subset_type, metric, cycle_name) %>%
    dplyr::summarise(distinct_subset_values = n_distinct(subset_value))

  if (!any(agm_anon$grand_mean %in% "Subset")) {
    logging$warning(
      "All subsets are masked for this report. If this happens often it " %+%
      "may be a bug."
    )
  } else if(!all(sub_vals_per_type$distinct_subset_values == 2)){
    logging$warning("Subset types with only one value appear in the anonymized agg_metrics. " %+%
           "This means a redundant & confusing bar will appear on the graphs. " %+%
           "Affected reporting units: " %+% team_id %+% ", " %+% class_id)
  }

  # Issues with the metascript make it difficult to tell which drivers are
  # actually in the data, because NA columns for all questions were inserted
  # earlier. Identify drivers that have 100% missing data for mean_value in
  # agm. No student has ever responded to these items been in the dataset for
  # this class/team.

  nan_mean_val_drivers <- agm_anon %>%
    dplyr::group_by(driver) %>%
    dplyr::summarise(prop_nan_mean = sum(is.nan(mean_value)) / n())
  drivers_in_data <- nan_mean_val_drivers[nan_mean_val_drivers$prop_nan_mean < 1, "driver"] %>%
    unlist(use.names = FALSE)
  undescribed_drivers <- drivers_in_data[!drivers_in_data %in% EXPECTED_DRIVERS]
  present_drivers <- drivers_in_data[drivers_in_data %in% EXPECTED_DRIVERS]

  ############ xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
  ############ COME BACK TO THIS MOVE ########################
  agm_anon$reporting_unit_name_f <- factor(agm_anon$reporting_unit_name, c(team_name, class_name))

  #for hjust # more negative goes further right

  triage_columns <- c("driver","reporting_unit_name","pct_good","question_text","cycle_name")
  pre_triage <- agm_anon[order(agm_anon$cycle_name, decreasing = TRUE), triage_columns]
  pre_triage <-
    pre_triage[ ! duplicated(pre_triage[,c("question_text","cycle_name")]), ]

  focus_populations <- c("All Students")
  plot_title_size <- 12
  panel_margin <- .2

  #new_basic_log <- data.frame()

  # These will hold the temporary file names of ggplot charts, so they can be
  # passed to the template during knitting. See the YAML block at the end of
  # this file.
  cross_section_charts <- list()
  cross_section_charts_base_64 <- list()
  cross_section_dfs <- list()
  timeline_charts <- list()
  timeline_charts_base_64 <- list()
  timeline_dfs <- list()

  # we need to keep track of those drivers for which we did not create any graph objects
  # later we will subtract them from the present_drivers, so the html template will not
  # get confused about what is truly present
  absent_drivers <- c()

  # also want to keep track of messages to display on the report of pct-improvement
  pct_improve_strings <- list()

  ##################################################################
  ##################################################################

  # BIG LOOP:
  profiler$add_event('big loop', 'create_saturn_report')
  # Loop through each driver and generate graphs
  for(driver in present_drivers){
    # format the question and reporting_unit_name labels


    # driver_df is the subset of agm_anon where the driver matches
    # the driver we're iterating over, and the pct_good value is not NA
    driver_df <- agm_anon[agm_anon$driver %in% driver &
                       !is.na(agm_anon$pct_good),]
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
      function(x) helpers$wrap_text(x, width = 15) #original width was 25
    ) %>%
      unlist()
    driver_df$reporting_unit_name_wrapped <- driver_df$reporting_unit_name_f %>%
      helpers$format_reporting_units() %>%
      lapply(function(x) helpers$wrap_text(x, width = 15)) %>%
      unlist()

    # combine cycles/questions with date ranges for time and bar graphs, respectively
    driver_df$question_with_dates_wrapped <- driver_df$question_text %+% "\n" %>%
      lapply(function(x) helpers$wrap_text(x, width = 19)) %>% unlist
    driver_df$reporting_unit_name_with_dates_wrapped <-
      driver_df$reporting_unit_name %+% "\n" %+% driver_df$dates_measured_phrase %>%
      lapply(function(x) helpers$wrap_text(x, width = 10)) %>%
      unlist()

    # create factor levels to ensure that team results always
    # get faceted above class results
    factor_reporting_unit_name_levels <- function(
      reporting_unit_name_col,
      reporting_unit_label_col,
      team_name,
      class_name
    ){
      levs <- data.frame(reporting_unit_name_col, reporting_unit_label_col) %>%
        unique
      team_levs <- levs[levs[,1] %in% team_name, 2] %>% as.character
      class_levs <- levs[levs[,1] %in% class_name, 2] %>% as.character
      factored_var <- factor(reporting_unit_label_col, c(team_levs, class_levs))
      return(factored_var)
    }

    driver_df$reporting_unit_name_wrapped <- factor_reporting_unit_name_levels(
      driver_df$reporting_unit_name,
      driver_df$reporting_unit_name_wrapped,
      team_name,
      class_name
    )

    # I want to define the most recent data as the most
    # recent cycle WITH VALID RESPONSES.
    driver_df_most_recent <- driver_df[
      driver_df$cycle_name %in% max(driver_df$cycle_name),
    ]

    # now format for display, including significance stars, greyed-out bars, etc.
    ddf_recent_stars <- driver_df_most_recent

    ddf_recent_stars$p_aux <- ddf_recent_stars$p
    ddf_recent_stars$p_aux[
      is.na(ddf_recent_stars$p_aux)] <- 1.0

    ddf_recent_stars$sign_star <- ''
    ddf_recent_stars$sign_star[
      ddf_recent_stars$p_aux <.05] <- '  *'
    ddf_recent_stars$sign_star[
      ddf_recent_stars$p_aux <.01] <- ' **'
    ddf_recent_stars$sign_star[
      ddf_recent_stars$p_aux <.001] <- '***'

    # put the stars over the 'disadvantaged' bars for now
    excluded_subset_vals <- subsets$subset_value[!subsets$disadv]
    ddf_recent_stars$sign_star[
      ddf_recent_stars$subset_value %in% excluded_subset_vals] <- ''

    #######################################

    ddf_recent_display <- ddf_recent_stars

    # make sure no extra panels got added
    unique_recent_question_levels <-
      ddf_recent_display$question_with_dates_wrapped %>% unique
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
    ddf_recent_display$subset_value <- ddf_recent_display$subset_value %>%
      factor(levels = c(subsets$subset_value, "All Students"))

    # order questions by their code in the items table
    buff_df <- ddf_recent_display[,c("metric", "question_text_wrapped")]
    buff_df <- buff_df[!duplicated(buff_df),] %>% arrange(metric)
    ordered_questions <- buff_df$question_text_wrapped %>% unique()
    buff_df <- NULL

    ddf_recent_display$question_text_wrapped <-
      ddf_recent_display$question_text_wrapped %>%
      factor(
        levels = ordered_questions
      )

    # set graph colors and shapes
    colors_ <- c("#0a5894","#3faeeb", "#d0d0d0")
    names(colors_) <- c("All Students", "Subset", "Masked")
    shapes <- c(1,4) #(14,2)

    # Set question text to driver label: C-SET plots composite scores rather
    # than individual item responses. Therefore, there needs to be something
    # different in the `question_text_wrapped` field. We will use the driver
    # name to create a more appropriate value to place there.

    if(all(util$is_blank(ddf_recent_display$question_text_wrapped))){
      driver_label <- drivers$driver_label[drivers$driver %in% driver]
      ddf_recent_display$question_text_wrapped <- "% of students experiencing " %+%
        driver_label %>%
        sapply(., function(x) helpers$wrap_text(x, 15))
    }

    sanitize_error <- sanitize_display$check_bar_graph_input(
      ddf_recent_display,
      subset_config = subsets
    )
    if (!is.null(sanitize_error)) {
      logging$warning(
        "Error found by sanitize_display$check_bar_graph_input(): " %+%
          sanitize_error %+%
          ". Affected RUs: " %+% team_id %+% ", " %+% class_id
      )
    }

    # Now plot
    driver_cross_section <-
      ggplot(ddf_recent_display,
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
        aes(label = pct_good_text),
        stat="summary",
        fun.y="mean",
        vjust = 0.3, #positive goes down in flipped bar graph (relative to bars)
        hjust = 1.05, #positive goes left in flipped bar graph (relative to bars)
        size = graphing$text_size/3,
        color = "white",
        fontface="bold") +
      # add significance stars
      geom_text(
        aes(y = max(pct_good, na.rm = T), label = sign_star),
        stat="summary",
        fun.y="mean",
        vjust = 0.3, #positive goes down in flipped bar graph (relative to bars)
        hjust = 0.3, #positive goes left in flipped bar graph (relative to bars)
        size = graphing$text_size/2,
        color = "black",
        fontface="bold",
        angle = 270
      ) +
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

    # save as encoded to base_64 for html template
    cross_section_charts_base_64[[driver]]  <- helpers$plot_to_base_64(driver_cross_section)
    # also save the data.frame and the regular plot too
    cross_section_dfs[[driver]] <- ddf_recent_display
    cross_section_charts[[driver]] <- driver_cross_section


    # only print the plot over time IFF there is more than one
    # cycle's worth of non-NA data!
    n_cycles <- driver_df %>%
      dplyr::filter(
        subset_value %in% focus_populations,
        !is.na(pct_good)
      ) %>%
      dplyr::select(cycle_name) %>%
      unique() %>%
      nrow()

    # @to-do: make the facets work when only 1 facet of data exists


    #############
    #############
    if(n_cycles > 1){
      # filter out any questions within the driver that have only
      # one cycle of data available
      recent <- driver_df %>%
        dplyr::group_by(question_text) %>%
        dplyr::summarise(cycle_name=max(cycle_name))

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
        helpers$compute_ordinal() %>%
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

      if(all(util$is_blank(df$question_text_wrapped))){
        driver_label <- drivers$driver_label[drivers$driver %in% driver]
        df$question_text_wrapped <- "% of students experiencing " %+%
          driver_label %>%
          sapply(., function(x) helpers$wrap_text(x, 15))
      }

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
      # save as encoded to base_64 for html template
      timeline_charts_base_64[[driver]]  <- helpers$plot_to_base_64(driver_time)
      # also save the data.frame and the regular plot too
      timeline_dfs[[driver]] <- df
      timeline_charts[[driver]] <- driver_time


      # Also! Since there are multiple cycles of data,
      # record an absolute-improvement message for this driver,
      # as long as it's a class-level report.
      if(!TEAM_ONLY) {

        # Get the class-level first pct-goods and last pct-goods for each question
        delta_pct_goods <- df %>%
          dplyr::filter(reporting_unit_type %in% "class_id") %>%
          dplyr::arrange(metric, cycle_name) %>%
          dplyr::group_by(metric) %>%
          dplyr::summarise(delta_pct_good = last(pct_good) - first(pct_good))

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
          dplyr::filter(class_id %in% my_class_id) %>%
          dplyr::select(participant_id, cycle_name, q_codes) %>%
          reshape2::melt(id.vars = c("participant_id", "cycle_name")) %>%
          dplyr::arrange(participant_id, variable, cycle_name) %>%
          dplyr::group_by(participant_id, variable) %>%
          dplyr::summarise(first = first(value),
                    last = last(value),
                    delta = last(value) - first(value)) %>%
          dplyr::summarise(mean_delta = mean(delta, na.rm = TRUE)) %>%
          dplyr::summarise(num_students_pos_delta = sum(mean_delta > 0, na.rm = TRUE),
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

    date_range <- ddf_recent_display$dates_measured_phrase_full %>% unique()
    if (length(date_range) > 1) {
      # if date range has multiple values, warn, and take the first one
      # you can consider stopping the scirpt too
      logging$warning("Date range has more than one value" %+% paste(date_range, sep = " "))
      date_range <- date_range[1]
    }
  }# End of big driver loop!

  profiler$add_event('after big loop', 'create_saturn_report')

  # original css: https://raw.githubusercontent.com/PERTS/gymnast/master/Rmd/rmd_styles.css
  # return the original form of TEAM_ONLY, in case it has been changed in the Rmd
  #TEAM_ONLY <- orig_TEAM_ONLY

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
  # export all outputs here pass them in a list
  create_sublist <- function(driver_name) {
    my_list <- list(
      label = driver_name,
      active = driver_name %in% actually_present_drivers,
      timeline_active = !is.null(timeline_charts[[driver_name]]),
      timeline_chart_64 = timeline_charts_base_64[[driver_name]],
      bar_chart_64 = cross_section_charts_base_64[[driver_name]],
      # These will be checked in tests, not displayed in the report.
      bar_chart_df = cross_section_dfs[[driver_name]],
      timeline_df = timeline_dfs[[driver_name]]
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

  # Save load point.
  if (should_save_rds) {
    saveRDS(as.list(environment()), rds_paths$return)
    logging$info("Crypt folder found, saving rds to ", rds_paths$return)
  }

  ############################################################################
  ########################## LOAD POINT: return ##############################
  ############################################################################

  # list2env(readRDS(rds_paths$return), globalenv())

  if (is.null(class_id)) {
    classroom_id_url <- NULL
  } else {
    classroom_id_url <- perts_ids$get_short_uid(class_id)
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
    team_id = team_id,
    team_id_url = perts_ids$get_short_uid(team_id),
    classroom_name = class_title,
    classroom_id = class_id,
    classroom_id_url = classroom_id_url,
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
  output_list$open_responses <- helpers$wrap_asis(
    output_list$open_responses,
    element_names
  )

  logging$debug("########## RESTORE OPTIONS ##########")

  options(scipen = original_scipen)
  options(stringsAsFactors = original_stringsAsFactors)

  profiler$print('create_saturn_report')
  profiler$clear('create_saturn_report')

  return(output_list)
}

