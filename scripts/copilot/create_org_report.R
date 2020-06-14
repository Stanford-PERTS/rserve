modules::import('dplyr', `%>%`)
community_graphs <- import_module("scripts/copilot/community_graphs")
logging <- import_module("logging")
util <- import_module("util")

create <- function(
  org_id,
  report_date,
  items,
  org_tbl,
  org_team_class,
  org_data_cat,
  subsets,
  team_tbl
) {
  # !! These variables needed for load points.
  crypt_path <- util$find_crypt_paths(list(root_name = "rserve_data"))
  # if not found value will be character(0)
  should_save_rds <- length(crypt_path$root_name) > 0
  rds_paths <- list(
    args = paste0(crypt_path$root_name,"/rds/create_org_report_args.rds")
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

  org_name <- org_tbl[org_tbl$organization_id %in% org_id, "organization_name"]

  # How many weeks does this org's data span? Note that `week_start` is
  # calculated with lubridate::floor_date(date, unit = 'week'), so it's
  # ideal for this.
  if (length(unique(org_data_cat$week_start)) <= 1) {
    logging$info("< 1 week of data. Skipping all plots and delta tables")
    team_by_week_plots <- list()
  } else {
    # plot_teams_by_week returns a list of lists of base_64 graphs
    # the first level groups by "question_code" (i.e., metric)
    # the second level has 1+ graphs. Each graph only has up to 4 teams.
    # For a full description, see comments above function.
    team_by_week_plots <- community_graphs$plot_teams_by_week(
      report_date = report_date,
      organization_associations = org_team_class,
      items,
      org_data_cat,
      base_64=TRUE
    )

    # change_by_team_subgroup returns a list of lists
    ##  The 1st layer of the list is indexed on each question_code.
    ##  The 2nd layer of the list is indexed as
    ##  "label" for printable metric name or "deltas" for a delta data.frame.
    # For a full description, see comments above function.
    delta_tables <- community_graphs$change_by_team_subgroup(
      report_date = report_date,
      organization_associations = org_team_class,
      items,
      org_data_cat,
      subgroups = data.frame(
        # It requires a name convention change in the subsets,
        # e.g. "target_group" to "target_group_cat"
        feature = paste0(unique(subsets$subset_type), "_cat"),
        value = subsets$subset_value[subsets$disadv] %>% as.character(),
        label = subsets$subset_value[subsets$disadv] %>% as.character(),
        stringsAsFactors = FALSE
      )
    )
  }

  # The structure of this list must match the expectations of the Neptune
  # and Triton APIs. Don't change it without coordinating w/ Dev Team.
  report_data <- list()

  # Wrangle the plots and tables for easier display.
  # Assume the plots are already in the correct order.
  report_data$items <- list()
  item_x <- 1
  for (item_id in names(team_by_week_plots)) {
    # `item_id` should be a `question_code` from the items df (totally
    # different from report_data$items below); look up the learning
    # condition (`driver`) from that df so the report can display it
    # nicely.
    driver <- items[items$question_code %in% item_id, "driver"]
    deltas <- delta_tables[[item_id]]$deltas
    report_data$items[[item_x]] <- list(
      team_by_week_plots = team_by_week_plots[[item_id]],
      label = delta_tables[[item_id]]$label,
      learning_condition_label = driver,
      item_id = item_id,
      delta_table = list(
        columns = names(deltas)[!names(deltas) %in% 'team_id'],
        rows = deltas
      )
    )
    item_x <- item_x + 1
  }

  report_data$teams <- list()
  for (team_id in unique(org_team_class$team_id)) {
    report_data$teams[[team_id]] <- list(
      name = team_tbl$team_name[team_tbl$team_id %in% team_id],
      short_uid = team_tbl$short_uid[team_tbl$team_id %in% team_id]
    )
  }

  report_data$id <- org_id
  report_data$organization_id <- org_id
  report_data$organization_name <- org_name
  report_data$report_date <- report_date
  report_data$filename <- paste0(report_date, ".html")

  return(report_data)
}
