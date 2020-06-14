modules::import("dplyr", `%>%`, "filter", "one_of", "rename", "select")
modules::import("lubridate", "floor_date")
modules::import("stats", "setNames")

logging <- import_module("logging")
scale_computation <- import_module("scale_computation")
util <- import_module("util")

helpers <- import_module("scripts/copilot/saturn_engagement_helpers")

`%+%` <- paste0

recode_response_data <- function(
  response_data,
  items,
  subset_config
){
  response_data_recoded <- response_data %>%
    util$apply_columns(util$as_numeric_if_number)

  logging$debug("########## CHUNK DATES ##########")
  ##  Survey responses are grouped by day for tracking purposes

  # compute week_start and day_start values
  survey_date_format <- "%Y-%m-%d %H:%M:%S"
  stripped_time <- strptime(response_data_recoded$StartDate , survey_date_format)
  response_data_recoded$week_start <- floor_date(
    stripped_time, unit = "week"
  ) %>%
    as.Date()
  response_data_recoded$day_start <- floor_date(
    stripped_time, unit = "day"
  ) %>%
    as.Date()

  # all observations must have a class name parameter. Otherwise set it to "Not reporting"
  response_data_recoded$class_name[
    util$is_blank(response_data_recoded$class_name)
  ] <- "Not reporting"

  ########################################################################
  ################### code binary advantaged/disadv subsets ##############
  logging$debug("########## code binary advantaged/disadv subsets ##########")

  adv_race_cols <- items$question_code[items$race_adv] %>% util$na_omit()
  disadv_race_cols <- items$question_code[items$race_disadv] %>% util$na_omit()
  # Participants are NA if they did not answer the race item; and
  # disadvantaged if they chose ANY disadvantaged categories
  response_data_recoded$race_disadv <- helpers$row_has_true(
    response_data_recoded[disadv_race_cols]
  )
  response_data_recoded$race_disadv[
    helpers$row_is_all_blank(response_data_recoded[c(adv_race_cols, disadv_race_cols)])
    ] <- NA

  response_data_recoded$gender_disadv <- !response_data_recoded$gender %in% "male"
  response_data_recoded$gender_disadv[
    util$is_blank(response_data_recoded$gender)
    ] <- NA
  # set the "other" category to NA UNTIL we have language on the reports to explain
  # the labels
  response_data_recoded$gender_disadv[
    response_data_recoded$gender %in% "other"
    ] <- NA

  # code financial stress if the relevant items appear in the subsets config
  subset_types <- unique(subset_config$subset_type)
  if(any(subset_types %in% "financial_stress")){
    fin_stress_vars <- items$question_code[items$financial_stress]
    response_data_recoded$is_food_insecure <- response_data_recoded$food_insecure %in% c(1,2)
    financial_cols <- fin_stress_vars[!fin_stress_vars %in% "food_insecure"]
    response_data_recoded$is_fin_str <- helpers$row_has_true(
      response_data_recoded[financial_cols]
    )
    response_data_recoded$financial_stress_disadv <- response_data_recoded$is_fin_str |
      response_data_recoded$is_food_insecure
    response_data_recoded$financial_stress_disadv[
      helpers$row_is_all_blank(response_data_recoded[c(financial_cols, "food_insecure")])
      ] <- NA
  }

  if(any(subset_types %in% "target_group")){
    response_data_recoded$target_group_disadv <- as.logical(
      response_data_recoded$in_target_group
    )
  }

  # now recode to categoricals based on the subset config
  for(type in subset_types){
    subset_rows <- subset_config %>% filter(subset_type %in% type)
    response_data_recoded[[type %+% "_cat"]] <- util$recode(
      response_data_recoded[[type %+% "_disadv"]],
      originals = subset_rows$disadv,
      replacements = subset_rows$subset_value
    )
  }

  ##### compute composites (where appropriate)
  # use the items df to define the scale_variables input to sc.append_scales
  scale_vars <- items %>%
    filter(composite_input) %>%
    select(question_code, driver) %>%
    rename(
      var_name = question_code,
      scale = driver
    )

  # recode numeric fields as numbers. This does nothing when no scales are requested
  response_data_recoded <- scale_computation$append_scales(
    response_data_recoded,
    scale_vars,
    add_z_score = FALSE
  ) %>%
    setNames(gsub("\\-", "_", names(.)))

  return(response_data_recoded)

}


check_present_recoded_subsets <- function(response_data_recoded, subset_config){
  # check for present subset columns here because prior to this point in the script,
  # we wouldn't expect them to exist.
  expected_subset_cols <- subset_config$subset_type %>% unique() %+% "_cat"
  PRESENT_SUBSET_COLS <- expected_subset_cols[
    expected_subset_cols %in% names(response_data_recoded)
  ]
  if(length(PRESENT_SUBSET_COLS) == 0){
    return("Error: no subset columns were found in imputed data. Rendering no reports.")
  }
}

check_recoded_numerics <- function(response_data_recoded, items){

  numeric_metrics <- items$question_code[
    items$metric_for_reports | items$composite_input
    ] %>%
    unique()

  expected_are_numeric <- response_data_recoded %>%
    select(one_of(numeric_metrics)) %>%
    lapply(is.numeric) %>%
    unlist()

  expected_numerics_are_all_blank <- response_data_recoded %>%
    select(one_of(numeric_metrics)) %>%
    lapply(., function(x) all(util$is_blank(x))) %>%
    unlist()

  # throw an error if expected are neither numeric nor all-blank
  if(!all(expected_are_numeric | expected_numerics_are_all_blank)){
    return("Non-numeric data types were found where numerics were expected." %+%
           " No reports were created.")
  }
}


check_recoded_composites <- function(response_data_recoded, items){
  # make sure all the right scales were computed successfully, if any were computed
  # if none were computed, skip the checks
  scale_vars <- items %>%
    filter(composite_input) %>%
    select(question_code, driver) %>%
    rename(
      var_name = question_code,
      scale = driver
    )

  expected_scale_vars <- scale_vars$scale %>% gsub("\\-", "_", .) %>% unique()

  if(length(expected_scale_vars) > 0){

    if(!all(expected_scale_vars %in% names(response_data_recoded))){
      return("Some expected scales were not computed. No reports generated.")
    }
    composites_are_numeric <- response_data_recoded[expected_scale_vars] %>%
      lapply(is.numeric) %>%
      unlist()
    if(any(!composites_are_numeric)){
      return("computing composites resulted in unexpected data types. No reports generated.")
    }

    all_blank_composites <- response_data_recoded[expected_scale_vars] %>%
      lapply(., function(x) all(util$is_blank(x))) %>%
      unlist()
    if(any(all_blank_composites)){
      logging$info("In computing composites, some columns were found with all-blank values. This is " %+%
                     "technically possible but highly unlikely. It could indicate a failed calculation.")
    }

  }
}



check_recoded_subsets <- function(
  response_data_recoded,
  subset_config,
  emailer = emailer
){
  ###### Make sure subsets got recoded correctly
  subset_cols <- subset_config$subset_type %>% unique() %+% "_cat"
  subset_vals_all <- response_data_recoded[subset_cols] %>%
    unlist() %>%
    unique() %>%
    util$na_omit()
  subset_vals_expected <- subset_config$subset_value

  if(any(!subset_vals_all %in% subset_vals_expected)){
    return("recoding of subsets produced unexpected subset values. No reports " %+%
           "generated.")
  }
  if(any(!subset_vals_expected %in% subset_vals_all)){
    missing_subsets <- subset_vals_expected[!subset_vals_expected %in% subset_vals_all]
    affected_teams <- response_data_recoded$team_id %>% unique()
    n_affected_participants <- length(unique(response_data_recoded$participant_id))
    emailer(
      to = c("", ""),
      subject = ("unexpectedly missing subsets"),
      body_text = "Some expected subsets were missing from the response data " %+%
        "generated for the following teams: " %+%
        paste0(affected_teams, collapse = ", ") %+%
        ". This means NONE of the following subset values were found in the data: " %+%
        paste0(missing_subsets, collapse = ", ") %+%
        ". This can happen by chance if students happen to be " %+%
        "particularly homogeneous within a given batch of reports. " %+%
        "However, it could also indicate problems with the recoding " %+%
        "of subsets. Investigate if the number of teams/participants " %+%
        "sounds too large for subsets to be missing by chance. " %+%
        "(N participants = " %+% n_affected_participants %+% " from " %+%
        length(affected_teams) %+% " team[s])"
    )
  }

  return(NULL)
}
