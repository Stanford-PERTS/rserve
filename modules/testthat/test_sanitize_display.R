context("sanitize_display.R")
# ^^^ First line of file, or else!
# https://github.com/r-lib/testthat/issues/700#issuecomment-367675035

# To run these tests:
#
# * Open a terminal window
# * Change to the rserve directory
# * Run this command:
#   RScript -e "testthat::auto_test('modules', 'modules/testthat')"
#
# The test runner will watch your code files and re-run your tests when you
# change something. Get all those yummy green checks!

if (grepl("modules/testthat$", getwd())) {
  # Switch to rserve root, not the tests directory.
  setwd("../..")
}

library(testthat)

modules::import("dplyr", `%>%`)

sanitize_display <- import_module("modules/sanitize_display")

# this is a mock-up of a subset_config object. In the pipeline,
# the object is called `subsets`
subset_config_default <- data.frame(
  subset_value = c("Race Struct. Adv.", "Race Struct. Disadv.",
                   "Male", "Female", "Target Grp.", "Not Target Grp."),
  subset_type = c("race_cat", "race_cat", "gender_cat", "gender_cat",
                  "target_group_cat", "target_group_cat"),
  stringsAsFactors = FALSE
)
agm_indexes <- c("reporting_unit_id", "metric", "cycle_name", "subset_value")

create_missing_unimputed <- function(subset_config){
  agm <- sanitize_display$simulate_agm(subset_config_default) %>%
    dplyr::filter(!(
      subset_value %in% "Male" &
      cycle_name %in% "Cycle 02 (Date)"
  ))
  return(agm)
}


describe('simulate_agm', {

  it('simulates expected default subsets', {

    agm <- sanitize_display$simulate_agm(subset_config_default)
    expect_equal(
      unique(subset_config_default$subset_value),
      unique(agm$subset_value[!agm$subset_value %in% "All Students"])
    )
  })

  it('creates the expected combined index', {
    agm <- sanitize_display$simulate_agm(subset_config_default)
    expect_equal(nrow(unique(agm)), nrow(unique(agm[agm_indexes])))
  })

  it('simulates small n types', {
    agm <- sanitize_display$simulate_agm(
      subset_config_default,
      small_n_types = "target_group_cat"
    )
    agm_tg <- agm %>%
      dplyr::filter(subset_type %in% "target_group_cat")
    expect_true(all(agm_tg$n < 5))
  })

  it('simulates small missing subset_values', {
    subset_config_no_male <- subset_config_default %>%
      dplyr::filter(!subset_value %in% "Male")
    agm <- sanitize_display$simulate_agm(subset_config_no_male)
    expect_false(any(agm$subset_value %in% "Male"))
  })

  it('can simulate unimputed data with fiddling', {
    # with unimputed data, we cannot count on the property that n values
    # from cycles 2+ will be >= n values from the previous cycle.
    # Therefore, it is possible for subset values to be missing JUST
    # from particular cycles, rather than for the whole agm object.
    agm <- create_missing_unimputed(subset_config_default)
    # make sure we've successfully removed "Male" from the cycle 2 data only
    agm_c2 <- agm %>%
      dplyr::filter(cycle_name %in% "Cycle 02 (Date)")
    expect_true(! "Male" %in% agm_c2$subset_value)
    expect_true("Male" %in% agm$subset_value)

  })

})




describe('anonymize_agm_df', {

  it('didnt change the combined index', {
    agm <- sanitize_display$simulate_agm(subset_config_default)
    agm_anonymized <- agm %>%
      sanitize_display$anonymize_agm_df(mask_within = agm_indexes)
    expect_equal(
      nrow(unique(agm_anonymized)),
      nrow(unique(agm_anonymized[agm_indexes]))
    )
  })

  it('filled in NA for small subset types', {
    small_subs_agm <- sanitize_display$simulate_agm(
      subset_config = subset_config_default,
      small_n_types = "race_cat"
    ) %>%
      sanitize_display$anonymize_agm_df()

    expect_true(
      all(is.na(small_subs_agm$pct_good[small_subs_agm$subset_type %in% "race_cat"]))
    )

    expect_true(
      all(is.na(small_subs_agm$se[small_subs_agm$subset_type %in% "race_cat"]))
    )
  })

  it('filled in NA for both subset values for small types', {
    agm_anon <- sanitize_display$simulate_agm(subset_config_default) %>%
      dplyr::mutate(n = ifelse(subset_value %in% "Male", 4, n)) %>%
      sanitize_display$pixellate_agm_df() %>%
      sanitize_display$anonymize_agm_df()

    # anonymize_agm_df should set pct_good values to NA for male AND female (becasuse
    # they share a subset_type)
    pct_good_male <- agm_anon$pct_good[agm_anon$subset_value %in% "Male"]
    pct_good_female <- agm_anon$pct_good[agm_anon$subset_value %in% "Female"]

    expect_true(all(is.na(c(pct_good_male, pct_good_female))))

  })

  it('didnt fill in NA for non-small subset value types', {
    agm_anon <- sanitize_display$simulate_agm(subset_config_default) %>%
      dplyr::mutate(n = ifelse(subset_value %in% "Male", 4, n)) %>%
      sanitize_display$pixellate_agm_df() %>%
      sanitize_display$anonymize_agm_df()
    # anonymize_agm_df should NOT set any other values to NA besides those
    # corresponding to the subset_type with one small-n cell
    pct_good_non_gender <- agm_anon$pct_good[
      !agm_anon$subset_type %in% "gender_cat"
    ]
    expect_true(!any(is.na(pct_good_non_gender)))
  })

})

describe('expand_subsets_agm_df',{

  #simulate the data
  subset_config_no_male <- subset_config_default %>%
    dplyr::filter(!subset_value %in% "Male")
  agm <- sanitize_display$simulate_agm(subset_config_no_male)
  agm_expanded <- agm %>%
    sanitize_display$expand_subsets_agm_df(subset_config_default)
  default_combined_index <- c("reporting_unit_id", "cycle_name", "subset_value", "metric")

  it('expanded subset values that are missing from all cycles', {
    expect_false("Male" %in% agm$subset_value)
    expect_true("Male" %in% agm_expanded$subset_value)
  })

  it('expands subset values for just cycles 2+ in unimputed data', {

    # there should be the same number of "Male" values for the unimputed
    # data.frame compared with the imputed one. (i.e., it shouldn't matter
    # whether the subset_value was missing from the whole df or from
    # just one cycle)

    agm_unimputed <- create_missing_unimputed(subset_config_default)
    agm_unimputed_expanded <- agm_unimputed %>%
      sanitize_display$expand_subsets_agm_df(subset_config_default)

    imputed_num_rows_male <- agm_expanded %>%
      dplyr::filter(subset_value %in% "Male") %>%
      nrow()
    unimputed_num_rows_male <- agm_unimputed_expanded %>%
      dplyr::filter(subset_value %in% "Male") %>%
      nrow()

    expect_equal(imputed_num_rows_male, unimputed_num_rows_male)

  })

  it('doesnt propagate non-subset indexes - no missing subsets', {

    # we want to add in missing subset values, but we don't
    # want to add in other indexes like cycle/reporting unit/metric
    # combos. So we'll remove one of these combos and check that
    # our expand values thing didn't put it right back in
    agm <- sanitize_display$simulate_agm(subset_config_default)
    agm_missing_metric <- agm %>%
      dplyr::filter(!(
        grepl("01", cycle_name) &
        reporting_unit_id %in% "Classroom_1" &
        metric %in% "mw2_1"
      ))
    agm_missing_metric_expanded <- sanitize_display$expand_subsets_agm_df(
      agm_missing_metric,
      subset_config_default
    )

    is_missing_index <- agm_missing_metric_expanded$metric %in% "mw2_1" &
      agm_missing_metric_expanded$reporting_unit_id %in% "Classroom_1" &
      grepl("01", agm_missing_metric_expanded$cycle_name)

    expect_true(!any(is_missing_index))

  })

  it('doesnt propagate non-subset indexes when subsets are missing', {
    agm_missing_metric_expanded <- create_missing_unimputed(
      subset_config_default
      ) %>%
      dplyr::filter(!(
        grepl("01", cycle_name) &
        reporting_unit_id %in% "Classroom_1" &
        metric %in% "mw2_1"
      )) %>%
      sanitize_display$expand_subsets_agm_df(subset_config_default)

    is_missing_index <- agm_missing_metric_expanded$metric %in% "mw2_1" &
      agm_missing_metric_expanded$reporting_unit_id %in% "Classroom_1" &
      grepl("01", agm_missing_metric_expanded$cycle_name)

    expect_true(!any(is_missing_index))

  })

  it('put NA values in pct_good field', {
    new_pct_good_vals <- agm_expanded$pct_good[
      agm_expanded$subset_value %in% "Male"
    ]
    expect_true(all(is.na(new_pct_good_vals)))
  })

  it('put NA values in se field', {
    new_se_vals <- agm_expanded$se[agm_expanded$subset_value %in% "Male"]
    expect_true(all(is.na(new_se_vals)))
  })

  it('put zeros in n field', {
    new_n_vals <- agm_expanded$n[agm_expanded$subset_value %in% "Male"]
    expect_true(all(!is.na(new_n_vals)))
    expect_true(all(new_n_vals == 0))
  })

  it('didnt change how agm is indexed', {
    unique_rows_by_index <- agm_expanded %>%
      dplyr::select(dplyr::one_of(default_combined_index)) %>%
      unique() %>%
      nrow()
    expect_equal(unique_rows_by_index, nrow(agm_expanded))
  })

  it('produces acceptable bar graph input after display handling', {
    agm_display <- agm_expanded %>%
      sanitize_display$pixellate_agm_df() %>%
      sanitize_display$anonymize_agm_df() %>%
      sanitize_display$display_anon_agm() %>%
      dplyr::mutate(
        question_text_wrapped = metric,
        reporting_unit_name_wrapped = reporting_unit_name
      ) %>%
      dplyr::mutate(max_cycle = max(cycle_name)) %>%
      dplyr::filter(cycle_name %in% max_cycle) %>%
      dplyr::ungroup()

    # There is a special error-checking function we should use, because it's
    # also used at run-time so it can examine live data, not just simulated
    # data.
    error <- sanitize_display$check_bar_graph_input(agm_display, subset_config_default)
    expect_null(error)

  })

  it('throws error when subset_values in data dont match config', {
    unmatching_agm <- sanitize_display$simulate_agm(subset_config_default) %>%
      dplyr::mutate(subset_type = gsub("_cat", "", subset_type))
    expect_error(
      sanitize_display$expand_subsets_agm_df(unmatching_agm, subset_config_default)
    )
  })

})

describe('bar graph input', {

  it('default case', {
    agm_most_recent_display <- sanitize_display$simulate_agm(subset_config_default) %>%
      sanitize_display$pixellate_agm_df() %>%
      sanitize_display$anonymize_agm_df() %>%
      dplyr::filter(cycle_name %in% max(cycle_name)) %>%
      dplyr::mutate(
        reporting_unit_name_wrapped = reporting_unit_name,
        question_text_wrapped = metric
      ) %>%
      sanitize_display$display_anon_agm()

    # There is a special error-checking function we should use, because it's
    # also used at run-time so it can examine live data, not just simulated
    # data.
    error <- sanitize_display$check_bar_graph_input(
      agm_most_recent_display,
      subset_config_default
    )

    # The double-bang unquotes the argument within `expect()` so the test
    # runner will display the _value_ of error$message when things go wrong,
    # which is more useful.
    # https://www.tidyverse.org/blog/2017/12/testthat-2-0-0/#quasiquotation-support
    # https://adv-r.hadley.nz/quasiquotation.html#unquoting
    expect_null(!!error)
  })

  it('generates good bar input with small n subset_types', {

    agm <- sanitize_display$simulate_agm(
       subset_config_default,
       small_n_types = "target_group_cat"
      ) %>%
      sanitize_display$pixellate_agm_df() %>%
      sanitize_display$anonymize_agm_df() %>%
      sanitize_display$display_anon_agm() %>%
      dplyr::mutate(
        question_text_wrapped = metric,
        reporting_unit_name_wrapped = reporting_unit_name
      )  %>%
      dplyr::filter(cycle_name == max(cycle_name))
    error <- sanitize_display$check_bar_graph_input(agm, subset_config_default)
    expect_null(!!error)

  })

  ###########

  it('generates good bar input with small n subset_values', {
    subset_config_no_female <- subset_config_default %>%
      dplyr::filter(!subset_value %in% "Female")

    agm <- sanitize_display$simulate_agm(subset_config_no_female) %>%
      sanitize_display$pixellate_agm_df() %>%
      sanitize_display$expand_subsets_agm_df(
        desired_subset_config = subset_config_default
      ) %>%
      sanitize_display$anonymize_agm_df() %>%
      sanitize_display$display_anon_agm() %>%
      dplyr::mutate(
        question_text_wrapped = metric,
        reporting_unit_name_wrapped = reporting_unit_name
      ) %>%
      dplyr::filter(cycle_name == max(cycle_name))
    error <- sanitize_display$check_bar_graph_input(agm, subset_config_default)
    expect_null(!!error)
  })

  it('generates good bar input with missing unimputed cycle 2+ subsets',{

    agm <- create_missing_unimputed(subset_config_default) %>%
      sanitize_display$pixellate_agm_df() %>%
      sanitize_display$expand_subsets_agm_df(
        desired_subset_config = subset_config_default
      ) %>%
      sanitize_display$anonymize_agm_df() %>%
      sanitize_display$display_anon_agm() %>%
      dplyr::mutate(
        question_text_wrapped = metric,
        reporting_unit_name_wrapped = reporting_unit_name
      ) %>%
      dplyr::filter(cycle_name %in% max(cycle_name))

      error <- sanitize_display$check_bar_graph_input(agm, subset_config_default)
      expect_null(!!error)
  })

})
