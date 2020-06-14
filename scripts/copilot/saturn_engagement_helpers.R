modules::import(
  "dplyr",
  `%>%`,
  "group_by",
  "group_by_at",
  "one_of",
  "rename",
  "summarise",
  "vars"
)
modules::import("ggplot2", "ggsave")
modules::import("lubridate", "wday")
modules::import("RCurl", "base64Encode")
modules::import("stats", "chisq.test", "na.omit", "var")
modules::import("stringr", "str_trim")

util <- import_module("util")

`%+%` <- paste0

########################## BEGIN HELPER FUNCTIONS ###################################
##############################################################################
##  Helper Functions


any_non_na <- function(x) {
  # returns TRUE if at least one value in a vector is not NA
  return_value <- FALSE
  if (any(!is.na(x))){
    return_value <- TRUE
  }
  return(return_value)
}

# for each code compute the days since the last participation
# for each team, compute the min days_since_last_entry
# exclude teams which have more than 9 days since last entry (for example)

compute_days_since_last_visit <- function(grouping_var, date_var, current_date) {
  # computes the number of days passed since the last visit on group level
  # For example, if
  # Args:
  # grouping_var - a vector which identifies a group membership, such as class, team or code
  # date_var - a date vector which shows the last visit of each member of the group.
  # current_date - a date object, which will be used as a reference point for measuring
  # time distance, it is usually the repot date
  # output:
  # a dataframe with time distance since last visit for each group
  # Example call:
  #  days_since_last_vist_per_code_df <- compute_days_since_last_visit(
  #   grouping_var = data$code,
  #   date_var = date(data$StartDate),
  #   current_date = as.Date(REPORT_DATE)
  # )
  df1 <- data.frame(grouping_var = grouping_var, date_var = date_var)
  df1$time_lag <- current_date - df1$date_var

  output_df <- df1 %>% group_by(grouping_var) %>%
    summarise(min_lag = min(time_lag, na.rm = T)) %>%
    as.data.frame()
  return(output_df)
}

compute_ordinal <- function(x) {
  # recodes a vector as ordianal values
  # e.g. c(1,3,9) becomes (1,2,3)
  orig_values <- unique(x) %>% sort()
  new_values <- 1:length(orig_values)
  x_new <- util$recode(x, orig_values, new_values)
  return(x_new)
}

create_triton_tables <- function(platform_data) {
  # takes platform data and creates triton table for the metascript
  org_tbl <- rename(
    platform_data$triton$Organization,
    organization_name = name,
    organization_id = uid
  )
  class_tbl <- rename(
    platform_data$triton$Classroom,
    class_name = name,
    class_id = uid
  )
  class_tbl$class_name <- str_trim(class_tbl$class_name)

  team_tbl <- rename(
    platform_data$triton$Team,
    team_name = name,
    team_id = uid
  )
  team_tbl$team_name <- str_trim(team_tbl$team_name)

  cycle_tbl <- platform_data$triton$Cycle

  triton_tbl <- merge(
    class_tbl,
    team_tbl,
    by = "team_id",
    all.x = TRUE,
    all.y = FALSE
  )
  keep_cols <- c("team_name", "class_name", "code", "num_students", "team_id",
                 "class_id")
  triton_tbl <- triton_tbl[, keep_cols]
  triton_tbl <- rename(triton_tbl, expected_n = num_students)


  return(list(
    triton_tbl = triton_tbl,
    org_tbl = org_tbl,
    class_tbl = class_tbl,
    team_tbl = team_tbl,
    cycle_tbl = cycle_tbl,
    user_tbl = rename(platform_data$triton$User, user_id = uid),
    participant_tbl = platform_data$triton$Participant,
    program_tbl = platform_data$triton$Program
  ))
}

get_expected_ns_table <- function(id_var, data_table) {
  # This is a little helper function to get the expected ns for each value of a given type
  # of reporting unit ID. It assumes that the data table argument has a column called "expected_n"
  # and another one that is the id_var.

  # sanity checks
  if(! "expected_n" %in% names(data_table)){
    stop(
      "In get_expected_ns_table, data_table requires a column called " %+%
      "'expected_n'"
    )
  }
  if(! id_var %in% names(data_table)){
    stop(
      "In get_expected_ns_table, id_var column " %+% id_var %+%
      " not found in data_table"
    )
  }

  my_table <- data_table %>%
    group_by_at(vars(one_of(id_var))) %>%
    summarise(expected_n = sum(expected_n, na.rm = TRUE)) %>%
    as.data.frame()
  my_table$reporting_unit_id <- as.vector(my_table[, id_var])
  my_table[, id_var] <- NULL
  return(my_table)
}

get_RAM <- function(){
  ram_available <- "24000" # this is the default in GAE. Use it if cannot measure true values.
  ram_msg <- "RAM message not generated. Default RAM is 24000 MB."
  try({
    os <- Sys.info()[1] %>% unname()
    if (os == "Darwin") {
      ram_msg <- system("vm_stat", intern = TRUE)
      ram_available <- vm_stat_total_mem(ram_msg)
      ram_msg <- paste0(ram_msg[1:5], collapse = ";\n")
    }
    if (os == "Linux") {
      ram_msg <- system('cat /proc/meminfo', intern = TRUE)
      ram_available <- sub("(MemTotal: *)(\\d+)( kB)", "\\2", ram_msg[1])
      ram_available <- (as.numeric(ram_available)/1000) %>% as.character()
      ram_msg <- paste0(ram_msg[1:3], collapse = ";\n")
    }
  })
  return(
    list(ram_msg = ram_msg,
         ram_available = ram_available,
         os = os)
  )
}

get_empty_report_notes <- function(ru_id, report_notes) {
  # Assemble messages for each list that the id appears in.
  # Uses the names of the list as messages.
  notes <- character()
  for (name in names(report_notes)) {
    if (ru_id %in% report_notes[[name]]) {
      notes <- c(notes, name)
    }
  }
  if (length(notes) == 0) {
    notes <- "unexplained"
  }

  paste(notes, collapse = ", ")
}

is_Monday <- function(string_date) {
  # check if a string date is Monday and returns TRUE/FALSE
  weekdays(as.Date(string_date)) == "Monday"
}

find_day_of_week <- function (increment, day_abbr) {
  current_day <- Sys.Date()
  for (i in 1:7) {
    day_of_week <- current_day %>%
      wday(., label = TRUE) %>%
      as.character()
    if (day_of_week == day_abbr) {
      return(as.character(current_day))
    }
    current_day <- current_day + increment
  }
}

next_monday <- function() find_day_of_week(+1, "Mon")
last_monday <- function() find_day_of_week(-1, "Mon")

format_reporting_units <- function(ru_id){
  ru_id %>%
    # remove underscores
    gsub("_", " ", .) %>%
    # remove double spaces
    gsub("  ", " ", .)
}

pacific_time <- function() {
  format(Sys.time(), tz = "America/Los_Angeles", usetz = TRUE)
}

p_chi_sq <- function(dv, iv){
  # provided an iv and dv, both categorical variables with two levels only,
  # returns the p-value from a chi-square test.
  # If the variables are not binomial, it returns 1, so if any error is made,
  # it is type 2. The main assumption in the script, however, is that the the
  # demographic predictors are binomial, so if this is vilated, many things need to
  # be changed.
  #iv <- df1$x
  #dv <- df1$y
  len_dv <- length(unique(dv[!is.na(dv)]))
  len_iv <- length(unique(iv[!is.na(iv)]))

  # check if both iv and dv are binary
  if (len_dv != 2 | len_iv != 2)  return(1)

  tryCatch({
    chisq.test(table(dv,iv))$p.value
  }, error = function(e){
    return(1)
  })
}

plot_to_base_64 <- function (plot, width = 5, height = 5, dpi = 150) {
  temp_png <- tempfile(pattern = "file", tmpdir = tempdir(), fileext = ".png")
  ggsave(
    temp_png,
    plot = plot,
    device = "png",
    width = width,
    height = height,
    dpi = dpi,
    units = "in"
  )
  return(
    readBin(temp_png, "raw", file.info(temp_png)[1, "size"]) %>%
      # N.B. Rumen used jsonlite::base64_enc(), which puts backslash-n's in
      # the data. This is okay as long as it's being sent as JSON and later
      # parsed as JSON because they become newline characters, which are
      # legal/ignored in data uris. **However** testing that output
      # by pasting it into an <img> tag will NOT work because the blackslash-n's
      # will corrupt the data. CM is experimenting with this RCurl function which
      # should work in both cases.
      base64Encode() %>%
      # Normaly returns type "base64", bad for jsonlite; make it a character.
      as.character()
  )
}

row_has_true <- function(df){
  # takes response level data and a set of columns, and returns a vector of length
  # nrow(df) where the value is TRUE if one or more values is TRUE
  apply(df, 1, function(x) any(x, na.rm = T))
}

row_is_all_blank <- function(df){
  # takes response level data and a set of columns, and returns a vector of length
  # nrow(df) where the value is TRUE if one or more values is blank and FALSE
  # otherwise
  apply(df, 1, function(x) all(util$is_blank(x)))
}

se <- function(x) sqrt(var(x,na.rm=TRUE)/length(na.omit(x)))

sentence_case <- function(str){
  # underscores to spaces
  # first letter capitalized
  str_spaces <- str %>%
    gsub("_"," ", .) %>%
    str_trim()
  substr(str_spaces,0,1) <- substr(str_spaces,0,1) %>%
    toupper()
  return(str_spaces)
}

vm_stat_total_mem <- function(ram_msg){
  # computes total memory from vm_stat output
  # https://apple.stackexchange.com/questions/81581/why-does-free-active-inactive-speculative-wired-not-equal-total-ram
  fields = list(
    'Pages free:' = NA,
    'Pages inactive:' = NA,
    'Pages speculative:' = NA,
    'Pages wired down:' = NA,
    'Pages stored in compressor:' = NA,
    'File-backed pages:' = NA,
    'Pages active:' = NA
  )
  for (i in 1:length(ram_msg)) {
    try({
      for (field in names(fields)) {
        if (grepl(field, ram_msg[i])) {
          fields[[field]] = sub(paste0("(",field," *)(\\d+)(\\..*)"), "\\2", ram_msg[i]) %>%
            as.numeric
          fields[[field]] <- (fields[[field]] * 4096)/1e+06 # Switch to Mb
        }

      }
    })
  }
  total_ram <- Reduce("+",Filter("is.numeric",fields))
}

wrap_asis <- function(in_list, element_names) {
  # wraps selected elements from a list into I() so they will not be auto_unboxed
  # when transformed to json.
  for (element_name in element_names) {
    in_list[[element_name]] <- I(in_list[[element_name]])
  }
  return(in_list)
}

wrap_text <- function(text, width=35) {
  wtext <- paste(strwrap(text,width=width), collapse=" \n ")
  return(wtext)
}
