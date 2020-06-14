modules::import('tools')
modules::import('stats')
modules::import('lubridate')
modules::import('utils')
modules::import('methods')
modules::import('dplyr')
modules::import('reshape2')
github_base_path <- "https://raw.githubusercontent.com/PERTS/gymnast/master/"
source(paste0(github_base_path,"R/util_legacy.R"), local = TRUE)

logging <- import_module("logging")
`%>%` <- dplyr::`%>%`

##############################################################################
########################## BEGIN HELPER FUNCTIONS ############################
##############################################################################

get_expected_ns_table <- function(id_var, data_table) {
  # This is a little helper function to get the expected ns for each value of a given type
  # of reporting unit ID. It assumes that the data table argument has a column called "expected_n"
  # and another one that is the id_var.

  # sanity checks
  if(! "expected_n" %in% names(data_table)){
    stop("In get_expected_ns_table, data_table requires a column called 'expected_n'")
  }
  if(! id_var %in% names(data_table)){
    stop("In get_expected_ns_table, id_var column " %+% id_var %+% " not found in data_table")
  }

  my_table <- data_table %>%
    dplyr::group_by_at(vars(one_of(id_var))) %>%
    summarise(expected_n = sum(expected_n, na.rm = TRUE)) %>%
    as.data.frame()
  my_table$reporting_unit_id <- as.vector(my_table[, id_var])
  my_table[, id_var] <- NULL
  return(my_table)
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
  apply(df, 1, function(x) all(util.is_blank(x)))
}

plot_to_base_64 <- function (plot, width = 5, height = 5, dpi = 150) {
  temp_png <- tempfile(pattern = "file", tmpdir = tempdir(), fileext = ".png")
  ggplot2::ggsave(
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
      RCurl::base64Encode() %>%
      # Normaly returns type "base64", bad for jsonlite; make it a character.
      as.character()
  )
}

snake_case <- function(str){
  str %>%
    gsub("\\.", "_", .) %>%
    gsub(" ", "_", .) %>%
    gsub("_$", "", .) %>%
    gsub("\\\\", "_", .) %>%
    tolower()
}

wrap_text <- function(text, width=35) {
  wtext <- paste( strwrap(text,width=width), collapse=" \n ")
  return(wtext)
}

sentence_case <- function(str){
  # underscores to spaces
  # first letter capitalized
  str_spaces <- str %>%
    gsub("_"," ", .) %>%
    util.trim()
  substr(str_spaces,0,1) <- substr(str_spaces,0,1) %>%
    toupper()
  return(str_spaces)
}

mkdn_friendly_html <- function(str){
  # regex removes spaces after carriage returns
  # (if a line starts with spaces, markdown renders as code block)
  gsub("[\r\n]+ *", "", str)
}

se <- function(x) sqrt(stats::var(x,na.rm=TRUE)/length(na.omit(x)))

p_aov <- function(dv, iv){
  # provided an iv and dv, return the omnibus p from an anova
  tryCatch({
    summary(aov(dv ~ iv))[[1]][["Pr(>F)"]][1]
  }, error = function(e){
    return(1)
  })
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

p_log_reg <- function(dv, iv){
  # provided an iv and dv, both categorical variables with two levels only,
  # returns the p-value from a logistic regression.
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
    summary(glm(dv~iv,family="binomial"))$coefficients[2,4]
    #chisq.test(table(dv,iv))$p.value
  }, error = function(e){
    return(1)
  })
}


##############################################################################
##  Reused Text Configuration ####



driver_desc <- list(
  "belonging" = list(
    "description" = "When students feel socially connected, supported, and respected,
    they are less distracted by insecurities and more likely to engage in learning effectively.
    See [mindsetscholarsnetwork.org/learning-mindsets/belonging/](http://mindsetscholarsnetwork.org/learning-mindsets/belonging).
    ",
    "introduction" = "
    <p>
    Students who are confident they belong and are valued by their teachers and
    peers are able to engage more fully in learning. They have fewer behavior
    problems, are more open to critical feedback, take greater advantage of
    learning opportunities, build important relationships, and generally have
    more positive attitudes about their classwork and teachers. In turn, they
    are more likely to persevere in the face of difficulty and do better in school.
    <br><br>
    When students are uncertain about whether they belong, they are vigilant
    for cues in the environment that signal whether or not they belong, fit in,
    or are welcome there. They may also be concerned about confirming a negative
    stereotype about their group. This hyper-vigilance and extra stress uses up
    cognitive resources that are essential for learning, diminishing their
    performance and discouraging them from building valuable relationships.
    </p>
    <p><b>Recommendations</b><br>
    To learn specific strategies for promoting belonging, visit
    the [PERTS Mindset Kit Belonging Course](http://www.mindsetkit.org/belonging).
    </p>

    "
  ),
  "feedback_for_growth" = list(
    "description" = 'Students learn more effectively when their teachers recognize and encourage progress, and offer supportive and respectful critical feedback to help them improve. To learn more, visit <a href="https://www.perts.net/conditions#feedback-for-growth">perts.net/conditions#feedback-for-growth</a>.',
    "introduction" = ""
  ),
  "teacher_caring" = list(
    "description" = 'Students engage more deeply in their work when they feel like their teachers like and care about them. To learn more, visit <a href="https://www.perts.net/conditions#teacher-caring">perts.net/conditions#teacher-caring</a>.',
    "introduction" = ""
  ),
  "meaningful_work" = list(
    "description" = 'Students are more motivated to learn when they see how class material relates to their lives outside of school. To learn more, visit <a href="https://www.perts.net/conditions#meaningful-work">perts.net/conditions#meaningful-work</a>.',
    "introduction" = ""
  ),
  "cultural_competence" = list(
    "description" = 'Students who feel a strong sense of belonging find greater success in school. Develop the skills to connect with and validate students from diverse backgrounds. To learn more, <a href="https://equitablelearning.org/books/n7Bw4Xso">see the <em>Cultural Competence</em> book on equitablelearning.org</a>.',
    "introduction" = ""
  ),
  "relevance" = list(
    "description" = "When students understand the value and relevance of their schoolwork, they are better able to learn deeply and resist distractions.
    See [mindsetscholarsnetwork.org/learning-mindsets/purpose-relevance/](http://mindsetscholarsnetwork.org/learning-mindsets/purpose-relevance/).",
    "introduction" = ""
  ),
  "growth_mindset_(general)" = list(
    "description" = "A growth mindset is the belief that intelligence can be developed.",
    "introduction" =
      "<p>Students’ beliefs about intelligence have important consequences for how they
    experience school and how they respond to setbacks and adversity. When students
    hold a fixed mindset, school can be a threatening place because they may be
    worried about proving their ability or avoiding “looking dumb.” This can
    lead students to avoid challenges and give up when they struggle. But
    when students hold a growth mindset, they may experience school as
    an exciting place to grow, embracing challenges as opportunities to develop mastery.
    </p>

    <p>
    Researchers have found that it is possible to promote a growth mindset by
    teaching students about neuroscience evidence showing that the brain is
    malleable and gets stronger through effort, trying new strategies, and
    seeking help when necessary. Researchers have also learned that we can
    encourage students to adopt more of a growth mindset by changing the way
    in which we interact with them.
    <br><br>
    Researchers have found that one way to help students develop a growth mindset
    is by teaching them about neuroscience evidence that shows the brain is
    malleable. In these programs, students learn that the brain is like a
    muscle—when you challenge it, it gets stronger. Importantly, students
    also learn that sheer effort is not enough. The right strategies and
    advice from others are equally important for strengthening the brain.
    <br><br>
    Crucially, mindset programs such as these do not simply tell
    students to adopt a growth mindset. They help them understand why
    effort, the right strategies, and good advice are important—because
    these actions help students develop their intelligence. And by asking
    students to write about what they’ve learned in service of other students
    who are struggling, students come to internalize the message themselves.
    <br><br>
    In multiple studies with thousands of students across the country,
    researchers have found that students who receive these programs earn
    more course credits, higher grades, and higher standardized test scores.
    </p>
    <p><b>Recommendations</b><br>
    To learn specific strategies for promoting a growth mindset, visit
    the [PERTS Mindset Kit](http://www.mindsetkit.org).
    </p>
    "
  ),
  "growth_mindset_(teacher_support)" = list(
    "description" = "A growth mindset is the belief that intelligence can be developed. When teachers do things like offering feedback to improve, asking students to explain their thinking, and praise students' effort,
    this sends the message to students that they can become smarter with effort and good strategies.",
    "introduction" =
      "<p>Students’ beliefs about intelligence have important consequences for how they
    experience school and how they respond to setbacks and adversity. When students
    hold a fixed mindset, school can be a threatening place because they may be
    worried about proving their ability or avoiding “looking dumb.” This can
    lead students to avoid challenges and give up when they struggle. But
    when students hold a growth mindset, they may experience school as
    an exciting place to grow, embracing challenges as opportunities to develop mastery.
    </p>

    <p>
    Researchers have found that it is possible to promote a growth mindset by
    teaching students about neuroscience evidence showing that the brain is
    malleable and gets stronger through effort, trying new strategies, and
    seeking help when necessary. Researchers have also learned that we can
    encourage students to adopt more of a growth mindset by changing the way
    in which we interact with them.
    <br><br>
    Researchers have found that one way to help students develop a growth mindset
    is by teaching them about neuroscience evidence that shows the brain is
    malleable. In these programs, students learn that the brain is like a
    muscle—when you challenge it, it gets stronger. Importantly, students
    also learn that sheer effort is not enough. The right strategies and
    advice from others are equally important for strengthening the brain.
    <br><br>
    Crucially, mindset programs such as these do not simply tell
    students to adopt a growth mindset. They help them understand why
    effort, the right strategies, and good advice are important—because
    these actions help students develop their intelligence. And by asking
    students to write about what they’ve learned in service of other students
    who are struggling, students come to internalize the message themselves.
    <br><br>
    In multiple studies with thousands of students across the country,
    researchers have found that students who receive these programs earn
    more course credits, higher grades, and higher standardized test scores.
    </p>
    <p><b>Recommendations</b><br>
    To learn specific strategies for promoting a growth mindset, visit
    the [PERTS Mindset Kit](http://www.mindsetkit.org).
    </p>"
  ),
  "growth_mindset_(ask_for_help)" = list(
    "description" = "When students adopt a growth mindset, they feel safe asking for help, because help is the way to increase intelligence, rather than a sign of low ability.",
    "introduction" = ""
  )
  )

# add names to the driver descriptions
for (driver in names(driver_desc)) {
  driver_desc[[driver]]$name <- gsub("_", " ", driver)
  driver_desc[[driver]]$name_sent_case <- driver_desc[[driver]]$name %>% sentence_case()
  driver_desc[[driver]]$name_title_case <- driver_desc[[driver]]$name %>% toTitleCase()
}



salt_n_hash_short <- function(x, salt){
  salt_n_hash(x, salt) %>%
    lapply(.,function(x) strtrim(x,7))
}

impute_values <- function(input_df, id_cols, time_col, columns_to_impute) {
  # If a value for a given question is missing, we replace the missing
  # value with the value from the previous time period. We will also
  # add a vector which encodes if the values in the original column was
  # imputed or not.
  # Args:
  ## input_df - the dataframe
  ## id_combination - columnname. this is a unique identifier for each participants.
  ##   typically it is a combination of team, report unit and user_id.
  ## The column consists of unique strings (e.g. "Dream_Team_Mr.Smith_id12345")
  ## primary_time - the time column which will be used to order values
  ##   typically this is the week_start, but it could be end_time
  ## columns_to_impute - the names of the columns which will be imputed
  # Output
  # the ouput is a data frame
  ## for each colum from column in columns_to_impute we will create a new column,
  ## having the suffix _imp, containing the imputed value, and another column,
  # with the suffix _imp_log, which will have information if the value is imputed
  # (TRUE) or not (FALSE)

  # Update: this function does not add new rows, it only substite existing missing values


  # Example call:
  #d <- impute_values(
  #  input_df = d_not_imputed,
  #  id_cols = c("team_id", "reporting_unit_id", "userID"),
  #  time_col = "EndDate",
  #  columns_to_impute =c ("fg1_1", "fg2_1", "fg3_1")
  #)

  # I will not be using dplyr because of the non-standard evaluation issues

  # if there are multiple id_cols, concatenate them together
  if (length(id_cols) > 1) {
    input_df$id_col_comp <- apply( input_df[,id_cols] , 1 , paste , collapse = "-" )
  } else {
    input_df$id_col_comp <- input_df[,id_cols]
  }

  # create imputation column names and imputation-logic column names
  column_names_imp <- paste(columns_to_impute, "_imp", sep = "")
  column_names_imp_log <- paste(columns_to_impute, "_imp_log", sep = "")
  input_df[,c(column_names_imp)] <- input_df[,c(columns_to_impute)]
  input_df[,c(column_names_imp_log)] <- FALSE

  # order by user_id composite index, and by time
  input_df$orig_row_indx <- rownames(input_df) %>% as.numeric()
  input_df <- input_df[
    order(
      input_df[,"id_col_comp"],
      input_df[,time_col]
    ),
    ]

  for (j in 2:nrow(input_df)) {
    if (input_df[j,"id_col_comp"] == input_df[j-1,"id_col_comp"]) {
      for (k in 1:length(columns_to_impute)) {
        if (util.is_blank(input_df[j,column_names_imp[k]]) &
            !util.is_blank(input_df[j-1,column_names_imp[k]])) {
          input_df[j,column_names_imp[k]] <- input_df[j-1,column_names_imp[k]]
          input_df[j,column_names_imp_log[k]] <- TRUE
        }
      }
    }
  }


  #restore original order and clean up unnecessary variables
  input_df <- input_df[order(input_df$orig_row_indx),]
  input_df$orig_row_indx <- NULL
  input_df$id_col_comp <- NULL

  return(input_df)
}

impute <- function(x, y){
  if(!is.na(x) & is.na(y)){
    y <- x
  }
  return(y)
}

is_imputed <- function(x, y){
  if(!is.na(x) & is.na(y)){
    return(TRUE)
  } else{
    return(FALSE)
  }
}



impute_values_purr <- function(input_df, id_cols, time_col, columns_to_impute) {
  # currently not working properly. Using group_by does not prevent it from
  # imputing from the subject above (imputation should be within subject)
  # further, it doesn't seem to decrease processing time much. As a test run with
  # 1900 rows, the for loop took 47seconds, the accumulate function took 45 seconds.


  # Example call:
  #d <- impute_values(
  #  input_df = d_not_imputed,
  #  id_cols = c("team_id", "reporting_unit_id", "userID"),
  #  time_col = "EndDate",
  #  columns_to_impute =c ("fg1_1", "fg2_1", "fg3_1")
  #)

  # I will not be using dplyr because of the non-standard evaluation issues

  # if there are multiple id_cols, concatenate them together
  if (length(id_cols) > 1) {
    input_df$id_col_comp <- apply( input_df[,id_cols] , 1 , paste , collapse = "-" )
  } else {
    input_df$id_col_comp <- input_df[,id_cols]
  }

  # create imputation column names and imputation-logic column names
  column_names_imp <- paste(columns_to_impute, "_imp", sep = "")
  column_names_imp_log <- paste(columns_to_impute, "_imp_log", sep = "")
  input_df[,c(column_names_imp)] <- input_df[,c(columns_to_impute)]
  input_df[,c(column_names_imp_log)] <- FALSE

  # order by user_id composite index, and by time
  input_df$orig_row_indx <- rownames(input_df) %>% as.numeric()
  input_df <- input_df[
    order(
      input_df[,"id_col_comp"],
      input_df[,time_col]
    ),
    ]
  grouped_in_df <- input_df %>% dplyr::group_by(id_col_comp)
  for(col in columns_to_impute){
    grouped_in_df[[col %+% "_imp_p"]] <- accumulate(grouped_in_df[[col]], .f = impute)
    grouped_in_df[[col %+% "_imp_log_p"]] <-
      ifelse(is.na(grouped_in_df[[col]]) & !is.na(grouped_in_df[[col %+% "_imp_p"]]),
             TRUE,
             FALSE
      )
  }

  input_df <- grouped_in_df %>% as.data.frame()
  for (j in 2:nrow(input_df)) {
    if (input_df[j,"id_col_comp"] == input_df[j-1,"id_col_comp"]) {
      for (k in 1:length(columns_to_impute)) {
        if (util.is_blank(input_df[j,column_names_imp[k]]) &
            !util.is_blank(input_df[j-1,column_names_imp[k]])) {
          input_df[j,column_names_imp[k]] <- input_df[j-1,column_names_imp[k]]
          input_df[j,column_names_imp_log[k]] <- TRUE
        }
      }
    }
  }
  #table(grouped_in_df$fg1_1_imputed, input_df$fg1_1_imp)
  #table(grouped_in_df$fg1_1_is_imputed, input_df$fg1_1_imp_log)

  #restore original order and clean up unnecessary variables
  input_df <- input_df[order(input_df$orig_row_indx),]
  input_df$orig_row_indx <- NULL
  input_df$id_col_comp <- NULL

  return(input_df)
}



any_non_na <- function(x) {
  # returns TRUE if at least one value in a vector is not NA
  return_value <- FALSE
  if (any(!is.na(x))){
    return_value <- TRUE
  }
  return(return_value)
}

compute_ordinal <- function(x) {
  # recodes a vector as ordianal values
  # e.g. c(1,3,9) becomes (1,2,3)
  orig_values <- unique(x) %>% sort()
  new_values <- 1:length(orig_values)
  x_new <- util.recode(x, orig_values, new_values)
  return(x_new)
}



insert_missing_weeks <- function(weeks_vec_in) {
  # takes a vector of strings which represent dates (week starts)
  # and adds the missing weeks. For example, if the string has week
  # 1 and 4 for September, the output vector will include weeks 1,2,3,4
  # example vector of string dates:
  # c("2017-04-23", "2017-04-16", "2017-04-02")
  # it assumes that dates distances are multiple of 7 days
  # it removes duplicated weeks

  #weeks_vec_in <- c("2017-04-23", "2017-04-16", "2017-04-02")
  weeks_vec_in <- weeks_vec_in %>% unique %>% sort %>% as.Date()
  # check if distances are multiple of 7
  difs <- diff(weeks_vec_in, 1) %>% as.numeric
  difs <- difs/7
  if ( !all(difs == floor(difs)) ) {
    msg <- "The distances between week starts are not multiples of 7. Script stopped."
    stop(msg)
  }

  out_vect <- c()
  time_steps <- difftime(max(weeks_vec_in), min(weeks_vec_in), units = "weeks") %>% as.numeric()
  for (j in 0:time_steps) {
    current_date <- min(weeks_vec_in) + j*7
    current_date <- current_date %>% as.character()
    out_vect <- c(out_vect, current_date)
  }
  out_vect <- out_vect %>% as.character()
  return(out_vect)
}





replace_missing <- function(in_df, shared_col, cols_to_fill_in, diverse_vals = FALSE) {
  # if there is deterministic dependecy between columns (e.g. the value
  # when the value of col1 is "white", the value of col2 is always "race")
  # we can use the values from the first column to fill in missing values in
  # the second

  # the assumption is that the rows either have missing values only in
  # cols_to_fill_in, or no missing values at all (in the same col)

  # if diverse_vals is set to TRUE, cols_to_fill_in might have different values (
  # instead of repeating the same value for each row)

  #Example call
  #grid_df <- replace_missing(
  #  in_df = grid_df,
  #  shared_col = "metric",
  #  cols_to_fill_in = c("question_with_dates_wrapped", "question_text")
  #)



  check_nrow <- nrow(in_df)
  in_df$rowindex <- as.numeric(rownames(in_df))
  # split in two dfs, one with missing values, the other one with
  # present values
  all_nas <- data.frame(in_df[,cols_to_fill_in]) %>%
    apply(., 1, function(x) all(is.na(x)))


  missing_df <- in_df[all_nas,]
  present_df <- in_df[!all_nas,]

  # create df which will have the missing information
  information_df <- present_df[,c(shared_col, cols_to_fill_in)]
  information_df <- information_df[!duplicated(information_df),]

  # if the rows did not have the same information, as expected, stop
  # the script
  test_length <- information_df[,shared_col] %>% duplicated %>% sum
  if (test_length > 0 & !diverse_vals) {
    stop("Error in the function replace_missing. The shared_col does not define unique rows, for the same value of shared_col, there are different values of cols_to_fill_in.")
  }


  # OLD VERSION - breaks on Dan's machine, switching to dplyr version below
  # missing_df[,cols_to_fill_in] <- NULL
  missing_df <- select(missing_df, -one_of(cols_to_fill_in))


  missing_df <- merge(
    missing_df,
    information_df,
    by = shared_col,
    all.x = TRUE,
    all.y = FALSE
  )
  in_df <- rbind(missing_df, present_df)
  if( nrow(in_df) != check_nrow) {
    stop("Error in the function replace_missing. Different number of rows for input and output!")
  }
  in_df <- in_df %>% arrange(rowindex)
  in_df$rowindex <- NULL
  return (in_df)
}

repair_pdds <- function(in_cell) {
  # it seems that in some cases the pdds are wrong
  # instead of __pdd__ we have _pdd__
  out_cell <- gsub("([^_])_pdd", "\\1__pdd", in_cell)
  return (out_cell)
}

repair_2nd_row <- function(in_df) {
  # it seems that in some cases the pdds are wrong
  # instead of __pdd__ we have _pdd__
  in_df[1,] <- in_df[1,] %>% apply (., 1, function(x) repair_pdds(x))
  return (in_df)
}

is_Monday <- function(string_date) {
  # check if a string date is Monday and returns TRUE/FALSE
  answer <- FALSE
  tryCatch({
    date_ <- as.Date(string_date)
    day_of_week <- weekdays(date_)
    if(day_of_week == "Monday") {
      answer = TRUE
    }
  }, error = function(e){
    util.warn("wrong data fromat for is_Monday!")
  })
  return(answer)
}

format_reporting_units <- function(ru_id){
  ru_id %>%
    # remove underscores
    gsub("_", " ", .) %>%
    # remove double spaces
    gsub("  ", " ", .)
}



make_percentage <- function(x) {
  paste(" (",x,"%)", sep = "")
}

# get the start and end dates of each week
add_days <- function(date_character_vector, n_days){
  date_character_vector %>%
    as.Date %>%
    as.numeric() %>%
    sapply(., function(x) sum(x, n_days)) %>%
    as.Date(origin = "1970-01-01")
}

#the vector bellow contains names of teams which we do not need to generate
# reports for (demos and testings)
excluded_team_ids <- c(
  "Example_Team_id"
)

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
  df1$time_lag <- (current_date - df1$date_var) %>% as.numeric()

  output_df <- df1 %>% dplyr::group_by(grouping_var) %>%
    summarise(min_lag = min(time_lag, na.rm = T)) %>%
    as.data.frame()
  return(output_df)
}

separate_reporting_units <- function(requested_rus, team_study_class_df) {
  # checks if requested reporting units are in the database, and creates
  # lists with teams and classes to include, and teams which need to be included in
  # the database
  # input:
  ## requested_rus - requessted reporting units, could be teams, classes, or both
  ## team_study_class_df - a data frame with classes and teams which are available for report
  ## generation. Available classes are those whose teams have recent data (not older than X days)
  ## ru_request$all_teams - all teams for which we will need to keep data
  ##       (a team could be included here because the class was requested)
  ## ru_request$requested_teams - teams which were explicitly requested and are present in the recent data
  ## ru_request$requested_classes - classes which are requested, and whose teams have recent data
  ## ru_request$non_matched_units - requested unitis which are not found in recent data
  ## ru_request$possible_classes <- classes which are part of all_teams
  # example call: ru_request <-
  #                 separate_reporting_units(requested_rus, team_study_class_df)

  ru_request <- list()

  ru_request$all_teams <- team_study_class_df$team_id[team_study_class_df$class_id %in% requested_rus]
  ru_request$requested_teams <- team_study_class_df$team_id[team_study_class_df$team_id %in% requested_rus] %>% unique
  ru_request$all_teams <- c(ru_request$all_teams, ru_request$requested_teams)  %>% unique

  ru_request$possible_classes <- team_study_class_df$class_id[team_study_class_df$team_id %in% ru_request$all_teams]
  ru_request$requested_classes <- requested_rus[requested_rus %in% ru_request$possible_classes]

  ru_request$non_matched_units <-
    requested_rus[!requested_rus %in% c(ru_request$all_teams, ru_request$possible_classes )]
  if (length(ru_request$non_matched_units) > 0 ) {
    logging$info("No recent data was found for the following reporting units: ",
           paste0(ru_request$non_matched_units, collapse = ", ")
    )
  }
  sum_check <- length(ru_request$requested_classes) +
    length(ru_request$requested_teams) +
    length(ru_request$non_matched_units)
  if (length(requested_rus) != sum_check) {
    "There is mismatch between the number of requested units and number of returned units. Check function `separate_reporting_units`" %>%
      stop()
  }
  return(ru_request)
}

find_day_of_week <- function (increment, day_abbr) {
  current_day <- Sys.Date()
  for (i in 1:7) {
    day_of_week <- current_day %>%
      lubridate::wday(., label = TRUE) %>%
      as.character()
    if (day_of_week == day_abbr) {
      return(as.character(current_day))
    }
    current_day <- current_day + increment
  }
}

next_monday <- function() find_day_of_week(+1, "Mon")
last_monday <- function() find_day_of_week(-1, "Mon")

df <- data.frame(a = c(1,2), b = c(1,2))
paste_tbl <- function(tbl, sep = "\t", include_names = FALSE) {
  # pastes table rows (matrix, data frame) as string sepated by \n
  out_str <- ""
  if(length(tbl) == 0) { return(out_str) }
  if (include_names) {
    out_str <- paste0(out_str, paste0(names(tbl), collapse = sep), "\n")
  }
  #concatenates separate lines from a table
  for (n in 1:nrow(tbl)) {
    out_str <- paste0(out_str, paste0(tbl[n,], collapse = sep), "\n")
  }
  return(out_str)
}


top_mem_objects <- function(env) {
  # returns the largest memory objects in Kb (if divisor is set to 1024)
  # Example call: top_mem_objects(env =  environment())
  ls_objects = ls(env)
  n <- 10
  divisor <-  1024
  # note: I tried to avoid the for loops by using sapply, but it worked only in some cases
  output_df <- data.frame()
  for (obj in ls_objects) {
    object_size <- NA
    tryCatch({
      object_size <- utils::object.size(get(obj, envir = env))/divisor
      object_size <- as.numeric(object_size) %>% round(.,0)
    } , error = function(err) {
      # pass
      # logging$error(paste0(obj, ": ",err))
    })
    current_df <- data.frame(var = obj, size = object_size)
    output_df <- dplyr::bind_rows(output_df, current_df)
  }
  output_df <- output_df %>% dplyr::arrange(-size)
  output_df <- output_df[1:n,]
  return_info <- paste0(output_df$var, ": ", output_df$size, collapse = ", ")
  return(return_info)
}

wrap_asis <- function(in_list, element_names) {
  # wraps selected elements from a list into I() so they will not be auto_unboxed
  # when transformed to json.
  for (element_name in element_names) {
    in_list[[element_name]] <- I(in_list[[element_name]])
  }
  return(in_list)
}

ru_availability <- function(requested_rus, data_raw, triton_tbl, team_class_output_list) {
  # creates a table with one entry per ru, and columns indicating in which lists the ru is present
  if (is.null(requested_rus)) {return(data.frame(ru=c("No requests"), note = c("Most likely, report created locally")))}
  requested_rus <- unique(requested_rus)
  requested_rus <- requested_rus[!is.na(requested_rus)]
  delivered_rus <- names(team_class_output_list) %>% unique
  delivered_rus <- delivered_rus[!is.na(delivered_rus)]

  # note: one of the merges bellow invovles a key which has many NAs with a key which
  # has no NAs. It works fine in the few scenarios I tested, but in the future this might have
  # to be rewriten

  output_tbl <- data_raw %>% group_by(code) %>%
    summarise(total_n = n(),
              max_start_date = max(StartDate,na.rm = T),
              min_start_date = min(StartDate,na.rm = T)) %>%
    mutate(in_qualtrics = 1) %>%
    merge(., triton_tbl, by = "code", all = TRUE) %>%
    merge(.,
          data.frame(requested_rus = requested_rus, id = requested_rus), # the separate id is to preserve the key column, otherwise it is deleted
          by.x = "class_id",
          by.y = "id",
          all = TRUE) %>%
    merge(.,
          data.frame(delivered_rus = delivered_rus, id = delivered_rus),
          by.x = "requested_rus",
          by.y = "id",
          all = TRUE
          )
  output_tbl$created <- pacific_time()
  return(output_tbl)
}

pacific_time <- function() {
  current_time <-
    Sys.time() %>% as.character %>%   as.POSIXct() %>%
    format(., tz="America/Los_Angeles",usetz=TRUE)
  return(current_time)
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

create_triton_tables <- function(platform_data) {
  # takes platform data and creates triton table for the metascript
  org_tbl <- dplyr::rename(
    platform_data$triton$Organization,
    organization_name = name,
    organization_id = uid
  )
  class_tbl <- dplyr::rename(
    platform_data$triton$Classroom,
    class_name = name,
    class_id = uid
  )
  class_tbl$class_name <- util.trim(class_tbl$class_name)

  team_tbl <- dplyr::rename(
    platform_data$triton$Team,
    team_name = name,
    team_id = uid
  )
  team_tbl$team_name <- util.trim(team_tbl$team_name)

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
  triton_tbl <- dplyr::rename(triton_tbl, expected_n = num_students)


  return(list(
    triton_tbl = triton_tbl,
    org_tbl = org_tbl,
    class_tbl = class_tbl,
    team_tbl = team_tbl,
    cycle_tbl = cycle_tbl,
    user_tbl = dplyr::rename(platform_data$triton$User, user_id = uid),
    participant_tbl = platform_data$triton$Participant,
    program_tbl = platform_data$triton$Program
  ))
}
############### END HELPER FUNCTIONS #####################################################
