# Generate reports for Neptune's CG program,
# "Growth Mindset For College Students"

google_keys <- import_module("google_keys")


handler_util <- import_module("handler_util")
# qualtrics <- import_module("qualtrics")

github_base_path <- "https://raw.githubusercontent.com/PERTS/gymnast/master/"
source(paste0(github_base_path,"R/util_legacy.R"), local = TRUE)


QUALTRICS_SURVEY_ID <- 'SV_1NZ8M7cjGFoA8OF' # full version 'SV_123abv134av'; training 'SV_1NZ8M7cjGFoA8OF'
GOOGLE_KEY_ID <- "https://docs.google.com/spreadsheets/d/e/2PACX-1vSVg412_RjHkULE7IoUEmcqcCLtRuy3wsmGqxrOXnmPxkTOgT79qj0xa5kp02dgtgcFOrN0bxJn1l-T/pub?gid=0&single=true&output=csv"

main <- function (
  auth_header = NA,
  platform_data = NA,
  qualtrics_service,
  script_params = NA,
  should_post = FALSE # if FALSE, data is returned but not posted
) {
  qualtrics_responses <- qualtrics_service$get_responses(QUALTRICS_SURVEY_ID)

  platform_data_present <- !is.na(platform_data) && !length(platform_data$neptune) %in% 0
  qualtrics_data_present <- !nrow(qualtrics_responses) %in% 0

  if (!platform_data_present) {
    print("cg.R$main received no platform data")
  }
  if (!qualtrics_data_present) {
    print("cg.R$main received no qualtrics data")
  }

  if (platform_data_present && qualtrics_data_present) {
    google_keys_df <- google_keys$read_google_key(GOOGLE_KEY_ID)
    fn_that_used_to_be_the_metascript(
      platform_data,
      qualtrics_responses,
      google_keys_df,
      should_post
    )
  }

  print("Continuing to Chris' fake posting code")

  report_data <- list(foo = 'bar', id = NA)
  for (unit in script_params$reporting_units) {
    report_data$id <- unit$id
    filename <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S.html")

    # POST a dataset to Neptune.
    dataset_payload <- list(
      content_type = 'application/json',
      filename = filename,
      data = report_data
    )
    dataset <- handler_util$post_to_platform(
      unit$post_url,
      auth_header,
      dataset_payload
    )

    # POST a reference to this dataset to a report task on Neptune.
    task_attachment_payload <- list(
      link = paste0('/datasets/', dataset$uid, '/passthrough/', filename),
      filename = filename
    )
    task <- handler_util$post_to_platform(
      unit$post_task_attachment_url,
      auth_header,
      task_attachment_payload
    )
  }

  return("cg.R$Main completed")
}

fn_that_used_to_be_the_rmd <- function(example_arg) {
  data <- paste0(example_arg, " -process test- ")
  return(data)
}


fn_that_used_to_be_the_metascript <- function(
  platform_data = NA,
  qualtrics_responses = NA,
  google_keys_df,
  should_post = TRUE
) {
  triton_tbl = create_triton_tbl(platform_data)
  # visually check if the data input is here
  dimensions = list(
    triton_tbl = paste0(dim(triton_tbl)),
    qualtrics_responses = paste0(dim(as.data.frame(qualtrics_responses))),
    google_keys = paste0(dim(google_keys_df))
  )
  print(dimensions)
  for (school in c(1,2,3)) {
    unit_info = list(info1 = "info1",
                     post_url= "url_xxxxxx" )
    example_arg <- paste0("This will be posted to the platform ", school)
    report_data <- fn_that_used_to_be_the_rmd(example_arg)

    if(should_post) {
      response <- handler_util$post_to_platform(
        unit_info$post_url,
        auth_header,
        report_data
      )
      print (response)
      print ("Data posted.")
    } else {
      print("`should_post` is FALSE, skipping POST...")
      print(report_data)
    }
  }
}

create_triton_tbl <- function(platform_data) {
  # takes platform data and creates triton table for the metascript

  class_tbl <- dplyr::rename(platform_data$triton$Classroom,
                             class_name = name,
                             class_id = uid)
  class_tbl$class_name <- util.trim(class_tbl$class_name)

  team_tbl <- dplyr::rename(platform_data$triton$Team,
                            team_name = name,
                            team_id = uid)
  team_tbl$team_name <- util.trim(team_tbl$team_name)

  triton_tbl <- merge(class_tbl, team_tbl, by = "team_id", all.x = T, all.y = F)
  keep_cols <- c("team_name", "class_name", "code",
                 "num_students", "team_id", "class_id")
  triton_tbl <- triton_tbl[,keep_cols]
  triton_tbl <- dplyr::rename(triton_tbl, expected_n = num_students)

  return(triton_tbl)
}


