`%>%` <- dplyr::`%>%`
modules::import("ggplot2")

# @todo: remove. This is for debugging/writing the function.
if(dave_debugging == TRUE){
  rm(list=ls())
  github_base_path <- "https://raw.githubusercontent.com/PERTS/gymnast/master/"
  source(paste0(github_base_path,"R/util_legacy.R"), local = TRUE)
  logging <- import_module("logging")
  crypt_path <- util.find_crypt_paths(list(root_name = "rserve_data"))
  should_save_rds <- length(crypt_path$root_name) > 0  # if not found value will be character(0)
  rds_paths <- list(
    args = paste0(crypt_path$root_name,"/rds/lc_by_month_args.rds")
  )
  list2env(readRDS(rds_paths$args), globalenv())
}
# this is real code again.
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
      RCurl::base64Encode() %>%
      # Normaly returns type "base64", bad for jsonlite; make it a character.
      as.character()
  )
}


lc_by_month <- function (
  organization_id = org_id,
  organization_name = org_name,
  report_date = REPORT_DATE,
  organization_associations = assoc,
  items,
  org_data_cat
) {

  # Save load point.
  # if (should_save_rds) {
  #   saveRDS(as.list(environment()), rds_paths$args)
  #   logging$info("Crypt folder found, saving rds to ", rds_paths$args)
  # }


  # SG codes here

  plot <- ggplot2::ggplot(datasets::mtcars, ggplot2::aes(factor(cyl))) + ggplot2::geom_bar()

  return(plot_to_base_64(plot))
}

improvement_by_team <- function (
  organization_id = org_id,
  organization_name = org_name,
  report_date = REPORT_DATE,
  organization_associations = assoc,
  items,
  org_data_cat
) {
  # SG codes here

  plot <- ggplot2::ggplot(datasets::mtcars, ggplot2::aes(factor(cyl))) + ggplot2::geom_bar()

  return(plot_to_base_64(plot))
}
