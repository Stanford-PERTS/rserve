# Generate reports for Copilot's Engagement Project
#
# Shares the metascript and create_report scripts with CCP.

logging <- import_module("logging")
program_reports <- import_module("scripts/copilot/program_reports")

main <- function (...) {
  template <- 'ep_report'

  logging$info(paste0("###### COPILOT REPORTS: ", template, " ######### \n"))

  program_reports$create_all(..., neptune_report_template = template)
}
