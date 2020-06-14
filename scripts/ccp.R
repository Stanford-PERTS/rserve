# Generate reports for Copilot's Classroom Connections Program (CCP).
#
# Uses exactly the same scripts as the Engagement Project, just POSTed to a
# different html template on Neptune.

logging <- import_module("logging")
program_reports <- import_module("scripts/copilot/program_reports")

main <- function (...) {
  template <- 'ccp_report'

  logging$info(paste0("###### COPILOT REPORTS: ", template, " ######### \n"))

  program_reports$create_all(..., neptune_report_template = template)
}
