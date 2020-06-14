# Generate reports for Copilot program: C-SET
#
# Shares the saturn_metascript with BELE-SET.

logging <- import_module("logging")
saturn_reports <- import_module("scripts/copilot/saturn_reports")

main <- function (...) {
  logging$info(paste0("###### C-SET REPORTS ######### \n"))
  return(saturn_reports$create_all(
    ...,
    neptune_report_template = 'beleset_cset_report',
    survey_label = 'cset19'
  ))
}
