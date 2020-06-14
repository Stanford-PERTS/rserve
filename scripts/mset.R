# Generate reports for Copilot program: BELE-SET
#
# Shares the saturn_metascript with C-SET.

logging <- import_module("logging")
saturn_reports <- import_module("scripts/copilot/saturn_reports")

main <- function (...) {
  logging$info(paste0("###### M-SET REPORTS ######### \n"))
  return(saturn_reports$create_all(
    ...,
    neptune_report_template = 'beleset_cset_report',
    survey_label = 'mset19'
  ))
}
