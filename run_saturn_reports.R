# This file runs RServe for a single program similar to how it would run in the
# cloud. The intended result is to consume real data and save real reports to
# our platforms. Recall that all Copilot reports are saved in preview mode such
# that only super admins can see them. They are released from preview mode on
# Sunday night.
#
# This will NOT sent any emails about the run because it presumes you are
# monitoring the results.
#
# ## Requirements
#
# * a "workspace" crypt mounted containing this folder structure:
#   - rserve_data/
#     - rds/
#     - copilot_request_(beleset|cset).json
# * the contents of the copilot request JSON file comes from
#   https://copilot.perts.net/cron/rserve/reports/{RSERVE SCRIPT NAME HERE}?really_send=false
#   which contains authentication valid for 8 hours, permitting this
#   script to save reports to Copilot/Neptune.
#   - Customize the URL for the intended program. If you want `cset.R` to run,
#     then use 'cset' in the URL.
#   - Visit the URL in your browser and save the result as a .json file
#     'rserve_data/copilot_request_{RSERVE SCRIPT NAME HERE}.json'. If you
#     can't access this URL, ask Chris for permission.
# * working directory set to analysis repo on your local machine
#
# ## Configuration
#
# * Choose if you should specify the report date, which would be the Monday on
#   which the report would be delivered, and which operates on the data
#   recorded 7 days prior.
# * Choose if you'd like the script to send emails to PERTS staff by adjusting
#   which `emailer` line is commented out.
#
# ## Run from the command line
#
# From the analysis directory, with 'cset' or 'beleset' as the program to run:
#
#     Rscript Copilot/run_saturn_reports.R <program-to-run>
#
# ## Run from RStudio
#
# Start a new R session with a clean environment. Uncomment one of the lines
# below to manually set a `script_path`. Source the whole file.

# Get name of script to run from command line arguments.
args <- commandArgs(trailingOnly = TRUE)
script_path <- args[1]

# If not using the command line, uncomment this and set the desired script name
#script_path <- 'beleset'
#script_path <- 'cset'

known_saturn_programs <- c('beleset', 'cset', 'mset')
if (!script_path %in% known_saturn_programs) {
  stop(paste(script_path, "is not a recognized saturn program script."))
}

if (!grepl('/rserve/?$', getwd())) {
  stop("Your working directory should be the root of the rserve repo.")
}

# # Load RServe's dependencies.
modules::depend('jsonlite')
deps <- unlist(jsonlite::read_json('package.json')$R$dependencies)
for (package in names(deps)) {
  modules::depend(package, deps[[package]])
}

# Import the scripts that RServe's request handlers normally would.
script <- import_module(paste0('scripts/', script_path, '.R'))
import_data <- import_module('modules/import_data.R')$import_data
saturn <- import_module('saturn.R')
util <- import_module('util.R')

# Get the request from the json file.
paths <- list(request = paste0('copilot_request_', script_path, '.json'))
raw_json <- util$find_crypt_paths(paths)$request
request <- jsonlite::fromJSON(raw_json, simplifyDataFrame = FALSE)

report_data_list <- script$main(
  auth_header = request$headers$Authorization,
  platform_data = import_data(
    neptune_sql_credentials = request$payload$neptune_sql_credentials,
    triton_sql_credentials = request$payload$triton_sql_credentials
  ),
  qualtrics_service = NULL,
  saturn_service = saturn$create_service(request$payload$saturn_sql_credentials),
  sheets_service = NULL,
  # fake emailer that just prints:
  emailer = function (to, subject, body_text, ...) {
    print('!!!!!!!!!!!!!!! fake email')
    print('to:')
    print(to)
    print('subject:')
    print(subject)
    print('body_text:')
    print(body_text)
  },
  # real emailer:
  # emailer = function (...) send_email(request$payload$mandrill_api_key, ...),
  script_params = list(
    # report_date = '2020-05-25',  # or else it will assume next monday
    reporting_units = request$payload$reporting_units
  )
)
