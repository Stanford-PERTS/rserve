# RServe

A pure R webserver. Boilerplate for running R routines as a webservice.


## Installation

1. Install docker.
2. Install the R version matching the `base` R-dependency in `package.json`.
  - [R 3.6.2 installer for MacOS](https://cran.r-project.org/bin/macosx/el-capitan/base/R-3.6.2.pkg)
3. Install brew: `/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"`
4. `brew update`
5. Install codeship's CLI for containerized builds called "jet":
   `brew cask install codeship/taps/jet`
6. Install python 2 (for rendering templates): `brew install python@2`
7. Insatll jinja2 (for rendering templates): `pip2 install --user jinja2`
8. Install node: `brew install node`
9. `npm install`

If, on step 9, you receive the error "npm: command not found", in at least one case, the following extra step solved it:

`brew postinstall node`


## Approaches to running RServe

Run all the following commands from the `rserve` directory:

`cd rserve`

### Running on your local R installation

This environment isn't as predictable because it uses whatever installation of R you already have. But it's much faster because you don't have to build or stop a docker container and the `nodemon` package can watch the source code for changes and automatically restart.

`npm run server`

You can see this running by visiting `localhost:8080` in your browser.

If you need to specify a port other than 8080, provide it as an argument to the script: `npm run server -- 9080`.

### Running a local Docker container

This requires you to have docker installed, and building and stopping means it's a little slow to see your changes, but the environment is more reliable because it uses a fixed version R and should be an exact copy of the deployed environment where it will actually run.

`./run_docker.sh`

If you're running another process on :8080, you can map the container's internal port to something else, e.g. `./run_docker.sh 4321` and then visit `localhost:4321`.

To turn it off (e.g. so you can rebuild it after a change):

`./stop_docker.sh`

### Deploying

You need to have gcloud installed and have editor permission on the `rserveplatform` project.

`./deploy_production.sh`

Then visit https://rserveplatform.appspot.com/ to see it run.

### Developing

App Engine logs anything sent to stdout. For R scripts, use the `logging.R` module. It has various levels to help separate different kinds of messages, going from most verbose to least verbose.

* DEBUG: Highly granular, almost certainly ignorable, most verbose level.
* INFO: Default choice for logging.
* WARNING: For events that are suspicious but aren't certain to be a problem.
* ERROR: Developers should be notified if any of these occur.
* CRITICAL: Really bad. Rarely used, least verbose level.

A logging call like this:

```r
logging$info("Got table:", dim(my_data_frame))
```

Results in a log like this:

```
INFO Got table: 32 11
```

Avoid other means of logging such cat `cat()`, `print()`, `message()`, or the html-based `util.warn()`.


## Writing scripts

Write a pure-R file that has, at minimum, this code:

```r
main <- function (auth_header,
                  platform_data,
                  qualtrics_service,
                  saturn_service,
                  sheets_service,
                  emailer) {
  # your code here
}
```

Don't use `library()`. Any packages you use other than `base` need to either be referenced explicitly with the `::` operator, or explicitly imported like `modules::import('utils', 'read.csv')`, so that your code can be loaded as a module; see the [modules vignette](https://cran.r-project.org/web/packages/modules/vignettes/modulesInR.html).

Name it in `lower_snake_case` with the extension `.R`. Save it either in the `rserve/scripts` directory, or—if you want it run daily—in the `rserve/scripts/daily` directory.

For helper code, add files to the `modules` directory and import them with `helper <- import_modules("my_helper")`

### Available data

* `auth_header` is used to send requests back to Neptune so that results can be saved there. See [Sending data to Neptune](#sending-data-to-neptune).
* `platform_data` is a list with elements `neptune` and `triton`, each of which are lists of available tables as data frames. For example, `platform_data$neptune$Checkpoint` is a data frame of all Neptune checkpoints.
* `qualtrics_servce` already contains necessary credentials for interacting with the Qualtrics API. Use methods documented in `qualtrics.R`, e.g. `qualtrics_service$get_responses(survey_id)`.
* `saturn_service` similarly helps connect to saturn.
* `sheets_service` similarly helps connect to google sheets owned by the `rserveplatform` GCP project.
* `emailer` similarly helps send email via PERTS' Mandrill account.

Further arguments may be available depending on how Neptune has passed data to this script via `script_params` in the request. Typical arguments include:

* `reporting_units` list, each element of which contains data about a reporting unit to process, including `unit_id` and `post_url`.
* `should_post` scalar boolean, when FALSE signals that the user wants the processed data returned at the end of the function and not send in a POST to Neptune.
* `post_url` scalar character, set when Neptune requests that the result data be POSTed to a specific URL.

## Making Requests

RServe is a web server, so a end-to-end test begins with making an http request. These requests must have correct authorization or will be rejected with a 401 status. Various parameters in the body of the request will customize behavior.

### Credentials

* All credentials for running the script are available in `rserve_credential_body.json` in `perts_crypt.vc`.
* The json above was assembled using information from the following files and folders in `perts_crypt.vc`: `/app engine credentials/neptune/service accounts/rserve-delegation.json`, `/ssl keys and certs/cloud_sql/neptune_analysis_replica`, `/ssl keys and certs/cloud_sql/triton_analysis_replica`.
* In addition there are two headers to include:
  - Authorization
    + Can be set to "Bearer testing" if testing RServe locally and in isolation
    + Can be set to a valid jwt signed by the platform private key. See [JWTs](#jwts).
  - Content-Type should be "application/json"

You can see exactly what credentials and parameters are used by Copilot to trigger RServe by visiting this URL as a project admin:

    https://copilot.perts.net/cron/rserve/ep?really_send=false

You can do the same thing with a local instance of copilot:

    http://localhost:10080/cron/rserve/ep?really_send=false

### JWTs (JSON Web Tokens)

RServe authenticates requests by looking for a jwt ("jot") in the Authorization header. These must be signed with the RSA512 algorithm by either the dev (local) or production (deployed) private key. Using jwt is required (as opposed to the "Bearer testing" shortcut) if you want Copilot and Neptune to recognize RServe's outgoing calls to create reports.

The easiest way to get a non-expired, correctly-signed jwt is to use the `really_send=false` URLs, described above. They can also be created manually; the private keys are either in the platform `config.py` files called `default_jwt_secret_rsa` for local environments or in a `SecretValue` datastore entity for deployed environments.

### Request parameters

The request body should be a JSON object with keys:

* `qualtrics_credentials` -   Object. Required to download qualtrics data, omitted if using faked qualtrics responses. Structure is `{"api_key": ""}`.
* `rserve_service_account_credentials` - Object. Optional. Allows use of Google APIs to record logs and other meta data in google sheets. Stored in `perts_crypt.vc`, see [Credentials](#credentials).
* `triton_sql_credentials` -  Object. Required. Empty object (`{}`) if not using SSL (e.g. for local databases). Otherwise structure is `{"ca": "", "cert": "", "key": ""}`, stored in `perts_crypt.vc`, see [Credentials](#credentials).
* `neptune_sql_credentials` - Object. Required. Same as `triton_sql_credentials`.
* `saturn_sql_credentials` - Object. Optional. Same structure as `triton_sql_credentials` except it includes a password for the database user. Use if Saturn is the survey engine for this program.
* `mandrill_api_key` - String. Optional. Allows RServe scripts to send email.
* `neptune_sql_user` - String. Optional. Used to connect to a particular db instance. A typical value for local testing is 'neptune'.
* `neptune_sql_ip` - String. Optional. Used to connect to a particular db instance. A typical value for local testing is '127.0.0.1'.
* `neptune_sql_password` - String. Optional. Used to connect to a particular db instance. A typical value for local testing is 'neptune'.
* `triton_sql_user` - String. Optional. Used to connect to a particular db instance. A typical value for local testing is 'triton'.
* `triton_sql_ip` - String. Optional. Used to connect to a particular db instance. A typical value for local testing is '127.0.0.1'.
* `triton_sql_password` - String. Optional. Used to connect to a particular db instance. A typical value for local testing is 'triton'.

Other parameters are defined by individual scripts.

#### ep.R

* `run_program` - String, optional. For this script, the value should only be "ep19" (as of Sep 2019). If this key is present, it overrides any value for `reporting_units` and instructs RServe to process all teams and classrooms in the program.
* `reporting_units` - Array of objects with structure:
  - `post_url` - String. URL where RServe should POST the completed report's dataset. This is the data that populates the report template.
  - `team_id` - String. Uid of related team.
  - `classroom_id` - String or null. Same as `id` if this unit is a classroom, otherwise null.
  - `id` - String. Uid of this reporting unit (may or may not be the same as `team_id`)
  - `post_report_url` String. URL where RServe should POST the report's meta data for Copilot/Triton. This is the data that allows Copilot to list available reports for a given team.
* `use_fake_qualtrics_responses` - Boolean. Optional. If this key is present, and the qualtrics credentials are omitted, RServe will generate fake qualtrics responses.
* `save_workspace_only` - Boolean. Optional. Default FALSE. If TRUE, it will skip most funtions of the script and just save rds files to a mounted crypt with a folder called "rserve_data". Only tested via R, not as an API parameter. See `create_metascript_workspace.R`.

This is a complete example request body for a single team using all local instances and fake data, assuming Neptune is on `localhost:8080`, Triton is on `localhost:10080`, and RServe is on `localhost:9080`.

```json
{
  "reporting_units": [
    {
      "id": "Team_36ec8bf27bfb",
      "post_url": "http://localhost:8080/api/datasets?parent_id=Team_36ec8bf27bfb",
      "post_report_url": "http://localhost:10080/api/reports",
      "classroom_id": null,
      "team_id": "Team_36ec8bf27bfb"
    }
  ],
  "neptune_sql_credentials": {},
  "neptune_sql_user": "neptune",
  "neptune_sql_ip": "127.0.0.1",
  "neptune_sql_password": "neptune",
  "triton_sql_credentials": {},
  "triton_sql_user": "triton",
  "triton_sql_ip": "127.0.0.1",
  "triton_sql_password": "triton",
  "use_fake_qualtrics_responses": true
}
```

#### ccp.R

This script is functionally identical to `ep.R`, except that it sends out reports with a different template name, `ccp_report.html`. Future code may

- `run_program` - String, optional. For this script, the value should only be "ccp19" (as of Sep 2019). If this key is present, it overrides any value for `reporting_units` and instructs RServe to process all teams and classrooms in the program.

### Sending data to Neptune

Send a POST request to Neptune like this:

```r
handler_util <- import_module("handler_util")
handler_util$post_to_platform(post_url, auth_header, data)
```

Note that the `url` may have been provided by the request, and that `auth_headers` is always the first argument provided.

Neptune should respond with a `Dataset` id.


## Code organization

* `app.R` - handles routing and structuring requests and responses
* `app.yaml` - configuration for when deployed to app engine
* `docker_init.sh` - the first script run when the docker container starts up, just runs `server.R`
* `Dockerfile` - configuration for docker container
* `environment.R` - tools for detecting run time environment, e.g. if deployed.
* `handlers.R` - functions called when requests to certain paths are received by RServe
* `modules/` - sets of functions that can be imported into other scripts
  - `handler_util.R` - helpers for web requests and responses
  - `http_error.R` - helpers for returning http error responses, e.g. 404
  - `qualtrics.R` - for interacting with the Qualtrics API
  - `import_data.R` - functions for downloading data from external services
* `server.R` - the root R file for launching RServe
* `scripts/` - all scripts that analyze data go here
* `scripts/daily/` - all scripts we wish to run daily go here
* `test` - end-to-end tests, including submodule clones of triton, neptune, and saturn


## RServe/Neptune interface

See [RServe/Neptune interface](https://gist.github.com/chris-perts/3d1cf3ad6446a13ebef0d61152f127bd).

### Inter-server authentication

Neptune needs to be able to prove its identity to RServe, so that RServe only responds to authentic requests.

RServe needs to be able to prove its identity to Neptune, so Neptune only stores authentic processed data.

1. Neptune stores a private key from an asymmetric key pair in the Datastore as a `SecretValue`.
2. Neptune composes an http request with a jwt with these properties:
  a. Header `{"typ": "JWT", "alg": "RS512"}` i.e. using asymmetric RSA encryption
  b. Payload with the path and digest of the body, to prevent forgery.
  c. Payload with a `jti` nonce, which RServe can repeat back.
3. RServe hard-codes the public key.
4. When receiving a request, RServe verifies it by:
  a. Verifying the JWT with the public key
  b. Checking that the path and body digest match the payload
5. RServe replies with a simple 200 and processes in the background.
6. When finished, Rserve sends a request to Neptune with its results in the body, including the `jti` value of the original request, but without further authentication.
7. Neptune receives the request, and verifies the incoming `jti` against its cache of known values. If valid, it stores the data/reports.


## Add new Copilot learning conditions

In Copilot code these are called "metrics". In R code these are called "drivers". In the report template they're called "learning conditions". They all mean the same thing. The current set we have available are:

* feedback for growth
* teacher caring
* meaningful work
* Celebrating Diversity
* Student Voice
* Classroom Belonging
* Social Belonging and Belonging Uncertainty
* Trust and Fairness
* Institutional Growth Mindset
* STEM self-efficacy
* Identity Threat

Each of the following things must be created or adjusted when adding new learning conditions to Copilot.

* Add a row to the `metric` table in the `triton` database in each environment you're working in (most criticially/obviously: in production).
  - `uid` and `short_uid` should follow the existing pattern; the random portion should be from the character set /[A-Za-z0-9]/
  - `name` should be in Title Case
  - `label` should be in dash-case
  - `links` should be similar to the others and contain a URL which will be used on the survey settings page of Copilot, where users can click the title of the learning condition to learn more about it.
* Add information about this metric to the `program.metrics` field for all programs that will have this metric available. See existing programs for examples. The `uid` key should match the metric, and the `default_active` key determines if that metric starts as active for new teams on this program.
* Add a block to the current student survey, checking if the dash-case label of the row added in the `metric` table is present within the `learning_conditions` query string parameter.
* Add questions to this block, noting the question id and the pdd tag in each case.
* Add the details of all these questions to the items csv at `config/copilot_items.csv`.
  - `question_code` must match the pdd tag
  - `metric_for_reports` column is TRUE
  - `driver` contains the snake_case name of the learning condition, e.g. "teacher_caring"
  - `min_good` and `max_good` are set correctly for the size of the likert scale.
* Add the learning condition to the `config` object at the top of any applicable report templates in Neptune, e.g. `/templates/ep_report.html`, using the dash-case name.

Test all this locally first!

1. Follow each of the above steps in a local development environment.
   1. Set up a team with classrooms and rosters (of >> 5 students, or the MIN_CELL filter will bite you) in local Copilot.
   2. Trigger RServe with a jwt from your local Copilot (`?really_send=false`), using local database credentials, and activating fake qualtrics responses. See [details about ep.R in the readme][2].
   3. Check local Copilot for posted reports, and ensure they appear correct.
2. PR, and QA both the Neptune and RServe code changes.
3. Make the needed changes in the production `triton` database and deploy Neptune and RServe.
4. Run the report cron job (remember to start and stop RServe) and preview the unrelease reports to make sure they're correct.
5. Sit back and let RServe take care of the rest.


[1]: https://docs.google.com/spreadsheets/d/1VKJNCTLVBzqvGpq9QR_dHCEWA8Nqw5O50uG5IXKwMrc/edit#gid=0
[2]: https://github.com/PERTS/analysis/blob/master/rserve/readme.md#epr

### Script for modifying anonymous sample

CM used this for adding the cultural competence metric.

```r
setwd('~/Sites/analysis/common')

anon_sample_file_name <- 'qualtrics_anonymous_sample_ep_2018.csv'
anon_sample <- read.csv(anon_sample_file_name, stringsAsFactors = FALSE)

cc1_qid <- 'Q99'
cc2_qid <- 'Q100'
cc3_qid <- 'Q101'

cc1_text <- '__pdd__cc1_1__pdd__ In this class, I feel proud of who I am and my background.'
cc2_text <- '__pdd__cc2_1__pdd__ In this class, I’ve learned new things about my culture and/or community.'
cc3_text <- '__pdd__cc3_1__pdd__ In this class, I have the chance to learn about the culture of others.'

cc1_or_qid <- 'Q102'
cc2_or_qid <- 'Q103'
cc3_or_qid <- 'Q104'

cc1_or_text <- '__pdd__or_cc1_1__pdd__ Please explain how you chose your answer. You can also add anything else about your answer that you would like your teacher to know.'
cc2_or_text <- '__pdd__or_cc2_1__pdd__ Please explain how you chose your answer. You can also add anything else about your answer that you would like your teacher to know.'
cc3_or_text <- '__pdd__or_cc3_1__pdd__ Please explain how you chose your answer. You can also add anything else about your answer that you would like your teacher to know.'

anon_sample[[cc1_qid]] <- c(
  cc1_text,
  sample(c(1:7, NA), nrow(anon_sample) - 1, replace = TRUE)
)
anon_sample[[cc2_qid]] <- c(
  cc2_text,
  sample(c(1:7, NA), nrow(anon_sample) - 1, replace = TRUE)
)
anon_sample[[cc3_qid]] <- c(
  cc3_text,
  sample(c(1:7, NA), nrow(anon_sample) - 1, replace = TRUE)
)

get_text_response <- function (...) {
  paragraph_population <- c(0, 0, 0, 0, 0, 0, 0, 0, 0, 1);
  num_paragraphs <- sample(paragraph_population, 1)
  return(ifelse(
    num_paragraphs,
    stringi::stri_rand_lipsum(num_paragraphs, start_lipsum = FALSE),
    ""
  ))
}

anon_sample[[cc1_or_qid]] <- c(
  cc1_or_text,
  sapply(1:(nrow(anon_sample) - 1), get_text_response)
)
anon_sample[[cc2_or_qid]] <- c(
  cc2_or_text,
  sapply(1:(nrow(anon_sample) - 1), get_text_response)
)
anon_sample[[cc3_or_qid]] <- c(
  cc3_or_text,
  sapply(1:(nrow(anon_sample) - 1), get_text_response)
)

write.csv(anon_sample, anon_sample_file_name)
```

## Creating Database Migration Files

For more details, see the [db-migrate documentation](https://db-migrate.readthedocs.io/en/latest/Getting%20Started/usage/#creating-migrations).

To generate a new migration file for triton named "foo-bar":

```
node_modules/.bin/db-migrate create foo-bar --config=migrations/triton.json --migrations-dir=migrations/triton --env=test --sql-file
```

## Unit tests

To run tests once:

```
Rscript run_tests.R
```

To run a test watcher to re-run tests in a directory every time you save:

```
RScript -e "testthat::auto_test('modules', 'modules/testthat')"
```

You may have to modify the above if you're developing code in a differenct directory.

## End-to-end Tests

RServe includes tools to simulate database states of the three PERTS platforms it typically connects to: Triton/Copilot, Neptune, and Saturn. A typical e2e test uses JavaScript mocking functions to create data, creates MySQL databases according to each platform's schema, inserts the data into those databases, and then runs an RServe script with connections to those databases. The result is a full end-to-end test of an RServe script with arbitrary simulated data, including the ability to render generated datasets as reports.

End-to-end tests do NOT cover:

* RServe's webserver code that normally triggers the script from an HTTP request
* Network calls that send data back to the platforms.

### Workstation Setup

_Download_, _install_ and _start_ the version of MySQL specified in the [PERTS Tools
for Web Development][1] document.

(TODO better instructions)

- create `neptune-test-rserve`, `saturn-test-rserve`, and `triton-test-rserve` databases
- add `neptune`, `saturn` and `triton` users to those databases
- grant those users permissions

Example:

```sql
CREATE USER 'neptune'@'localhost' IDENTIFIED BY 'neptune';
CREATE USER 'saturn'@'localhost' IDENTIFIED BY 'saturn';
CREATE USER 'triton'@'localhost' IDENTIFIED BY 'triton';

GRANT ALL PRIVILEGES ON `neptune-test-rserve` . * TO 'neptune'@'localhost';
GRANT ALL PRIVILEGES ON `saturn-test-rserve` . * TO 'saturn'@'localhost';
GRANT ALL PRIVILEGES ON `triton-test-rserve` . * TO 'triton'@'localhost';
```

[1]: https://docs.google.com/document/d/184dsSF-esWgJ-TS_da3--UkFNb1oIur-r99X-7Xmhfg/edit#heading=h.boxrppiycym8

### Running Tests

```
npm test [<test file>]
```

You may need to specify a root password for your local MySQL server:

```
MYSQL_PASSWORD="myRootPassword" npm test
```

## What's Going On

- Tests are run on [jest](https://jestjs.io/).
- Test files must be located in the `__tests__` directory.
- Test files must have the `.(spec|test).(ts|tsx|js)` extension.
- Test files can imported from `triton` and `saturn`, with some assumptions:
  - `triton` is assumed to be located at `'../../triton/src'`
  - `saturn` is assumed to be located at `'../../saturn/hosting/src'`
  - The above is because that's what @taptapdan currently needed access to.
  - Additional directories can be added to the `moduleDirectories` property of `jest.config.js`.
  - But be careful. The order specified is the order preference that these will be imported. Any naming collisions across `moduleDirectories` might cause problems.
  - Any npm modules that are dependencies of imported modules from our other repos must be specified in the `devDependencies` of `package.json` so that they are installed with `npm install`.
- Setup & teardown of databases (`beforeEach`, `afterEach` in the jest tests) is handled by [`db-migrate`](https://github.com/db-migrate/node-db-migrate).
  - Although you can use db-migrate from the command line, we are using it as a [module](https://db-migrate.readthedocs.io/en/latest/API/programable/).
  - Currently, the migration files are located in `analysis`, but this will most likely be moved into `neptune`, `saturn` and `triton` repositories and imported here.

## TODO / Fixes

Because we're importing from saturn and saturn is importing firebase, but
analysis isn't using/running firebase, environment variables aren't being set
and so we get the following:

```
  console.warn ../../saturn/functions/node_modules/firebase-functions/lib/setup.js:53
    Warning, FIREBASE_CONFIG and GCLOUD_PROJECT environment variables are missing. Initializing firebase-admin will fail
```

## To do

* put path and body digest in jwt payload
* run scripts in separate processes, package `callr`?
