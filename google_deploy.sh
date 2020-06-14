#!/bin/bash

# Authenticate with the Google Services
codeship_google authenticate

# switch to the directory containing your app.yml (or similar) configuration file
# note that your repository is mounted as a volume to the /deploy directory
cd /deploy/

# Now we can use gcloud commands; see the following file.
# The --quiet option prevents waiting for human confirmation.
gcloud app deploy app.yaml \
  --project=rserveplatform \
  --version=production \
  --no-promote \
  --quiet
