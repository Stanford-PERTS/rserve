#!/bin/sh

# requries command line argument with version name

gcloud app deploy app.yaml \
  --project=rserveplatform \
  --version=$1 \
  --no-promote
