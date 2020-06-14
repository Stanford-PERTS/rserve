#!/bin/bash
# Script to be run in CI (codeship pro), see codeship-steps.yml

if [ "$CI" = "true" ]; then
  # 2020-02-20 CAM found that ggplot2 would not install. Found this solution:
  # https://github.com/cburgmer/csscritic/issues/69#issuecomment-145784047
  # -q for quiet, -y to answer yes to all prompts (obviously the installation
  # is unattended)
  sudo apt-get -q -y install libjpeg-dev
fi

Rscript run_tests.R
