#!/bin/sh
NEPTUNE_DB_NAME=neptune-test-rserve \
NEPTUNE_DB_USER=neptune \
NEPTUNE_DB_PASS=neptune \
SATURN_DB_NAME=saturn-test-rserve \
SATURN_DB_USER=saturn \
SATURN_DB_PASS=saturn \
TRITON_DB_NAME=triton-test-rserve \
TRITON_DB_USER=triton \
TRITON_DB_PASS=triton \
jest --runInBand $1
