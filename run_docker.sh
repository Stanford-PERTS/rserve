#!/bin/bash

# port defaults to 8080
if [ ! $1 ]
then
  set -- "8080"
fi

echo "Docker container listening to port $1"

docker build -t rserve . && docker run --rm -p $1:8080 --name rserve-01 rserve
