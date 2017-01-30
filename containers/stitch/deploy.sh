#!/bin/bash

set -e

if [ "$#" -ne 1 ]; then
    echo Usage: $0 VERSION
    exit 1
fi

name=stitchdata/stream-hubspot
version=$1
repo=218546966473.dkr.ecr.us-east-1.amazonaws.com
docker build -t $name:$version -f Dockerfile ../..
docker tag $name:$version $repo/$name:$version
sudo docker push $repo/$name:$version
