#!/bin/bash

docker build -t streaming_similarity_joins .
docker run --name streaming_similarity_joins streaming_similarity_joins
docker cp streaming_similarity_joins:/test/join_output.out .