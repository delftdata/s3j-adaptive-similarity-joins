#!/bin/bash
git clone https://github.com/jorgeSia/flink-release-1.12.1 ./flink
cd ./flink
mvn clean install -DskipTests -Dfast
cd flink-dist
mvn clean install -DskipTests -Dfast
cd ..
cd ..
rm -rf ./flink