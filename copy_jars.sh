#!/bin/bash

cp target/online_partitioning_for_ssj-1.0.jar /Users/gsiachamis/Dropbox/"My Mac (Georgios’s MacBook Pro)"/Documents/GitHub/ssj-jars/online_partitioning_for_ssj-1.0.jar
cp target/online_partitioning_for_ssj-1.0.jar src/main/java/StreamGeneratorComponent/libs/org/example/online_partitioning_for_ssj/1.0/online_partitioning_for_ssj-1.0.jar
cp target/online_partitioning_for_ssj-1.0.jar /Users/gsiachamis/Dropbox/"My Mac (Georgios’s MacBook Pro)"/Documents/GitHub/SSJ-Coordinator/jars/online_partitioning_for_ssj-1.0.jar
cp src/main/java/StreamGeneratorComponent/target/StreamGenerator-1.0.jar /Users/gsiachamis/Dropbox/"My Mac (Georgios’s MacBook Pro)"/Documents/GitHub/ssj-jars/StreamGenerator-1.0.jar
cp src/main/java/StreamGeneratorComponent/target/StreamGenerator-1.0.jar /Users/gsiachamis/Dropbox/"My Mac (Georgios’s MacBook Pro)"/Documents/GitHub/SSJ-Coordinator/jars/StreamGenerator-1.0.jar

docker build -t gsiachamis/ssj-coordinator /Users/gsiachamis/Dropbox/"My Mac (Georgios’s MacBook Pro)"/Documents/GitHub/SSJ-Coordinator/.