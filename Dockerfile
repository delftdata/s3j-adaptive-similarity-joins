FROM ubuntu:20.04
ENV TESTFILE=1K_2D_Array_Stream_v2.txt
WORKDIR /test
RUN apt update && apt -y install openjdk-11-jdk && java -version
COPY ./target/*.jar .
COPY ./src/test/resources/*.txt .
CMD java -cp online_partitioning_for_ssj-1.0.jar onlinePartitioningForSsj ${TESTFILE} && cat ./join_results/* > join_output.out
