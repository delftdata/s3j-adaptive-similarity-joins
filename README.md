# onlinePartitioningForSsj

# Set up modified Flink

- Get the jar from here: https://surfdrive.surf.nl/files/index.php/s/qGBEpsokMOrabVr
- Put the jar in the root directory of the project and run the following command:

```
mvn install:install-file -DlocalRepositoryPath=libs -DcreateChecksum=true -Dpackaging=jar -Dfile=flink-dist_2.11-1.12.1.jar -DgroupId=org.apache.flink -DartifactId=flink-dist -Dversion=1.12.1
```