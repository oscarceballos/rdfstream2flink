# rdfstream2flink

An approach for transforming a given CQELS-QL query into an Apache Flink program for querying massive RDF stream data

Initially to launch the main class of this repository you will install the Maven installation manager (```mvn```) and run the file ```libs.sh```, to install the local libraries necessary to run the class ```RDFStream2Flink```.

## Generate jars for
* With libraries
```
mvn clean compile assembly:single
```
* without libraries
```
mvn package
```

## Parameters
* path: path to query [path/query.txt]
* host: if you pass this parameter, the urls established in streams within the query are changed to the use of that value.
* port: It is mandatory in case of using the previous parameter

### Example for run jar
```
$ cd ./target
$ java -cp rdfstream2flink-1.0-SNAPSHOT-jar-with-dependencies.jar RDFStream2Flink \
    path=../example/Q1.txt host=localhost port=5555
```