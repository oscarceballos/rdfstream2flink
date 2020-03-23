# rdfstream2flink
An approach for transforming a given CQELS-QL query into an Apache Flink program for querying massive RDF stream data

## Parameters
* path: path to query [path/query.txt]
* host: if you pass this parameter, the urls established in streams within the query are changed to the use of that value.
* port: It is mandatory in case of using the previous parameter