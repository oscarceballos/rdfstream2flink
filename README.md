## RDFStream2Flink library

An approach for transforming a given CQELS-QL query into an Apache Flink program for querying massive RDF stream data. The RDFStream2Flink library is composed of two modules, called: Mapper and Runner, as shown in Figure 1.

![Image text](/examples/rdfstream2flink.png)

The RDFStream2Flink library, through a Mapper module, receives a CQELS-QL query and transforms it into a Logical Query Plan based on CQELS-QL Algebra. This Logical Query Plan is translated to a Flink program based on DataStream API transformations. The library generates a Java program compiled by the Maven command into a command-line terminal generating a .jar file. Then, the Flink program is executed by the Runner module. It is necessary to pass the RDF streams data in an N-Triple format serialization, i.e., each stream element is to be in the form (<s, p, o>, t) where <s, p, o> is a triple and t is a timestamp. Finally, after the process of the Flink program, the library returns a result. The Mapper and Runner modules and their submodules will explain in more detail in the following section.

## Mapper module
The Mapper module transforms a declarative CQELS-QL query into a DataStream program in Apache Flink, and it is composed of three submodules:

### Load CQELS-QL query file
This submodule loads the declarative CQELS-QL query from a file with a .txt extension. The following listing shows an example of a CQELS-QL query that retrieves the traffic congestion level captured by the TrafficData182955 sensor. CQELS-QL is the query language for the CQELS engine. According to Le-Phuoc et al. [2011], CQELS-QL takes the best of previous proposals such as Streaming SPARQL [Bolles et al., 2008] and C-SPARQL [Barbieri et al., 2009, 2010].

```
PREFIX ses: <http://www.insight−centre.org/dataset/SampleEventService#>
PREFIX ssn : <http :// purl . oclc . org/NET/ssnx/ssn#>
PREFIX sao : <http :// purl . oclc . org/NET/sao/>

SELECT ?obId1 ?v1
WHERE {
    STREAM ses : TrafficData182955 [RANGE 10m]
    {
        ?obId1 ssn:observedProperty ses:CongestionLevel . 
        ?obId1 sao:hasValue ?v1 .
    }
}
```

### Tranlate query to a Logical Query Plan
This submodule translates the CQELS-QL query into a Logical Query Plan (LQP) expressed with CQELS-QL Algebra op- erators. The LQP is represented with an RDF-centric syntax provided by Jena, which is called SPARQL Syntax Expression (SSE) [Apache-Jena, 2011]. The following listing shows an LQP of the CQELS-QL query example.

```
(project (?obId1 ?v1 ?obId2 ?v2)
    (join
        (quadpattern (quad <localhost:9995> ?obId1 ssn:observedProperty ses:CongestionLevel))
        (quadpattern (quad <localhost:9995> ?obId1 sao:hasValue ?v1))
     )
)
```

### Convert Logical Query Plan into DataStream Flink program
In general, each triple pattern within a quadpattern is encoded as a combination of keyBy, window and process trans- formations; the join operator is encoded as a where, equalTo, window, and apply transformation; whereas the project operator is expressed as a map transformation. The following listing shows the Java Flink program after transforming the CQELS- QL query example.

```
...
public class Query {
    public static void main( String [] args ) throws Exception {
        //************ StreamExecutionEnvironment and Time Characteristic ************
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        
        //************ Loading RDF stream from localhost:9995 ************
        DataStream<TripleTS> rdfStream = LoadRDFStream.fromSocketTripleTS(env, "localhost", 9995)
            .assignTimestampsAndWatermarks(new PeriodicAssigner());
            
        //************ Applying Transformations to rdfStream ************
        DataStream<SolutionMapping> sm1 = rdfStream
            .keyBy(new WindowKeySelectorTripleTS())
            .window(TumblingEventTimeWindows.of(Time.minutes(10))).process(new TripleTS2SolutionMapping("?obId1",
                    "http://purl.oclc.org/NET/ssnx/ssn#observedProperty",
                    "http://www.insight−centre.org/dataset/SampleEventService#CongestionLevel"));
         
         DataStream<SolutionMapping> sm2 = rdfStream
            .keyBy(new WindowKeySelectorTripleTS())
            .window(TumblingEventTimeWindows.of(Time.minutes(10))).process(new TripleTS2SolutionMapping("?obId1",
                    "http://purl.oclc.org/NET/sao/hasValue", "?v1"));
                    
         DataStream<SolutionMapping> sm3 = sm1.join(sm2)
            .where(new JoinKeySelector(new String[]{"?obId1"}))
            .equalTo(new JoinKeySelector(new String[]{"?obId1"}))
            .window(TumblingEventTimeWindows.of(Time.minutes(10))).apply(new Join());
            
         DataStream<SolutionMapping> sm4 = sm3
            .map(new Project(new String[]{"?obId1", "?v1", "?obId2", "?v2"}));
            
         //************ Sink ************
         sm4.writeAsText(params.get("output")+"Result", FileSystem.WriteMode.OVERWRITE)
            .setParallelism (1);
         
         env.execute("CQELS−QL to Flink Program − DataStream API");
    }
}
```

## Runner module
This module allows executing a Flink program (as a jar file) on an Apache Flink stand-alone or local cluster mode. This module is composed of three submodules: i) Load Flink Program that loads a Flink program with .jar extension; ii) Load RDF stream that loads an RDF stream in N-Triples format serialization; and ii) Functions that contains several Java classes that allows to solve the transformations within the Flink program.


## Process to launch RDFStream2Flink library
Initially to launch the main class of this repository you will install the Maven installation manager (```mvn```) and run the file ```libs.sh```, to install the local libraries necessary to run the class ```RDFStream2Flink```.

### Generate jars for
* With libraries
```
mvn clean compile assembly:single
```
* without libraries
```
mvn package
```

### Parameters
* path: path to query [path/query.txt]
* host: if you pass this parameter, the urls established in streams within the query are changed to the use of that value.
* port: It is mandatory in case of using the previous parameter

### Example for run jar
```
$ cd ./target
$ java -cp rdfstream2flink-1.0-SNAPSHOT-jar-with-dependencies.jar RDFStream2Flink \
    path=../example/Q1.txt host=localhost port=5555
```


## References
Le-Phuoc, D., Dao-Tran, M., Parreira, J. X., and Hauswirth, M. (2011). A native and adaptive approach for unified processing of linked streams and linked data. In Proceedings of the 10th International Conference on The Semantic Web - Volume Part I, ISWC’11, pages 370–388, Berlin, Heidelberg. Springer-Verlag

Bolles, A., Grawunder, M., and Jacobi, J. (2008). Streaming sparql extending sparql to process data streams. In Proceed- ings of the 5th European Semantic Web Conference on The Semantic Web: Research and Applications, ESWC’08, page 448?462, Berlin, Heidelberg. Springer-Verlag

Barbieri, D. F., Braga, D., Ceri, S., Della Valle, E., and Grossniklaus, M. (2009). C-sparql: Sparql for continuous querying. In Proceedings of the 18th International Conference on World Wide Web, WWW ’09, page 1061?1062, New York, NY, USA. Association for Computing Machinery

Barbieri, D. F., Braga, D., Ceri, S., and Grossniklaus, M. (2010). An execution environment for c-sparql queries. In Pro- ceedings of the 13th International Conference on Extending Database Technology, EDBT ’10, page 441?452, New York, NY, USA. Association for Computing Machinery.

Apache-Jena (2011). Sparql syntax expression. https://jena.apache.org/documentation/notes/sse.html. Apache Software Fundation
