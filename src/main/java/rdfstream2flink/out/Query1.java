package rdfstream2flink.out;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.jena.graph.Triple;
import rdfstream2flink.runner.*;
import rdfstream2flink.runner.functions.*;

public class Query1 {
	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		if (!params.has("server") && !params.has("port")) {
			System.out.println("Use --server to specify server and use --port to specify port number.");
		}

		//************ Environment (DataStream) and Stream (RDF Stream) ************
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Triple> rdfStream = LoadRDFStream.fromSocket(env, params.get("server"), Integer.parseInt(params.get("port")));

		//************ Applying Transformations ************
		DataStream<SolutionMapping> sm1 = rdfStream
			.keyBy(new MyKeySelector())
			.countWindow(100)
			.process(new Triple2SolutionMapping2("?product", "http://www.w3.org/2000/01/rdf-schema#label", "?label"));

		DataStream<SolutionMapping> sm2 = sm1
			.map(new Project(new String[]{"?product", "?label"}));

		//************ Sink  ************
		sm2.writeAsText(params.get("output")+"Query1-Flink-Result", FileSystem.WriteMode.OVERWRITE)
			.setParallelism(1);

		env.execute("CQELS-QL to Flink Programan - DataStream API");
	}
}