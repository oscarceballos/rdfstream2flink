package rdfstream2flink.mapper;

import com.hp.hpl.jena.sparql.algebra.Op;

import java.nio.file.Path;

public class LogicalQueryPlan2FlinkProgram {

    private Op logicalQueryPlan;
    private String className;

    public LogicalQueryPlan2FlinkProgram(Op logicalQueryPlan, Path path){
        this.logicalQueryPlan = logicalQueryPlan;
        this.className = path.getFileName().toString();
        this.className = this.className.substring(0, this.className.indexOf('.'));
        this.className = this.className.toLowerCase();
        this.className = this.className.substring(0, 1).toUpperCase() + this.className.substring(1, this.className.length());
    }

    public String logicalQueryPlan2FlinkProgram() {
        String flinkProgram = "";

        flinkProgram += "package rdfstream2flink.out;\n\n" +
                "import org.apache.flink.api.common.typeinfo.TypeInformation;\n" +
                "import org.apache.flink.api.java.utils.ParameterTool;\n" +
                "import org.apache.flink.core.fs.FileSystem;\n" +
                "import org.apache.flink.streaming.api.datastream.DataStream;\n" +
                "import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;\n" +
                "import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;\n" +
                "import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;\n" +
                "import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;\n" +
                "import org.apache.flink.streaming.api.windowing.assigners.*;\n" +
                "import org.apache.flink.streaming.api.windowing.time.Time;\n" +
                "import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;\n" +
                "import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;\n" +
                "import org.apache.flink.streaming.api.windowing.windows.TimeWindow;\n" +
                "import org.apache.jena.graph.Triple;\n" +
                "import rdfstream2flink.runner.*;\n" +
                "import rdfstream2flink.runner.functions.*;\n\n" +
                "import java.util.concurrent.TimeUnit;\n\n" +

                "\npublic class "+className+" {\n" +
                "\tpublic static void main(String[] args) throws Exception {\n\n" +
                "\t\tfinal ParameterTool params = ParameterTool.fromArgs(args);\n\n" +
                "\t\tif (!params.has(\"output\")) {\n" +
                "\t\t\tSystem.out.println(\"Use --output to specify output path.\");\n" +
                "\t\t}\n\n"+
                "\t\t//************ Environment (DataStream) and Stream (RDF Stream) ************\n" +
                "\t\tfinal StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();\n\n";

        logicalQueryPlan.visit(new ConvertLQP2FlinkProgram());

        flinkProgram += ConvertLQP2FlinkProgram.getFlinkProgram();

        flinkProgram += "\t\t//************ Sink  ************\n" +
                "\t\tsm"+(SolutionMapping.getIndiceSM()-1) +
                ".writeAsText(params.get(\"output\")+\""+className+"-Flink-Result\", FileSystem.WriteMode.OVERWRITE)\n" +
                "\t\t\t.setParallelism(1);\n\n"+
                //".print();\n\n" +
                "\t\tenv.execute(\"CQELS-QL to Flink Programan - DataStream API\");\n";

        flinkProgram += "\t}\n}";

        return flinkProgram;
    }
}
