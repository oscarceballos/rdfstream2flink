
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

import com.hp.hpl.jena.sparql.algebra.Op;
import org.deri.cqels.engine.OpRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rdfstream2flink.mapper.*;


public class RDFStream2Flink {

    public static void main(String[] args) throws Exception {
        Path path;

        HashMap<String, String> parameters = new HashMap<>();
        Logger logger = LoggerFactory.getLogger(RDFStream2Flink.class);

        for (String s : args) {
            parameters.put(s.split("=")[0].trim(), s.split("=")[1].trim());
        }

        if (parameters.containsKey("path")) {
            path = Paths.get(parameters.get("path"));
        } else {
            logger.warn("\nYou should to specify path query file argument.\nFor example: path_query_file/query_file.rq\n"+
                    "\nExecuting sample with default SPARQL query saved in << examples >> directory");
            path = Paths.get("./examples/query.rq");
        }

        if (parameters.containsKey("typeTime")) SolutionMapping.setTypeTime(parameters.get("typeTime"));

        if (parameters.containsKey("typeWindow")) SolutionMapping.setTypeWindow(parameters.get("typeWindow"));

        LoadQueryFile queryFile = new LoadQueryFile(path.toString());

        String queryString;
        if(parameters.containsKey("host") && parameters.containsKey("port"))
            queryString = queryFile.maskUris(parameters.get("host"), Integer.parseInt(parameters.get("port")));
        else queryString = queryFile.loadSQFile();

        Query2LogicalQueryPlan query2LQP = new Query2LogicalQueryPlan(queryString);
        Op logicalQueryPlan = query2LQP.translationSQ2LQP();

        LogicalQueryPlan2FlinkProgram lQP2FlinkProgram = new LogicalQueryPlan2FlinkProgram(logicalQueryPlan, path);
        String flinkProgram = lQP2FlinkProgram.logicalQueryPlan2FlinkProgram();

        CreateFlinkProgram javaFlinkProgram = new CreateFlinkProgram(flinkProgram, path);
        javaFlinkProgram.createFlinkProgram();
    }
}
