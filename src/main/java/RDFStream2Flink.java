
import java.nio.file.Path;
import java.nio.file.Paths;

import com.hp.hpl.jena.sparql.algebra.Op;
import org.deri.cqels.engine.OpRouter;
import rdfstream2flink.mapper.CreateFlinkProgram;
import rdfstream2flink.mapper.LoadQueryFile;
import rdfstream2flink.mapper.LogicalQueryPlan2FlinkProgram;
import rdfstream2flink.mapper.Query2LogicalQueryPlan;


public class RDFStream2Flink {

    public static void main(String[] args) {
        Path path;

        if (args != null && args.length == 1) {
            path = Paths.get(args[0]);
        } else {
            System.out.println("\nYou should to specify path query file argument.\nFor example: path_query_file/query_file.rq\n"+
                    "\nExecuting sample with default SPARQL query saved in << examples >> directory");
            path = Paths.get("./examples/query.rq");
        }

        LoadQueryFile queryFile = new LoadQueryFile(path.toString());
        String queryString = queryFile.loadSQFile();

        System.out.print(queryString);

        Query2LogicalQueryPlan query2LQP = new Query2LogicalQueryPlan(queryString);
        Op logicalQueryPlan = query2LQP.translationSQ2LQP();
        System.out.println(logicalQueryPlan);

        LogicalQueryPlan2FlinkProgram lQP2FlinkProgram = new LogicalQueryPlan2FlinkProgram(logicalQueryPlan, path);
        String flinkProgram = lQP2FlinkProgram.logicalQueryPlan2FlinkProgram();

        System.out.println(flinkProgram);

        //CreateFlinkProgram javaFlinkProgram = new CreateFlinkProgram(flinkProgram, path);
        //javaFlinkProgram.createFlinkProgram();
    }
}
