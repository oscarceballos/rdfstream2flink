package rdfstream2flink.mapper;

import cqels.engine.ContinuousSelect;
import cqels.engine.ExecContext;
import cqels.engine.OpRouter;

public class Query2LogicalQueryPlan {

    private String queryString;

    public Query2LogicalQueryPlan(String queryString){
        this.queryString = queryString;
    }

    public OpRouter translationSQ2LQP() {

        //Set<Thread> threads = Thread.getAllStackTraces().keySet();
        //threads.forEach(k -> {});
        ExecContext context = new ExecContext("src/main/resources/cqelshome", true);
        ContinuousSelect cs = context.registerSelect(this.queryString);
        OpRouter op = cs.sub();

        return op;
    }
}
