package rdfstream2flink.mapper;


import com.hp.hpl.jena.sparql.algebra.Op;
import org.deri.cqels.engine.ContinuousSelect;
import org.deri.cqels.engine.ExecContext;

public class Query2LogicalQueryPlan {

    private String queryString;

    public Query2LogicalQueryPlan(String queryString){
        this.queryString = queryString;
    }

    public Op translationSQ2LQP() {

        ExecContext context = new ExecContext("src/main/resources/cqelshome", true);
        ContinuousSelect cs = context.registerSelect(this.queryString);
        Op op = cs.getOp();

        return op;
    }
}
