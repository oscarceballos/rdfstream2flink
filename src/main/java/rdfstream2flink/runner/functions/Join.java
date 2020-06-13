package rdfstream2flink.runner.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.util.Collector;

//SolutionMapping - Flat Join Function
public class Join implements FlatJoinFunction<SolutionMapping, SolutionMapping, SolutionMapping> {

    @Override
    public void join(SolutionMapping left, SolutionMapping right, Collector<SolutionMapping> out) throws Exception {
        System.out.println("left="+left.toString()+"\n \t right="+right.toString());
        out.collect(left.join(right));
    }
}
