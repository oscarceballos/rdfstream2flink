package rdfstream2flink.runner.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

//Triple to SolutionMapping - Map Function
public class Triple2SolutionMapping2 extends ProcessWindowFunction<Triple, SolutionMapping, Node, GlobalWindow> {

    private String subject, predicate, object = null;

    public Triple2SolutionMapping2(String s, String p, String o){
        this.subject = s;
        this.predicate = p;
        this.object = o;
    }

    @Override
    public void process(Node key, Context context, Iterable<Triple> in, Collector<SolutionMapping> out) throws Exception{
        int i=1;
        for (Triple t : in) {
            //System.out.println("t.subject--->" +  t.getSubject().toString() + " --- subject--->" + subject);
            //System.out.println("t.object--->" +  t.getObject().toString() + " --- object--->" + object);
            //System.out.println("=============================================================================");
            if(subject.contains("?") && !predicate.contains("?") && !object.contains("?")) {
                if(t.getPredicate().toString().equals(predicate) && t.getObject().toString().equals(object)) {
                    SolutionMapping sm = new SolutionMapping();
                    sm.putMapping(subject, t.getSubject());
                    out.collect(sm);
                }
            } else if(!subject.contains("?") && predicate.contains("?") && !object.contains("?")) {
                if(t.getSubject().toString().equals(subject) && t.getObject().toString().equals(object)) {
                    SolutionMapping sm = new SolutionMapping();
                    sm.putMapping(predicate, t.getPredicate());
                    out.collect(sm);
                }
            } else if(!subject.contains("?") && !predicate.contains("?") && object.contains("?")) {
                if(t.getSubject().toString().equals(subject) && t.getPredicate().toString().equals(predicate)) {
                    SolutionMapping sm = new SolutionMapping();
                    sm.putMapping(object, t.getObject());
                    out.collect(sm);
                }
            } else if(!subject.contains("?") && predicate.contains("?") && object.contains("?")) {
                if(t.getSubject().toString().equals(subject)) {
                    SolutionMapping sm = new SolutionMapping();
                    sm.putMapping(predicate, t.getPredicate());
                    sm.putMapping(object, t.getObject());
                    out.collect(sm);
                }
            } else if(subject.contains("?") && !predicate.contains("?") && object.contains("?")) {
                if(t.getPredicate().toString().equals(predicate)) {
                    SolutionMapping sm = new SolutionMapping();
                    sm.putMapping(subject, t.getSubject());
                    sm.putMapping(object, t.getObject());
                    out.collect(sm);
                    //System.out.println("****** Window: "+context.window() + " i: "+i);
                }
            } else if(subject.contains("?") && predicate.contains("?") && !object.contains("?")) {
                if(t.getObject().toString().equals(object)) {
                    SolutionMapping sm = new SolutionMapping();
                    sm.putMapping(subject, t.getSubject());
                    sm.putMapping(predicate, t.getPredicate());
                    out.collect(sm);
                }
            } else {
                SolutionMapping sm = new SolutionMapping();
                sm.putMapping(subject, t.getSubject());
                sm.putMapping(predicate, t.getPredicate());
                sm.putMapping(object, t.getObject());
                out.collect(sm);
            }
            i++;
        }
    }
}