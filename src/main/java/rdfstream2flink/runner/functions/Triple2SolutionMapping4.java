package rdfstream2flink.runner.functions;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

public class Triple2SolutionMapping4 implements WindowFunction<Triple, SolutionMapping, Node, TimeWindow> {

    private String subject, predicate, object;

    public Triple2SolutionMapping4(String s, String p, String o){
        this.subject = s;
        this.predicate = p;
        this.object = o;
    }

    public void apply(Node key, TimeWindow w, Iterable<Triple> in, Collector<SolutionMapping> out) {
        for (Triple t : in) {
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
                    //System.out.println("sm--> " + sm.getMapping().toString());
                    System.out.println("opbject--> "+object+" w.start--> "+w.getStart()+" w.end--> "+w.getEnd());
                    out.collect(sm);
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
        }
    }
}