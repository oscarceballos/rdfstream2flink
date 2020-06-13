package rdfstream2flink.runner.functions;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.jena.graph.Node;
import rdfstream2flink.runner.TripleTS;

public class Triple2SolutionMapping3 implements WindowFunction<TripleTS, SolutionMapping, String, TimeWindow> {

    private String subject, predicate, object;

    public Triple2SolutionMapping3(String s, String p, String o){
        this.subject = s;
        this.predicate = p;
        this.object = o;
    }

    public boolean evalObject(Node node){
        Boolean flag = false;
        if(node.isLiteral()) {
            if (node.getLiteralValue().toString().equals(object)){
                flag = true;
            }
        } else if (node.isURI()) {
            if (node.getURI().toString().equals(object)) {
                flag = true;
            }
        }
        return flag;
    }

    public void apply(String key, TimeWindow w, Iterable<TripleTS> in, Collector<SolutionMapping> out) {
        for (TripleTS t : in) {
            //System.out.println("TripleTS = \nSubject="+t.getSubject()+"\nPredicate="+t.getPredicate()+"\nObject="+t.getObject()+"\nTimeStamp="+t.getTimeStamp());
            if(subject.contains("?") && !predicate.contains("?") && !object.contains("?")) {
                if(t.getPredicate().getURI().toString().equals(predicate) && evalObject(t.getObject())) {
                    System.out.println("t.getPredicate()="+t.getPredicate().toString()+"\n\t predicate="+predicate);
                    System.out.println("t.getObject()="+t.getObject().toString()+"\n\t object="+object);
                    SolutionMapping sm = new SolutionMapping();
                    sm.putMapping(subject, t.getSubject());
                    out.collect(sm);
                }
            } else if(!subject.contains("?") && predicate.contains("?") && !object.contains("?")) {
                if(t.getSubject().getURI().toString().equals(subject) && evalObject(t.getObject())) {
                    SolutionMapping sm = new SolutionMapping();
                    sm.putMapping(predicate, t.getPredicate());
                    out.collect(sm);
                }
            } else if(!subject.contains("?") && !predicate.contains("?") && object.contains("?")) {
                if(t.getSubject().getURI().toString().equals(subject) && t.getPredicate().getURI().toString().equals(predicate)) {
                    System.out.println("t.getSubject()="+t.getSubject().toString()+"\n\t subject="+subject);
                    System.out.println("t.getPredicate()="+t.getPredicate().toString()+"\n\t predicate="+predicate);
                    SolutionMapping sm = new SolutionMapping();
                    sm.putMapping(object, t.getObject());
                    out.collect(sm);
                }
            } else if(!subject.contains("?") && predicate.contains("?") && object.contains("?")) {
                if(t.getSubject().getURI().toString().equals(subject)) {
                    SolutionMapping sm = new SolutionMapping();
                    sm.putMapping(predicate, t.getPredicate());
                    sm.putMapping(object, t.getObject());
                    out.collect(sm);
                }
            } else if(subject.contains("?") && !predicate.contains("?") && object.contains("?")) {
                if(t.getPredicate().getURI().toString().equals(predicate)) {
                    System.out.println("t.getPredicate()="+t.getPredicate().toString()+"\n\t predicate="+predicate);
                    SolutionMapping sm = new SolutionMapping();
                    sm.putMapping(subject, t.getSubject());
                    sm.putMapping(object, t.getObject());
                    out.collect(sm);
                }
            } else if(subject.contains("?") && predicate.contains("?") && !object.contains("?")) {
                if(evalObject(t.getObject())) {
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