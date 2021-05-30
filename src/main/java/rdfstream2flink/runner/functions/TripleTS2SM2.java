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
import rdfstream2flink.runner.TripleTS;

//Triple to SolutionMapping - Map Function
public class TripleTS2SM2 extends ProcessWindowFunction<TripleTS, SolutionMapping, String, GlobalWindow> {

    private String subject, predicate, object = null;

    public TripleTS2SM2(String s, String p, String o){
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

    @Override
    public void process(String key, Context context, Iterable<TripleTS> in, Collector<SolutionMapping> out) throws Exception{
        int i=1;
        for (TripleTS t : in) {
            if(subject.contains("?") && !predicate.contains("?") && !object.contains("?")) {
                //System.out.println("T2SM-2: ?s - -");
                if(t.getPredicate().getURI().toString().equals(predicate) && evalObject(t.getObject())) {
                    SolutionMapping sm = new SolutionMapping();
                    sm.putMapping(subject, t.getSubject());
                    out.collect(sm);
                }
            } else if(!subject.contains("?") && predicate.contains("?") && !object.contains("?")) {
                //System.out.println("T2SM-2: - ?p -");
                if(t.getSubject().getURI().toString().equals(subject) && evalObject(t.getObject())) {
                    SolutionMapping sm = new SolutionMapping();
                    sm.putMapping(predicate, t.getPredicate());
                    out.collect(sm);
                }
            } else if(!subject.contains("?") && !predicate.contains("?") && object.contains("?")) {
                //System.out.println("T2SM-2: - - ?o");
                //System.out.println("t.getSubject()-->"+t.getSubject().getURI().toString()+" == \n\t subject-->"+subject);
                //System.out.println("\n");
                //System.out.println("t.getPredicate-->"+t.getPredicate().getURI().toString()+" == \n\t predicate-->"+predicate);
                if(t.getSubject().getURI().toString().equals(subject) && t.getPredicate().getURI().toString().equals(predicate)) {
                    SolutionMapping sm = new SolutionMapping();
                    sm.putMapping(object, t.getObject());
                    out.collect(sm);
                }
            } else if(subject.contains("?") && predicate.contains("?") && !object.contains("?")) {
                //System.out.println("T2SM-2: ?s ?p -");
                if(evalObject(t.getObject())) {
                    SolutionMapping sm = new SolutionMapping();
                    sm.putMapping(subject, t.getSubject());
                    sm.putMapping(predicate, t.getPredicate());
                    out.collect(sm);
                }
            } else if(subject.contains("?") && !predicate.contains("?") && object.contains("?")) {
                //System.out.println("T2SM-2: ?s - ?o");
                if(t.getPredicate().getURI().toString().equals(predicate)) {
                    SolutionMapping sm = new SolutionMapping();
                    sm.putMapping(subject, t.getSubject());
                    sm.putMapping(object, t.getObject());
                    out.collect(sm);
                }
            } else if(!subject.contains("?") && predicate.contains("?") && object.contains("?")) {
                //System.out.println("T2SM-2: - ?p ?o");
                if(t.getSubject().getURI().toString().equals(subject)) {
                    SolutionMapping sm = new SolutionMapping();
                    sm.putMapping(predicate, t.getPredicate());
                    sm.putMapping(object, t.getObject());
                    out.collect(sm);
                }
            } else {
                //System.out.println("T2SM-2: - - -");
                SolutionMapping sm = new SolutionMapping();
                sm.putMapping(subject, t.getSubject());
                sm.putMapping(predicate, t.getPredicate());
                sm.putMapping(object, t.getObject());
                out.collect(sm);
            }
            i++;
        }
        //System.out.println("=========> i = "+ i);
    }
}