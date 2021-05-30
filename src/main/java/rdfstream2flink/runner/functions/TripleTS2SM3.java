package rdfstream2flink.runner.functions;

//import com.hp.hpl.jena.graph.Node;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.jena.graph.Node;
import rdfstream2flink.runner.TripleTS;

public class TripleTS2SM3 extends ProcessWindowFunction<TripleTS, SolutionMapping, String, TimeWindow> {

    private String subject, predicate, object;

    public TripleTS2SM3(String s, String p, String o) {
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

    public void process(String key, Context context, Iterable<TripleTS> in, Collector<SolutionMapping> out) {
        int i=0;
        for (TripleTS t : in) {
            if(subject.contains("?") && !predicate.contains("?") && !object.contains("?")) {
                if(t.getPredicate().getURI().toString().equals(predicate) && evalObject(t.getObject())) {
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
                    SolutionMapping sm = new SolutionMapping();
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
            } else if(subject.contains("?") && !predicate.contains("?") && object.contains("?")) {
                if(t.getPredicate().getURI().toString().equals(predicate)) {
                    SolutionMapping sm = new SolutionMapping();
                    sm.putMapping(subject, t.getSubject());
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
            }  else {
                SolutionMapping sm = new SolutionMapping();
                sm.putMapping(subject, t.getSubject());
                sm.putMapping(predicate, t.getPredicate());
                sm.putMapping(object, t.getObject());
                out.collect(sm);
            }
            i++;
        }
        //System.out.println("Window-start(): "+context.window().getStart()+"\t---\t"+" Window-end(): "+context.window().getEnd()+"\t--\tTotal: "+i);
    }
}