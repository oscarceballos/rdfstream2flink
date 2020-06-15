package rdfstream2flink.runner.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.jena.graph.Node;
import rdfstream2flink.runner.TripleTS;

// SolutionMapping - Distinct Key Selector
public class WindowKeySelector implements KeySelector<TripleTS, String> {

    String subject, predicate, object = null;

    public WindowKeySelector(String s, String p, String o){
        this.subject = s;
        this.predicate = p;
        this.object = o;
    }

    @Override
    public String getKey(TripleTS t) {
        if(subject.contains("?") && !predicate.contains("?") && !object.contains("?")) {
            return (t.getPredicate().toString()+","+t.getObject().toString());
        } else if(!subject.contains("?") && predicate.contains("?") && !object.contains("?")) {
            return (t.getSubject().toString()+","+t.getObject().toString());
        } else if(!subject.contains("?") && !predicate.contains("?") && object.contains("?")) {
            return (t.getSubject().toString()+","+t.getPredicate().toString());
        } else if(!subject.contains("?") && predicate.contains("?") && object.contains("?")) {
            return t.getSubject().toString();
        } else if(subject.contains("?") && !predicate.contains("?") && object.contains("?")) {
            return t.getPredicate().toString();
        } else if(subject.contains("?") && predicate.contains("?") && !object.contains("?")) {
            return t.getObject().toString();
        }
        return (t.getSubject()+","+t.getPredicate().toString()+","+t.getObject().toString());
    }
}
