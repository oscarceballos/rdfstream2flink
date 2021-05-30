package rdfstream2flink.runner.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import rdfstream2flink.runner.TripleTS;

// SolutionMapping - Distinct Key Selector
public class WindowKSTriple implements KeySelector<Triple, String> {

    @Override
    public String getKey(Triple t) {
        /*if(subject.contains("?") && !predicate.contains("?") && !object.contains("?")) {
            //System.out.println("?s - -");
            return (t.getPredicate().toString()+","+t.getObject().toString());
        } else if(!subject.contains("?") && predicate.contains("?") && !object.contains("?")) {
            //System.out.println("- ?p -");
            return (t.getSubject().toString()+","+t.getObject().toString());
        } else if(!subject.contains("?") && !predicate.contains("?") && object.contains("?")) {
            //System.out.println("- - ?s");
            return (t.getSubject().toString()+","+t.getPredicate().toString());
        } else if(subject.contains("?") && predicate.contains("?") && !object.contains("?")) {
            //System.out.println("?s ?p -");
            return (t.getObject().toString());
        } else if(subject.contains("?") && !predicate.contains("?") && object.contains("?")) {
            //System.out.println("?s - ?o");
            return t.getPredicate().toString();
        } else if(!subject.contains("?") && predicate.contains("?") && object.contains("?")) {
            //System.out.println("- ?p ?o");
            return (t.getSubject().toString());
        }*/
        return t.getPredicate().toString();
    }
}
