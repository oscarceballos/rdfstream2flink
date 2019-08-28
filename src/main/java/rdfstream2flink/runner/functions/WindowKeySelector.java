package rdfstream2flink.runner.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.jena.graph.Node;
import rdfstream2flink.runner.TripleTS;

// SolutionMapping - Distinct Key Selector
public class WindowKeySelector implements KeySelector<TripleTS, Node> {

    @Override
    public Node getKey(TripleTS t) {
        if(t.getSubject().isVariable() && !t.getPredicate().isVariable() && !t.getObject().isVariable()) {
            return t.getPredicate();
        } else if(!t.getSubject().isVariable() && t.getPredicate().isVariable() && !t.getObject().isVariable()) {
            return t.getSubject();
        } else if(!t.getSubject().isVariable() && !t.getPredicate().isVariable() && t.getObject().isVariable()) {
            return t.getPredicate();
        } else if(!t.getSubject().isVariable() && t.getPredicate().isVariable() && t.getObject().isVariable()) {
            return t.getSubject();
        } else if(t.getSubject().isVariable() && !t.getPredicate().isVariable() && t.getObject().isVariable()) {
            return t.getPredicate();
        } else if(t.getSubject().isVariable() && t.getPredicate().isVariable() && !t.getObject().isVariable()) {
            return t.getObject();
        }
        return t.getPredicate();
    }
}
