package rdfstream2flink.runner.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

// SolutionMapping - Distinct Key Selector
public class WindowKeySelector implements KeySelector<Triple, Node> {

    @Override
    public Node getKey(Triple t) {
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
