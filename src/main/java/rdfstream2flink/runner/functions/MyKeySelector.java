package rdfstream2flink.runner.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

// SolutionMapping - Distinct Key Selector
public class MyKeySelector implements KeySelector<Triple, Node> {

    @Override
    public Node getKey(Triple t) {
        return t.getPredicate();
    }
}
