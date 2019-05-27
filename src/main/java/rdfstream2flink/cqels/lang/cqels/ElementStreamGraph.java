package rdfstream2flink.cqels.lang.cqels;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.syntax.ElementNamedGraph;
import com.hp.hpl.jena.sparql.syntax.ElementVisitor;
import com.hp.hpl.jena.sparql.util.NodeIsomorphismMap;
import rdfstream2flink.cqels.engine.Window;

public class ElementStreamGraph extends ElementNamedGraph {
	private Window window;
	public ElementStreamGraph(Node n, Window w, Element el) {
		super(n, el);
		window = w;
	}
	
	public Window getWindow() {	return window; }
}
