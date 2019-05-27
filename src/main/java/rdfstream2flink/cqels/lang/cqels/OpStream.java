package rdfstream2flink.cqels.lang.cqels;

import java.util.Arrays;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpGraph;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadPattern;
import com.hp.hpl.jena.sparql.algebra.op.OpTriple;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.Var;
import rdfstream2flink.cqels.engine.Window;

public class OpStream extends OpQuadPattern {
	Window window;
	public OpStream(Node node,BasicPattern pattern, Window window) {
		super(node, pattern);
		this.window=window;
	}
	
	public OpStream(Node node, Triple triple, Window window) {
		this(node, BasicPattern.wrap(Arrays.asList(triple)), window);
	}
	public Window getWindow() {
		return window;
	}

}
