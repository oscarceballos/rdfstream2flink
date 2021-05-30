package rdfstream2flink.runner;

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.StreamRDFBase;
import org.apache.jena.sparql.core.Quad;

public class SourceContextAdapterTriple extends StreamRDFBase {

	private SourceContext<Triple> ctx;

	public SourceContextAdapterTriple(SourceContext<Triple> ctx){
		this.ctx = ctx;
	}

	@Override
	public void quad(Quad quad) {
		ctx.collect(new Triple(quad.getSubject(), quad.getPredicate(), quad.getObject()));
	}

	@Override
	public void triple(Triple t) {
		ctx.collect(t);
	}

	@Override
	public void finish() {}
}
