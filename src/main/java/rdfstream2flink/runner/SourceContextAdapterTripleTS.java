package rdfstream2flink.runner;

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.StreamRDFBase;
import org.apache.jena.sparql.core.Quad;

public class SourceContextAdapterTripleTS extends StreamRDFBase {

	private SourceContext<TripleTS> ctx;

	public SourceContextAdapterTripleTS(SourceContext<TripleTS> ctx){
		this.ctx = ctx;
	}

	@Override
	public void quad(Quad quad) {
		Long timeStamp = Long.valueOf(quad.getGraph().getURI().replace(XSDDatatype.XSDlong.getURI()+"#", ""));
        //System.out.println("=========> timeStamp="+timeStamp);
		ctx.collect(new TripleTS(quad.getSubject(), quad.getPredicate(), quad.getObject(), timeStamp));
	}

	@Override
	public void triple(Triple t) {
		ctx.collect(new TripleTS(t));
	}

	@Override
	public void finish() {}
}
