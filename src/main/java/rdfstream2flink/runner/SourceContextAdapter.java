package rdfstream2flink.runner;

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.StreamRDFBase;
import org.apache.jena.sparql.core.Quad;

public class SourceContextAdapter extends StreamRDFBase {

	private SourceContext<TripleTS> ctx;

	public SourceContextAdapter(SourceContext<TripleTS> ctx){
		this.ctx = ctx;
	}

	@Override
	public void quad(Quad quad) {
		Long timeStamp = Long.valueOf(quad.getGraph().getURI().replace(XSDDatatype.XSDlong.getURI()+"#", ""));
		//System.out.println("TripleTS = \nSubject="+quad.getSubject()+"\nPredicate="+quad.getPredicate()+"\nObject="+quad.getObject()+"\nTimeStamp="+timeStamp);
		ctx.collect(new TripleTS(quad.getSubject(), quad.getPredicate(), quad.getObject(), timeStamp));
	}

	@Override
	public void triple(Triple t) {
		ctx.collect(new TripleTS(t));
	}

	@Override
	public void finish() {}
}
