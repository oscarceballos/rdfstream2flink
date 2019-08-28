package rdfstream2flink.runner;

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.StreamRDFBase;

public class SourceContextAdapter extends StreamRDFBase {

	private SourceContext<TripleTS> ctx;

	public SourceContextAdapter(SourceContext<TripleTS> ctx){
		this.ctx = ctx;
	}

	public void tripleTS(TripleTS t){
		ctx.collect(t);
	}

	@Override
	public void triple(Triple t) {
		ctx.collect(new TripleTS(t));
	}

	@Override
	public void finish(){}
}
