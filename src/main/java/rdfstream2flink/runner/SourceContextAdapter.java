package rdfstream2flink.runner;

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.StreamRDFBase;

public class SourceContextAdapter extends StreamRDFBase {

	private SourceContext<Triple> ctx;

	public SourceContextAdapter(SourceContext<Triple> ctx){
		this.ctx = ctx;
	}

	@Override
	public void triple(Triple t){
		ctx.collect(t);
	}

	@Override
	public void finish(){}
}
