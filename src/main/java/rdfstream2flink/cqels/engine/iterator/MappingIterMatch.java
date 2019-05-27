package rdfstream2flink.cqels.engine.iterator;

import rdfstream2flink.cqels.data.Mapping;
import rdfstream2flink.cqels.data.MappingWrapped;
import rdfstream2flink.cqels.engine.ExecContext;

public class MappingIterMatch extends MappingIterProcessBinding {
	Mapping mapping;
	public MappingIterMatch(ExecContext context, MappingIterator mIter, Mapping mapping) {
		super(mIter, context);
		this.mapping = mapping;
	}
	
	@Override
	public Mapping accept(Mapping mapping) {
		//System.out.println("compa "+ mapping +" "+this.mapping.isCompatible(mapping) +" "+ this.mapping );
		if(this.mapping.isCompatible(mapping)) { 
			return new MappingWrapped(context, mapping, this.mapping);
		}
		//TODO : check order of MappingWrapper initialization new MappingWrapped(context, this.mapping, mapping);
		return null;
	}

}
