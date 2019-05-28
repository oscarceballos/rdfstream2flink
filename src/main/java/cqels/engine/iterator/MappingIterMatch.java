package cqels.engine.iterator;

import cqels.data.Mapping;
import cqels.data.MappingWrapped;
import cqels.engine.ExecContext;

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
