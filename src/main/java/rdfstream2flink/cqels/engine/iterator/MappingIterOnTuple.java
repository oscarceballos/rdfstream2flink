package rdfstream2flink.cqels.engine.iterator;

import java.util.HashMap;
import java.util.Iterator;

import org.openjena.atlas.lib.Tuple;

import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.tdb.store.NodeId;
import rdfstream2flink.cqels.data.Mapping;
import rdfstream2flink.cqels.data.MappingWrapped;
import rdfstream2flink.cqels.data.TupleMapping;
import rdfstream2flink.cqels.engine.ExecContext;

public class MappingIterOnTuple extends MappingIter {
	
	Iterator<Tuple<NodeId>> tupleItr;
	HashMap<Var,Integer> var2Idx;
	Mapping mapping;
	public MappingIterOnTuple(ExecContext context, Iterator<Tuple<NodeId>> tupleItr,
							  HashMap<Var,Integer> var2Idx, Mapping mapping) {
		super(context);
		this.tupleItr = tupleItr;
		this.var2Idx = var2Idx;
		this.mapping = mapping;
	}
	
	public MappingIterOnTuple(ExecContext context, Iterator<Tuple<NodeId>> tupleItr, 
			HashMap<Var,Integer> var2Idx) {
		this(context, tupleItr, var2Idx, null);
	}
	
	@Override
	protected void closeIterator() {
		//TODO: close ??? or remove?
	}

	@Override
	protected boolean hasNextMapping() {		
		return tupleItr.hasNext(); 
	}

	@Override
	protected Mapping moveToNextMapping() { 
		if(mapping == null) {
			return new TupleMapping(context, tupleItr.next(), var2Idx);
		}
		return new MappingWrapped(context, new TupleMapping(
				context, tupleItr.next(), var2Idx), mapping);
	}

	@Override
	protected void requestCancel() {
		// TODO: cancel a the tuple Iterator?
	}

}
