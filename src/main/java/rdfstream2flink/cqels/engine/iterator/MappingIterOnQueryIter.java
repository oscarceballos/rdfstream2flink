package rdfstream2flink.cqels.engine.iterator;

import com.hp.hpl.jena.sparql.engine.QueryIterator;
import rdfstream2flink.cqels.data.Binding2Mapping;
import rdfstream2flink.cqels.data.Mapping;
import rdfstream2flink.cqels.engine.ExecContext;

public class MappingIterOnQueryIter extends MappingIter {
	QueryIterator queryItr;
	public MappingIterOnQueryIter(ExecContext context, QueryIterator queryItr) {
		super(context);
		this.queryItr = queryItr;
	}

	@Override
	protected void closeIterator() {
		queryItr.close();
	}

	@Override
	protected boolean hasNextMapping() {
		// TODO Auto-generated method stub
		return queryItr.hasNext();
	}

	@Override
	protected Mapping moveToNextMapping() {
		if(!queryItr.hasNext()) {
			return null;
		}
		return new Binding2Mapping(context, queryItr.next()) ;
	}

	@Override
	protected void requestCancel() {
		queryItr.cancel();	
	}
}
