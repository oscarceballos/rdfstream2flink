package rdfstream2flink.cqels.engine.iterator;

import rdfstream2flink.cqels.engine.ExecContext;

public abstract class MappingIter extends MappingIteratorBase {
	protected ExecContext context;

	public MappingIter(ExecContext context) {
		this.context = context;
	}
}
