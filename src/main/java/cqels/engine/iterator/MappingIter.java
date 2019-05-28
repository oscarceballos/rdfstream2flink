package cqels.engine.iterator;

import cqels.engine.ExecContext;

public abstract class MappingIter extends MappingIteratorBase {
	protected ExecContext context;

	public MappingIter(ExecContext context) {
		this.context = context;
	}
}
