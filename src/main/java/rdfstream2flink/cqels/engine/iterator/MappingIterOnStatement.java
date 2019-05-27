package rdfstream2flink.cqels.engine.iterator;

import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.SafeIterator;
import rdfstream2flink.cqels.data.Mapping;
import rdfstream2flink.cqels.engine.ExecContext;

public class MappingIterOnStatement extends MappingIter {
	SafeIterator<EventBean> safeItr;
	EPStatement stmt;
	public MappingIterOnStatement(ExecContext context, EPStatement stmt) {
		this(context, stmt, false);
	}
	
	public MappingIterOnStatement(ExecContext context,EPStatement stmt,boolean init) {
		super(context);
		this.stmt = stmt;
		if(init) {
			init();
		}
			
	}

	public void init() {
		safeItr = stmt.safeIterator();
	}
	
	@Override
	protected void closeIterator() {
		// TODO Auto-generated method stub
		if(safeItr != null) {
			safeItr.close();
		}
	}

	@Override
	protected boolean hasNextMapping() {
		if(safeItr != null) {
			return safeItr.hasNext();
		}
		return false;
	}

	@Override
	protected Mapping moveToNextMapping() {
		if(safeItr != null) {
			return (Mapping) safeItr.next().getUnderlying();
		}
		return null;
	}

	@Override
	protected void requestCancel() {
		if(safeItr != null) {
			safeItr.close();
		}
	}

}
