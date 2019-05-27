package rdfstream2flink.cqels.engine.iterator;

import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.SafeIterator;
import com.hp.hpl.jena.sparql.core.Quad;
import rdfstream2flink.cqels.data.EnQuad;
import rdfstream2flink.cqels.data.Mapping;
import rdfstream2flink.cqels.data.MappingQuad;
import rdfstream2flink.cqels.engine.ExecContext;

public class MappingIterOnQuadStatement extends MappingIter {
	SafeIterator<EventBean> safeItr;
	EPStatement stmt;
	Quad quad;
	public MappingIterOnQuadStatement(ExecContext context, EPStatement stmt, Quad quad) {
		this(context, stmt, quad, false);
	}
	
	public MappingIterOnQuadStatement(ExecContext context, EPStatement stmt, 
					Quad quad, boolean init) {
		super(context);
		this.stmt = stmt;
		this.quad = quad;
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
			return  new MappingQuad(context,quad,(EnQuad) safeItr.next().getUnderlying());
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
