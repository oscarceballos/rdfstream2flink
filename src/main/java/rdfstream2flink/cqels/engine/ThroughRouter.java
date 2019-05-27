package rdfstream2flink.cqels.engine;

import java.lang.reflect.Method;
import java.util.ArrayList;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.Op;
import rdfstream2flink.cqels.data.Mapping;
import rdfstream2flink.cqels.data.MappingWrapped;
import rdfstream2flink.cqels.engine.iterator.MappingIterator;

public class ThroughRouter extends OpRouterBase {
	ArrayList<OpRouter> dataflows;
	public ThroughRouter(ExecContext context, ArrayList<OpRouter> dataflows) {
		super(context, dataflows.get(0).getOp());
		this.dataflows = dataflows;
	}

	@Override
	public void route(Mapping mapping) {
		_route(new MappingWrapped(context, mapping));
	}
	
	@Override
	public MappingIterator searchBuff4Match(Mapping mapping) {
		return dataflows.get(0).searchBuff4Match(mapping);
	}
	
	@Override
	public MappingIterator getBuff() {
		return dataflows.get(0).getBuff();
	}

	public void visit(RouterVisitor rv) {
		rv.visit(this);
		OpRouter router = dataflows.get(0);
		try {
			Method method = router.getClass().getDeclaredMethod("visit", RouterVisitor.class);
			method.invoke(router, rv);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
