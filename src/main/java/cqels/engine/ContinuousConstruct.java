package cqels.engine;

import java.util.ArrayList;

import com.hp.hpl.jena.query.Query;
import cqels.data.Mapping;

/**
 * This class acts as a router standing in the root of the tree 
 * if the query is a construct-type
 * @author		Danh Le Phuoc
 * @author 		Chan Le Van
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 * @email   chan.levan@deri.org
 * @see org.deri.cqels.engine.OpRouter
 * @see OpRouterBase
 */
public class ContinuousConstruct extends OpRouter1 {
	Query query;
	
	ArrayList<ConstructListener> listeners;
	public ContinuousConstruct(ExecContext context, Query query, OpRouter subRouter) {
		super(context, subRouter.getOp(), subRouter);
		this.query = query;
		listeners = new ArrayList<ConstructListener>();
	}
	
	@Override
	public void route(Mapping mapping) {
		for(ConstructListener lit : listeners)
			lit.update(mapping);
	}
	
	public void register(ConstructListener lit) {
		lit.setTemplate(query.getConstructTemplate());
		listeners.add(lit); 
	}

	public void visit(RouterVisitor rv) {
		rv.visit(this);
		this.subRouter.visit(rv);
	}
}
