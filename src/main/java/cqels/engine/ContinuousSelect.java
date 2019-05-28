package cqels.engine;

import java.util.ArrayList;

import com.hp.hpl.jena.query.Query;
import cqels.data.Mapping;

/**
 * This class acts as a router standing in the root of the tree 
 * if the query is a select-type
 * @author		Danh Le Phuoc
 * @author 		Chan Le Van
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 * @email   chan.levan@deri.org
 * @see org.deri.cqels.engine.OpRouter
 * @see OpRouterBase
 */
public class ContinuousSelect extends OpRouter1 {
	Query query;
	ArrayList<ContinuousListener> listerners;
	
	public ContinuousSelect(ExecContext context, Query query, OpRouter subRouter) {
		super(context, subRouter.getOp(), subRouter);
		listerners = new ArrayList<ContinuousListener>();
	}
	
	@Override
	public void route(Mapping mapping) {
		//System.out.println("out select "+mapping);
		for(ContinuousListener lit:listerners)
			lit.update(mapping);
	}
	
	public void register(ContinuousListener lit){ 
		listerners.add(lit); 
	}

	public void visit(RouterVisitor rv) {
		rv.visit(this);
		this.subRouter.visit(rv);
	}
}
