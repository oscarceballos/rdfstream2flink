package cqels.engine;

import com.hp.hpl.jena.sparql.algebra.Op;
import cqels.data.Mapping;
import cqels.engine.iterator.MappingIterator;

/**
 *This class implements the basic behaviors of a router
 * @author		Danh Le Phuoc
 * @author 		Chan Le Van
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 * @email   chan.levan@deri.org
 * @see org.deri.cqels.engine.OpRouter
 */

public abstract class OpRouterBase implements OpRouter {
	static int count = 0;
	Op op;
	/** An execution context that the router is working on */
	ExecContext context;
	int id;
	public OpRouterBase(ExecContext context, Op op) {
		this.context = context;
		this.op = op;
		id = ++count;
		//System.out.println("new op "+op);
		context.router(id, this);
	}
	
	public Op getOp() {
		// TODO Auto-generated method stub
		return op;
	}
	
	public int getId() { 
		return id;
	}
	
	public void _route( Mapping mapping) {
		//System.out.println("_route "+mapping);
		mapping.from(this);
		context.policy().next(this, mapping).route(mapping);
	}
	
	public void route(Mapping mapping) {
		// do nothing
	}
	
	public MappingIterator searchBuff4Match(Mapping mapping) {
		//TODO: missing
		return null;
	}
	
	public MappingIterator getBuff() {
		// TODO Auto-generated method stub
		return null;
	}

}
