package org.deri.cqels.engine;

import java.util.List;

import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.core.Var;
import org.deri.cqels.data.Mapping;
import org.deri.cqels.data.ProjectMapping;
import org.deri.cqels.engine.iterator.MappingIterator;
import org.deri.cqels.engine.iterator.NullMappingIter;

/**
 * This class implements the router with project operator
 * @author		Danh Le Phuoc
 * @author 		Chan Le Van
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 * @email   chan.levan@deri.org
 * @see org.deri.cqels.engine.OpRouter1
 */
public class ProjectRouter extends OpRouter1 {
	List<Var> vars;
	public ProjectRouter(ExecContext context, OpProject op, OpRouter sub) {
		super(context, op, sub);
		vars = op.getVars();
	}

	@Override
	public void route(Mapping mapping) {
		//System.out.println("project "+mapping);
		_route(new ProjectMapping(context, mapping, vars));
	}
	
	@Override
	public MappingIterator searchBuff4Match(Mapping mapping) {
		//TODO: check if necessary to call this method
		return NullMappingIter.instance();
	}
	
	@Override
	public MappingIterator getBuff() {
		return NullMappingIter.instance();
	}

	public void visit(RouterVisitor rv) {
		rv.visit(this);
		this.subRouter.visit(rv);
	}
}
