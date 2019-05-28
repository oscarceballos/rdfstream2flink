package cqels.engine;

import java.util.List;

import com.hp.hpl.jena.sparql.algebra.op.OpGroup;
import com.hp.hpl.jena.sparql.core.VarExprList;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterGroup;
import com.hp.hpl.jena.sparql.expr.ExprAggregator;
import cqels.data.Mapping;
import cqels.data.ProjectMapping;
import cqels.engine.iterator.MappingIterOnQueryIter;
import cqels.engine.iterator.MappingIterator;
import cqels.engine.iterator.QueryIterOnMappingIter;

/**
 * This class implements the router with group-by operator
 * @author		Danh Le Phuoc
 * @author 		Chan Le Van
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 * @email   chan.levan@deri.org
 * @see org.deri.cqels.engine.OpRouter1
 */
public class GroupRouter extends OpRouter1 {
	private VarExprList groupVars ;
    private List<ExprAggregator> aggregators ;
    
	public GroupRouter(ExecContext context, OpGroup op, OpRouter sub) {
		
		super(context, op, sub);
		//System.out.println("group "+op);
		groupVars = ((OpGroup)op).getGroupVars();
		aggregators = ((OpGroup)op).getAggregators() ;
	}
	
	@Override
	public void route(Mapping mapping) {
		ProjectMapping project = new ProjectMapping(context, mapping, groupVars.getVars());
		MappingIterator itr = calc(mapping.from().searchBuff4Match(project));
		//System.out.println("check filter" +project);
		while (itr.hasNext()) {
			Mapping _mapping = itr.next();
			//System.out.println("rout next"+_mapping);
			_route(_mapping);
		}
		itr.close();
	}
	
	@Override
	public MappingIterator searchBuff4Match(Mapping mapping) {
		return calc(sub().searchBuff4Match(mapping));
	}
	
	private MappingIterator calc(MappingIterator itr){
		QueryIterGroup groupItrGroup = new QueryIterGroup(new QueryIterOnMappingIter(context, itr),
															groupVars, aggregators, context.getARQExCtx());
		return new MappingIterOnQueryIter(context,groupItrGroup);
	}
	
	@Override
	public MappingIterator getBuff() {
		return calc(sub().getBuff());
	}

	public void visit(RouterVisitor rv) {
		rv.visit(this);
		this.subRouter.visit(rv);
	}
}



