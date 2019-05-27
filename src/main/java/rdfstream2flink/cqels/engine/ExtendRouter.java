package rdfstream2flink.cqels.engine;

import com.hp.hpl.jena.sparql.algebra.op.OpExtend;
import com.hp.hpl.jena.sparql.core.VarExprList;
import rdfstream2flink.cqels.data.ExtendMapping;
import rdfstream2flink.cqels.data.Mapping;
import rdfstream2flink.cqels.engine.iterator.MappingIterator;
import rdfstream2flink.cqels.engine.iterator.NullMappingIter;

public class ExtendRouter extends OpRouter1 {
	VarExprList exprs;
	public ExtendRouter(ExecContext context, OpExtend op, OpRouter sub) {
		super(context, op, sub);
		exprs = op.getVarExprList();
	}

	@Override
	public void route(Mapping mapping) {
		//System.out.println("extend "+mapping);
		_route(new ExtendMapping(context, mapping, exprs));
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
