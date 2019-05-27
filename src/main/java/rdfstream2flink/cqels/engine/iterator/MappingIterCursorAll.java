package rdfstream2flink.cqels.engine.iterator;

import java.util.ArrayList;

import com.hp.hpl.jena.sparql.core.Var;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import rdfstream2flink.cqels.data.Mapping;
import rdfstream2flink.cqels.engine.ExecContext;
import rdfstream2flink.cqels.util.Utils;

public class MappingIterCursorAll extends MappingIterCursor {
	ArrayList<Var> vars;
	public MappingIterCursorAll(ExecContext context, Database database, ArrayList<Var> vars) {
		super(context, database);
		this.vars = vars;
		//_readNext();
	}
	
	@Override
	public void _readNext() {
		DatabaseEntry key = new DatabaseEntry(); 
		curEnt = new DatabaseEntry();
		if(cursor == null) {
			cursor = db.openCursor(null, CursorConfig.READ_COMMITTED);
		}
		if(!(cursor.getNext(key, curEnt, LockMode.DEFAULT) == OperationStatus.SUCCESS)) {
			curEnt = null;
		}
		//System.out.println("read Next not null");
	}

	@Override
	protected Mapping moveToNextMapping() {
		Mapping mapping = null;
		if(curEnt != null) {
			mapping = Utils.data2Mapping(context, curEnt, vars);
			_readNext();
		}
		return mapping;
	}
	
	

}
