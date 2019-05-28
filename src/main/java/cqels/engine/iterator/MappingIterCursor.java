package cqels.engine.iterator;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import cqels.engine.ExecContext;

public abstract class MappingIterCursor extends MappingIter {
	
	protected Database db;
	protected Cursor cursor;
	protected DatabaseEntry curEnt;
	
	public MappingIterCursor(ExecContext context, Database db) {
		super(context);
		this.db = db;
	}
	
	public abstract void _readNext();
	
	@Override
	protected boolean hasNextMapping() {
		if(cursor == null) {
			_readNext();
		}
		return curEnt != null; 
	}
	

	@Override
	protected void closeIterator() { 
		if(cursor != null) {
			cursor.close(); 
		}
		cursor = null;
	}
	
	@Override
	protected void requestCancel() { 
		closeIterator();
	}
}
