package com.sleepycat.je.dbi;

import com.sleepycat.je.OperationFailureException;
import com.sleepycat.je.txn.Locker;

/**
 * Thrown by Txn.markDeleteAtTxnEnd, if the deletedDatabases contains the same 
 * database with different deleteAtCommit variables. This case will happen if 
 * doing truncateDatabase and removeDatabase operations on the same database in 
 * the same txn.
 */
public class OperationAbortException extends OperationFailureException {

    private static final long serialVersionUID = 1;

    /**
     * For internal use only.
     * @hidden
     */
    public OperationAbortException(final Locker locker, final String message) {
        super(locker, true /*abortOnly*/, message, null /*cause*/);
    }

    /**
     * For internal use only.
     * @hidden
     */
    private OperationAbortException(String message,
                                    OperationAbortException cause) {
        super(message, cause);
    }

    /**
     * For internal use only.
     * @hidden
     */
    @Override
    public OperationAbortException wrapSelf(String msg) {
        return new OperationAbortException(msg, this);
    }
}
