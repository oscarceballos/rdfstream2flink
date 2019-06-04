package org.deri.cqels.engine.iterator;

import java.util.Iterator;

import org.openjena.atlas.lib.Closeable;

import org.deri.cqels.data.Mapping;

public interface MappingIterator extends Closeable, Iterator<Mapping> {
    /** 
     *Get next binding 
     */ 
    public Mapping nextMapping() ;
     
    /**
     * Cancels the query as soon as is possible for the given iterator
     */
    public void cancel();
}
