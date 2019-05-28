package cqels.engine;

import com.hp.hpl.jena.query.Query;
import cqels.data.Mapping;

/**
 * This interface contains set of methods which are behaviors of routing policy
 * 
 * @author		Danh Le Phuoc
 * @author 		Chan Le Van
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 * @email   chan.levan@deri.org
 */
public interface RoutingPolicy 
{
	public OpRouter next(OpRouter curRouter, Mapping mapping);
	public ContinuousSelect registerSelectQuery(Query query);
	public ContinuousConstruct registerConstructQuery(Query query);
}
