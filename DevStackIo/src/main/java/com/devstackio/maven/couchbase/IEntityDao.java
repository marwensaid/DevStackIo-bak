package com.devstackio.maven.couchbase;

/**
 *
 * @author devstackio
 */
public interface IEntityDao<T> {
	/**
	 * creates entity in database
	 * @param entityobj
	 * @return id of entity after insertion
	 */
	public String create( T entityobj );
	public Object read( String id );
	public void update( T entityobj );
	public void delete( String docid );
	
}
