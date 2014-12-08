package com.devstackio.maven.databaseshared;

/**
 *
 * @author devstackio
 */
public interface IDao<T> {
	/**
	 * database CRUD
	 * @param entityobj
	 * @return id of entity after insertion
	 */
	public String create( T entityobj );
	public Object read( String id );
	public void update( T entityobj );
	public void delete( String docid );
	
}
