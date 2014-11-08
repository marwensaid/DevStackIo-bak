package com.devstackio.maven.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

/**
 *
 * @author devstackio
 */
public abstract class CouchbaseEntity {
	
	protected String id;
	protected String prefix;

	public String getId() {
		return id;
	}

	public String getPrefix() {
		return prefix;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}
	
	public String generateId( Bucket bucket ) {
		String returnobj = "";
		try {
			returnobj = bucket.counter(prefix, 1).toString();
		} catch (Exception e) {
			e.printStackTrace();
		}
		this.setId( returnobj );
		return returnobj;
	}
	/**
	 * adds id and prefix into JsonObject
	 * this should be called after adding any custom parameters into the CouchbaseEntity
	 * ex: 
	 * content = JsonObject.empty().put("contract", contract);
	 * <bold>content = this.addDefaults( content );</bold>
	 * returnobj = JsonDocument.create( this.getPrefix()+":"+this.getId(), content);
	 * @param obj
	 * @return 
	 */
	protected JsonObject addDefaults( JsonObject obj ) {
		JsonObject returnobj = obj;
		returnobj.put("id", this.getId());
		returnobj.put("prefix",this.getPrefix());
		return returnobj;
	}
	
	public abstract JsonDocument build();
	
}
