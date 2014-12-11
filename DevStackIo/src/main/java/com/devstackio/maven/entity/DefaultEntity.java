package com.devstackio.maven.entity;

/**
 *
 * @author devstackio
 */
public class DefaultEntity {
	
	protected String id;
	protected String prefix;
	protected transient String bucket;
	
	public DefaultEntity() {
		this.prefix = this.getClass().getSimpleName();
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	/**
	 * 
	 * @return getClass().getSimpleName()
	 */
	public String getPrefix() {
		return this.getClass().getSimpleName();
	}
	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}
	/**
	 * couchbase document id
	 * @return getPrefix()+":"+this.getId();
	 */
	public String getDocId() {
		return this.getPrefix()+":"+this.getId();
	}

	public String getBucket() {
		return bucket;
	}

	public void setBucket(String bucket) {
		this.bucket = bucket;
	}
	
}
