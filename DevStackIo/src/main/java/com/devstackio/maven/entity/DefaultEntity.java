package com.devstackio.maven.entity;

/**
 *
 * @author devstackio
 */
public class DefaultEntity {
	
	protected String id;

	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getPrefix() {
		return this.getClass().getSimpleName();
	}
	
}
