package com.devstackio.maven.entity;

/**
 *
 * @author devstackio
 */
public class DefaultEntity {
	
	protected String id;
	private String prefix;
	
	public DefaultEntity() {
		this.prefix = this.getClass().getSimpleName();
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getPrefix() {
		return this.getClass().getSimpleName();
	}
	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}
	
}
