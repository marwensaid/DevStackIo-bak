package com.devstackio.maven.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.JsonLongDocument;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.transcoder.JsonTranscoder;
import com.couchbase.client.java.transcoder.RawJsonTranscoder;
import com.devstackio.maven.application.config.AbstractAppData;
import com.google.gson.Gson;
import com.devstackio.maven.entity.DefaultEntity;
import com.devstackio.maven.logging.IoLogger;
import com.devstackio.maven.uuid.UuidGenerator;
import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import org.apache.log4j.Level;

/**
 * if entity Dao is going to couchbase, extend this
 * has the two conversion methods needed to store documents into couchbase using java client 2.0.1 for couchbase 3
 * @author devstackio
 */
@RequestScoped
public abstract class EntityCbDao implements IEntityDao {
	
	protected String bucketName;
	protected Gson gson;
	protected UuidGenerator uuidGenerator;
	protected CbDao cbDao;
	protected IoLogger ioLogger;
	protected AbstractAppData appData;
	
	/**
	 * bucket name used for all crud ops on sub entity object
	 * @return 
	 */
	public abstract String getBucketName();
	/**
	 * override this method to return the Object as that specific entity type [ ex a ContractEntity obj would get returned ]
	 * @param jsonDoc JsonDocument being returned from bucket.get(id) in {@link #read(String) read}
	 * @return 
	 */
	public abstract Object convert( JsonDocument jsonDoc ) throws NullPointerException;
	/**
	 * for generating session store into couchbase we need to implement getAppData, returning an object extending AbstractAppData with an appName
	 * @return 
	 */
	protected abstract AbstractAppData getAppData();
	
	@Inject
	public void setIoLogger(IoLogger iologger) {
		this.ioLogger = iologger;
	}
	@Inject
	public void setCbDao(CbDao cbdao) {
		this.cbDao = cbdao;
	}
	@Inject
	public void setUuidGenerator(UuidGenerator uuidgenerator) {
		this.uuidGenerator = uuidgenerator;
	}
	/**
	 * creates entity in database
	 * @param obj
	 * @return id of entity after insertion
	 * catches DocumentAlreadyExistsException already in couchbase - logs to "DocAlreadyExists.log"
	 * persists to Master node
	 */
	@Override
	public String create( Object entityobj ) {
		
		String returnobj = "";
		Bucket bucket = this.getBucket();
		String prefix = "";
		try {
			DefaultEntity entity = (DefaultEntity) entityobj;
			prefix = entity.getPrefix();
			
			String entid = entity.getId();
			if( entid == null || entid.isEmpty() ) {
				String entityId = this.generateId( bucket, prefix );
				entity.setId( entityId );
			}
			
			RawJsonDocument rJsonDoc = this.convertToRawJsonDoc( entity.getDocId(), entity );
			returnobj = prefix+":"+entity.getId();
			
			bucket.insert( rJsonDoc,PersistTo.MASTER );
			String logMsg = "-- tried upsert on : " + rJsonDoc + " --";
			this.ioLogger.logTo("DevStackIo-debug", Level.INFO, logMsg);
			
		} catch (DocumentAlreadyExistsException e) {
			this.ioLogger.logTo("DevStackIo-debug", Level.INFO, "document : " + returnobj + " already exists in couchbase.");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return returnobj;
	}
	/**
	 * stores this entity object to couchbase using user's uuid instead of an incremented id
	 * uuid will be stored as browser cookie
	 * @param entityobj
	 * @return 
	 */
	public String createToSession( Object entityobj ) {
		
		String returnobj = "";
		String uuid = "";
		
		try {
			uuid = this.uuidGenerator.getUuid(this.getAppData().getAppName());
			DefaultEntity entity = (DefaultEntity) entityobj;
			entity.setId( uuid );
			returnobj = this.create( entity );
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return returnobj;
		
	}
	/**
	 * used to query simple gets from couchbase
	 * @param id
	 * @return object as entityobj (ex: ContractEntity)
	 * *requires protected Object convert( JsonDocument jsonDoc ) to be implemented in subclass
	 */
	@Override
	public Object read( String id ) {
		
		Object returnobj = new Object();
		Bucket bucket = this.getBucket();
		String docId = id;
		try {
			returnobj = this.convert( bucket.get(docId) );
			
		} catch (NullPointerException e) {
			this.ioLogger.logTo("DevStackIo-debug", Level.INFO, "document : " + docId + " not found in couchbase.");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return returnobj;
	}
	/**
	 * if document does not exist in couchbase will return null
	 * @param entityobj
	 * @return 
	 */
	public Object readFromSession( Object entityobj ) {
		
		Object returnobj = new Object();
		Bucket bucket = this.getBucket();
		String docId = "";
		DefaultEntity entity = new DefaultEntity();
		try {
			entity = (DefaultEntity) entityobj;
			docId = entity.getPrefix()+":"+this.appData.getUuid();
			String logMsg = "trying readFromSession using docid : " + docId + " bucket is : " + bucket;
			this.ioLogger.logTo("DevStackIo-debug", Level.INFO, logMsg);
			entity.setId( docId );
			returnobj = this.convert( bucket.get(docId) );
			
		} catch (NullPointerException e) {
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return returnobj;
	}
	/**
	 * updates entityObject to couchbase
	 * catches DocumentDoesNotExistException if not found - logs to "DocDoesNotExist.log"
	 * persists to Master node
	 * @param entityobj 
	 */
	@Override
	public void update( Object entityobj ) {
		
		Bucket bucket = this.getBucket();
		String docId = "";
		try {
			DefaultEntity entity = (DefaultEntity) entityobj;
			docId = entity.getPrefix()+":"+entity.getId();
			
			RawJsonDocument rJsonDoc = this.convertToRawJsonDoc( entity.getDocId(), entity );
			
			bucket.replace( rJsonDoc,PersistTo.MASTER );
			String logMsg = "-- tried replace on : " + rJsonDoc + " --";
			this.ioLogger.logTo("DevStackIo-debug", Level.INFO, logMsg);
			
		} catch (DocumentDoesNotExistException e) {
			this.ioLogger.logTo("DevStackIo-debug", Level.INFO, "document : " + docId + " not found in couchbase.");
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	public void updateToSession( Object entityobj ) {
		
		Bucket bucket = this.getBucket();
		String docId = "";
		try {
			DefaultEntity entity = (DefaultEntity) entityobj;
			docId = entity.getPrefix()+":"+this.getAppData().getUuid();
			String logMsg = "update to session call : docid is : " + docId;
			this.ioLogger.logTo("DevStackIo-debug", Level.INFO, logMsg);
			
			RawJsonDocument rJsonDoc = this.convertToRawJsonDoc( entity.getDocId(), entity );
			
			bucket.replace( rJsonDoc,PersistTo.MASTER );
			
		} catch (DocumentDoesNotExistException e) {
			this.ioLogger.logTo("DevStackIo-debug", Level.INFO, "document : " + docId + " not found in couchbase.");
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	/**
	 * persists to Master node
	 * @param docid 
	 */
	@Override
	public void delete( String docid ) {
		
		Bucket bucket = this.getBucket();
		try {
			bucket.remove( docid,PersistTo.MASTER );
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	/**
	 * returns Bucket from protected String bucketName
	 * @return 
	 */
	protected Bucket getBucket() {
		
		Bucket returnobj = null;
		try {
			returnobj = this.cbDao.getBucket( this.getBucketName() );
		} catch (Exception e) {
			e.printStackTrace();
		}
		return returnobj;
	}
	
	/**
	 * creates full JsonDocument required for inserting to couchbase
	 * @param docid should be entity.prefix() + ":" + entity.getId();
	 * @param jsonobj created by {@link #convertToRawJsonDoc(Object) convertToRawJsonDoc};
	 * @return 
	 */
	protected JsonDocument JsonObjectToJsonDocument( String docid, JsonObject jsonobj ) {
		
		JsonDocument returnobj = null;
		try {
			returnobj = JsonDocument.create( docid, jsonobj );
		} catch (Exception e) {
			e.printStackTrace();
		}
		return returnobj;
		
	}
	/**
	 * uses gson and JsonTranscoder to create a JsonDocument for use with {@link #JsonObjectToJsonDocument(String, JsonObject) JsonObjectToJsonDocument}
	 * @param entityobj
	 * @return 
	 */
	protected RawJsonDocument convertToRawJsonDoc( String docid, Object entityobj ) {
		
		RawJsonDocument returnobj = null;
		Gson gson = new Gson();
		String docId = docid;
		String jsonData = "";
		
		try {
			jsonData = gson.toJson( entityobj );
			returnobj = RawJsonDocument.create( docId , jsonData );
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return returnobj;
		
	}
	
	/**
	 * increments entity prefix counter on couchbase
	 *
	 * @return returns id related to String (entity)prefix passed in(ex: "22")
	 */
	protected String generateId( Bucket bucket, String prefix) {
		
		String returnobj = "";
		Bucket cbBucket = bucket;

		try {
			JsonLongDocument jsonLongDoc = cbBucket.counter(prefix, 1);
			Long idLong = jsonLongDoc.content();
			returnobj = Long.toString( idLong );
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return returnobj;
		
	}
	/**
	 * @return entity.prefix + ":" + uuid
	 */
	public String getEntitySessionId() {
		
		String returnobj = "";
		try {
			String className = this.getClass().getName();
			int index = className.indexOf("Entity");
			returnobj = className.substring( 0,index+6 );
			returnobj += ":" + this.uuidGenerator.getUuid( this.appData.getAppName() );
		} catch (Exception e) {
			e.printStackTrace();
		}
		return returnobj;
	}
	
}
