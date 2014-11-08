package com.devstackio.maven.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;
import com.devstackio.maven.logging.IoLogger;
import com.devstackio.maven.uuid.UuidGenerator;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/**
 * connecting / updating / retrieving couchbase data
 *
 * @author devstackio
 */
@ApplicationScoped
public class CbDao {
	
	private final int QUERY_TIMEOUT = 25000;									// wait up to 10 seconds for an operation to succeed
	private final int QUEUE_TIMEOUT = 5000;									// wait up to 5 seconds when trying to enqueue an operation
	private Cluster cluster;
	private final String LOGFILE = "cbDao";
	private int sessionEntityTimeout = 60 * 30;									// amount of time session objects remain in couchbase
	private HashMap<String,Bucket> buckets;
	private UuidGenerator uuidGenerator;
	private IoLogger ioLogger;

	@Inject
	public void setIoLogger(IoLogger iologger) {
		this.ioLogger = iologger;
	}
	@Inject
	public void setUuidGenerator(UuidGenerator uuidgenerator) {
		this.uuidGenerator = uuidgenerator;
	}
	public CbDao() {
		this.buckets = new HashMap();
	}
	/**
	 * use if needed outside web application ( running core Java files )
	 * @param webBased
	 */
	public CbDao( boolean webBased ) {
		this();
		if( !webBased ) {
			this.uuidGenerator = new UuidGenerator();
			this.ioLogger = new IoLogger();
		}
	}
	
//
//	@Inject
//	public void setioLogger(ioLogger ioLogger) {
//		this.ioLogger = ioLogger;
//	}
//	@Inject
//	public void setUuidUtil(UuidUtil uuidutil) {
//		this.uuidUtil = uuidutil;
//	}
//	public CbDao() {
//		client = new HashMap<String, CouchbaseClient>();
//		cbTransfer = new CbTransfer();
//		cbGet = new CbGet();
//	}
	
//	public CouchbaseClient getClient(String bucketname) {
//		CouchbaseClient returnobj = null;
//		try {
//			returnobj = this.client.get(bucketname);
//		} catch (Exception e) {
//			this.ioLogger.logTo(this.LOGFILE, Level.ERROR, "trying to get client from bucket : " + bucketname + " : error : " + e.getMessage());
//			e.printStackTrace();
//		}
//		return returnobj;
//	}
	
	/**
	 * saves and creates new couchbase bucket to hashMap
	 * should be called from contextInitialized [ ServletContextListener ]
	 * @param ips ip of server ex: {"127.0.0.1", "ionblitz.com"}
	 * @param bucketname
	 * @param bucketpass
	 */
	public void createConnection(String[] ips, String bucketname, String bucketpass) {
		
		ArrayList<String> ipList = new ArrayList();
		for (int i = 0; i < ips.length; i++) {
			String string = ips[i];
			ipList.add(string);
		}
		this.storeBucketReference( ipList, bucketname, bucketpass );
		
	}
	/**
	 * saves and creates new couchbase bucket to hashMap
	 * should be called from contextInitialized [ ServletContextListener ]
	 * @param ips ip[] of server ex: ["127.0.0.1","ionblitz.com"]
	 * @param bucketname
	 * @param bucketpass 
	 */
	public void createConnection(ArrayList<String> ips, String bucketname, String bucketpass) {
		
		this.storeBucketReference(ips, bucketname, bucketpass);
		
	}
	private void storeBucketReference(ArrayList<String> ips,String bucketname, String bucketpass) {

		ArrayList<String>ipList = ips;
		String bName = bucketname;
		String bPass = bucketpass;

		try {
			cluster = CouchbaseCluster.create( ipList );
			Bucket bucket = cluster.openBucket( bName, bPass);
			this.buckets.put( bName, bucket);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public void closeClusterConnection() {
		this.cluster.disconnect();
	}
	public Bucket getBucket( String bucketname ) {
		
		Bucket returnobj = null;
		
		try {
			returnobj = this.buckets.get( bucketname );
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return returnobj;
	}
	/**
	 * should be called from contextDestroyed [ ServletContextListener ]
	 */
	public void destroyConnection(String bucketname) {
		try {
			//this.ioLogger.logTo(this.LOGFILE, Level.INFO, "destroyingConnection to : " + bucketname );
			this.buckets.get(bucketname).close();
		} catch (Exception e) {
			//this.ioLogger.logTo(this.LOGFILE, Level.ERROR, "destroyingConnection to : " + bucketname + " : error : " + e.getMessage());
			e.printStackTrace();
		}
	}
	
	/**
	 * used to return a Map<String,Object> of the List of Document IDs returned
	 * from a View that uses emit(meta.id);
	 *
	 * @param client
	 * @param designDoc
	 * @param viewName - must use emit(meta.id);
	 * @return
	 */
	public ArrayList<JsonDocument> getBulkData( Bucket bucket, String designDoc, String viewName ) {
		
		ArrayList<JsonDocument> returnobj = new ArrayList();
		
		try {
			ViewQuery viewQuery = ViewQuery.from(designDoc, viewName);
			bucket.query(viewQuery);
			ViewResult viewResult = bucket.query( viewQuery );
			Iterator<ViewRow> viewResults = viewResult.rows();
			
			while ( viewResults.hasNext() ) {
				ViewRow row = viewResults.next();
				returnobj.add( row.document() );
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return returnobj;
	}
	
	public void setToSession( CouchbaseEntity entity, Bucket bucket ) {
		
		try {
			JsonDocument entityDoc = entity.build();
			bucket.upsert( entityDoc );
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * will automatically concatenate ":"+uuid to passed in entity prefix
	 * @param appid used as cookiename
	 * @param prefix entity prefix ex: "resort"
	 * @param cbclient client to store to
	 * @param data json for entity ex: resortEntity.getValue()
	 */
//	public void setSessionEntity(String appid, String prefix, Bucket bucket, JsonDocument data) {
//
//		String cookieName = appid;
//		String uuid = "";
//		String docid = "";
//
//		Bucket cbBucket = bucket;
//
//		try {
//
//			uuid = this.uuidGenerator.getUuid( cookieName );
//			docid = prefix + ":" + uuid;
//			
//			JsonObject obj = JsonObject.create();
//			obj.put("name", "gart");
//			obj.put("test","ing");
//			JsonDocument doc = JsonDocument.create("docId", obj);
//			doc.
//			JsonDocument inserted = bucket.upsert(doc);
//			
//			cbBucket.upsert(J)
//			cbBucket.set(docid, this.getSessionEntityTimeout(), data);
//
//		} catch (NullPointerException e) {
//			//this.ioLogger.logTo(this.LOGFILE, Level.INFO, "nullPointer in ["+appid+"] setSessionEntity : " + prefix + " : data : " + data );
//		} catch (Exception e) {
//			//this.ioLogger.logTo(this.LOGFILE, Level.ERROR, e.getMessage());
//			e.printStackTrace();
//		}
//	}
	/**
	 * returns session entity from couhchbase, resets document's expiry time
	 * @param appid used as cookiename
	 * @param prefix entity prefix ex: "resort"
	 * @param cbclient client to get from
	 * @return Object to use in CouchbaseDoc's convert method ex : (ContractEntity) returnobj.convert((String)this.cbDao.getSessionEntity(...), ContractEntity.class);
	 */
//	public Object getSessionEntity(String appid, String prefix, CouchbaseClient cbclient) {
//
//		Object returnobj = new Object();
//		String cookieName = appid;
//		String uuid = "";
//		String docid = "";
//
//		CouchbaseClient cbClient = cbclient;
//
//		try {
//
//			uuid = this.uuidUtil.getUuid( cookieName );
//			docid = prefix + ":" + uuid;
//			CASValue<Object> cbData = cbClient.getAndTouch(docid, this.getSessionEntityTimeout());
//			returnobj = cbData.getValue();
//
//		} catch (NullPointerException e) {
//			this.ioLogger.logTo(this.LOGFILE, Level.INFO, "nullPointer in ["+appid+"] getSessionEntity : " + prefix );
//		} catch (Exception e) {
//			this.ioLogger.logTo(this.LOGFILE, Level.ERROR, e.getMessage());
//			e.printStackTrace();
//		}
//
//		return returnobj;
//	}
	
	
	/**
	 * sets CouchbaseDoc entity by it's id [ entity.getPrefix() + : + entity.getId() ]
	 * @param entity
	 */
//	public void setEntity(CouchbaseDoc entity, String bucketname) {
//
//		CouchbaseDoc cbEntity = entity;
//		String bucketName = bucketname;
//
//		try {
//
//			String key = cbEntity.getPrefix()+":"+cbEntity.getId();
//			this.getClient( bucketName ).set( key, cbEntity.getValue() );
//
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
	
	private ArrayList<URI> createNodesFromIps(String[] ips) {
		ArrayList<URI> nodes = new ArrayList();
		for (int i = 0; i < ips.length; i++) {
			String ip = ips[i];
			nodes.add(URI.create(ip));
		}
		return nodes;
	}
	
	private ArrayList<URI> createNodesFromIps(ArrayList<String> ips) {
		ArrayList<URI> nodes = new ArrayList();
		for (int i = 0; i < ips.size(); i++) {
			String ip = ips.get(i);
			nodes.add(URI.create(ip));
		}
		return nodes;
	}
	
	public int getSessionEntityTimeout() {
		return sessionEntityTimeout;
	}
	
	public void setSessionEntityTimeout(int sessionEntityTimeout) {
		this.sessionEntityTimeout = sessionEntityTimeout;
	}
	
}

