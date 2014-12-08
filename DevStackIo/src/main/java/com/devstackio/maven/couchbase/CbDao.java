package com.devstackio.maven.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.view.Stale;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;
import com.devstackio.maven.logging.IoLogger;
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
	
	private static Cluster cluster;
	private HashMap<String,Bucket> buckets;
	private IoLogger ioLogger;

	@Inject
	public void setIoLogger(IoLogger iologger) {
		this.ioLogger = iologger;
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
			this.ioLogger = new IoLogger();
		}
	}
	
	/**
	 * creates cluster connection
	 * should be called from contextInitialized [ ServletContextListener ]
	 * @param ips ip of server ex: {"127.0.0.1", "devstackio.com"}
	 * @param bucketname
	 * @param bucketpass
	 */
	public void createConnection(String[] ips) {
		ArrayList<String> ipList = new ArrayList();
		for (int i = 0; i < ips.length; i++) {
			String string = ips[i];
			ipList.add(string);
		}
		this.initCluster( ipList );
	}
	/**
	 * creates cluster connection
	 * should be called from contextInitialized [ ServletContextListener ]
	 * @param ips ip[] of server ex: ["127.0.0.1","devstackio.com"]
	 * @param bucketname
	 * @param bucketpass 
	 */
	public void createConnection(ArrayList<String> ips) {
		this.initCluster( ips );
	}
	private void initCluster(ArrayList<String> ips) {

		try {
			cluster = CouchbaseCluster.create( ips );
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public void addBucketToCluster( String bucketname, String bucketpass ) {
		String bName = bucketname;
		String bPass = bucketpass;
		try {
			Bucket bucket = cluster.openBucket( bName, bPass);
			this.getBuckets().put( bName, bucket);
			
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
			returnobj = this.getBuckets().get( bucketname );
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
			this.getBuckets().get(bucketname).close();
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
			viewQuery.stale(Stale.FALSE);
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

	public HashMap<String,Bucket> getBuckets() {
		return buckets;
	}
	
}

