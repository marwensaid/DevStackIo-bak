package com.devstackio.maven.propertyloader;

import java.io.InputStream;
import java.util.Properties;
import javax.enterprise.context.ApplicationScoped;

/**
 *
 * @author devstackio
 */
@ApplicationScoped
public class PropertyLoader {
	
	/**
	 * load property from META-INF/filePath
	 * @param filepath directory starting at META-INF/
	 * @return 
	 */
	public Properties loadProperties( String filepath ) {
		
		Properties returnobj = new Properties();
		String path = "META-INF/"+filepath;

		InputStream in = ClassLoader.getSystemResourceAsStream( path );
		try {
			returnobj.load(in);
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try { 
				in.close();
			} catch (Exception e){
				e.printStackTrace();
			}
		}
		return returnobj;
		
	}
	/**
	 * used to grab Properties if needed @ ApplicationContextListener
	 * @param thread
	 * @return 
	 */
	public Properties loadFromThread( Thread thread, String filepath ) {
		
		Properties returnobj = new Properties();
		try {
			returnobj.load( thread.getContextClassLoader().getResourceAsStream( filepath ));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return returnobj;
	}
	
}
