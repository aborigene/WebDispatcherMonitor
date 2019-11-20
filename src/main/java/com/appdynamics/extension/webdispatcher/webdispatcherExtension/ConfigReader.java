package com.appdynamics.extension.webdispatcher.webdispatcherExtension;

import java.io.FileNotFoundException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConfigReader {
	String result = "";
	InputStream inputStream;
        Logger LOGGER;
        
        public ConfigReader(){
            LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
        }
	public Properties getPropValues(String propFileName) throws IOException {
		
		Properties prop = new Properties();
		
		try {
			File configFile = new File(propFileName);
                         LOGGER.log(Level.WARNING, "Config file path: {0}", configFile.getCanonicalPath());
			inputStream = new FileInputStream(configFile);
			//inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
			//getClass().getClassLoader().getResourceAsStream(propFileName).
			if (inputStream != null) {
				prop.load(inputStream);
			} else {
				throw new FileNotFoundException("Property file '" + propFileName + "' not found in the classpath");
			}
		} catch (Exception e) {
                        LOGGER.log(Level.WARNING, "Exception reading configuration file: {0}", e);
		} finally {
			inputStream.close();
		}
		return prop;
	}
	
	public boolean setPropValues(String propFileName, Properties properties) throws IOException{
		boolean result=false;
		OutputStream outputStream;
		try {
			File configFile = new File(propFileName);
                        LOGGER.log(Level.WARNING, "Config file path: {0}", configFile.getCanonicalPath());
			outputStream = new FileOutputStream(configFile);
			outputStream.write(properties.toString().replace("{", "").replace("}", "").replace(", ",System.getProperty("line.separator")).getBytes());
			outputStream.flush();
			outputStream.close();
                        result=true;
		} catch (Exception e) {
			System.out.println("Exception writing configuration file: " + e);
                        LOGGER.log(Level.WARNING, "Exception writing configuration file: {0}", e);
		}
		
		return result;
	}
}
