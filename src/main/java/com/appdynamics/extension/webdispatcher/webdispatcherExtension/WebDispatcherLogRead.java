package com.appdynamics.extension.webdispatcher.webdispatcherExtension;

import java.util.Properties;
import java.util.List;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.util.stream.*;
import java.io.*;
import java.util.Comparator;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.Formatter;
import java.io.IOException;
import java.sql.Connection;
import java.text.DateFormat;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.SimpleFormatter;
import org.graalvm.compiler.nodes.BreakpointNode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WebDispatcherLogRead{
	
        Logger LOGGER;
        String MetricRootProperty;
        
        public WebDispatcherLogRead(String MetricRoot){
            LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
            MetricRootProperty = MetricRoot;
        }
        
        private boolean clearReadProperties(ConfigReader configuration){
            try{
                Properties writeProperties = new Properties();
                writeProperties.setProperty("last_position", "0");
                writeProperties.setProperty("last_file_length", "0");
                writeProperties.setProperty("last_line_contents", "");
                //System.out.println("Writing properties: "+writeProperties.toString().replace("{", "").replace("}", ""));
                LOGGER.log(Level.INFO, "Writing properties: {0}", writeProperties.toString().replace("{", "").replace("}", ""));
                configuration.setPropValues("read.properties", writeProperties);
                return true;
            }
            catch (IOException e2){
                //System.out.println("Unable to write properties: "+e2.getMessage());
                LOGGER.log(Level.WARNING, "Unable to write properties: {0}", e2.getMessage());
                e2.printStackTrace();
                return false;
            }
        }
        
        private void processLine(String newLine, balancingData bd){
            String[] log_fields;
            log_fields=newLine.split(" ");
            String[] URL = log_fields[5].split("\\?");
            String SID, method, FQDN, HTTPCode, RequestSize, RequestResponseTime;
            SID = log_fields[14].toUpperCase();
            method = log_fields[4];
            FQDN = log_fields[11];
            HTTPCode = log_fields[7];
            RequestSize = log_fields[9];
            RequestResponseTime = log_fields[10];
            //printing response code count for the FQDN
            System.out.println("name=Custom Metrics|"+
                    MetricRootProperty+
                    "|General Metrics|"+
                    FQDN+" - "+SID+
                    "|HTTP Response codes|"+
                    method.toUpperCase()+"|"+
                    HTTPCode+" Count,aggregator=AVERAGE,value=1");
            System.out.println("name=Custom Metrics|"+
                    MetricRootProperty+
                    "|General Metrics|"+
                    FQDN+" - "+SID+
                    "|HTTP Response codes|All Methods|"+
                    HTTPCode+" Count,aggregator=AVERAGE,value=1");
            //printing reponse times for the FQDN
            System.out.println("name=Custom Metrics|"+
                    MetricRootProperty+
                    "|General Metrics|"+
                    FQDN+" - "+SID+"|"+
                    method.toUpperCase()+" Response time,aggregator=AVERAGE,value="+RequestResponseTime.replace("ms", ""));
            System.out.println("name=Custom Metrics|"+
                    MetricRootProperty+
                    "|General Metrics|"+
                    FQDN+" - "+SID+
                    "|All Methods Response time,aggregator=AVERAGE,value="+RequestResponseTime.replace("ms", ""));
            //printing request size for the FQDN
            System.out.println("name=Custom Metrics|"+
                    MetricRootProperty+
                    "|General Metrics|"+
                    FQDN+" - "+SID+
                    "|"+method.toUpperCase()+" Request size,aggregator=AVERAGE,value="+RequestSize);
            System.out.println("name=Custom Metrics|"+
                    MetricRootProperty+
                    "|General Metrics|"+
                    FQDN+" - "+SID+
                    "|All Methods Request size,aggregator=AVERAGE,value="+RequestSize);
            
            //printing response code for each URL request
            System.out.println("name=Custom Metrics|"+
                    MetricRootProperty+
                    "|General Metrics|"+
                    FQDN+" - "+SID+
                    "|Requests|"+URL[0]+
                    "|HTTP Response codes|"+method.toUpperCase()+"|"+HTTPCode+" Count,aggregator=AVERAGE,value=1");
            System.out.println("name=Custom Metrics|"+
                    MetricRootProperty+
                    "|General Metrics|"+
                    FQDN+" - "+SID+
                    "|Requests|"+URL[0]+
                    "|HTTP Response codes|All Methods|"+HTTPCode+" Count,aggregator=AVERAGE,value=1");            
            //printing requestsize for each URL request
            System.out.println("name=Custom Metrics|"+
                    MetricRootProperty+
                    "|General Metrics|"+
                    FQDN+" - "+SID+
                    "|Requests|"+URL[0]+
                    "|"+method.toUpperCase()+" Request size,aggregator=AVERAGE,value="+RequestSize);
            System.out.println("name=Custom Metrics|"+
                    MetricRootProperty+
                    "|General Metrics|"+
                    FQDN+" - "+SID+
                    "|Requests|"+URL[0]+
                    "|All Methods Request size,aggregator=AVERAGE,value="+RequestSize);
            //printing response time for each URL request
            System.out.println("name=Custom Metrics|"+
                    MetricRootProperty+
                    "|General Metrics|"+
                    FQDN+" - "+SID+
                    "|Requests|"+URL[0]+
                    "|"+method.toUpperCase()+" Response time,aggregator=AVERAGE,value="+RequestResponseTime.replace("ms", ""));
            System.out.println("name=Custom Metrics|"+
                    MetricRootProperty+
                    "|General Metrics|"+
                    FQDN+" - "+SID+
                    "|Requests|"+URL[0]+
                    "|All Methods Response time,aggregator=AVERAGE,value="+RequestResponseTime.replace("ms", ""));
        }
        
        private void processLine(String newLine, balancingData bd, Connection h2con){
            String[] log_fields;
            log_fields=newLine.toLowerCase().split(" ");
            String[] URL = log_fields[5].split("\\?");
            String SID, method, FQDN, HTTPCode, RequestSize, RequestResponseTime;
            SID = log_fields[14].toUpperCase();
            method = log_fields[4];
            FQDN = log_fields[11];
            HTTPCode = log_fields[7];
            RequestSize = log_fields[9];
            RequestResponseTime = log_fields[10];
            //printing response code count for the FQDN
            System.out.println("name=Custom Metrics|"+
                    MetricRootProperty+
                    "|General Metrics|"+
                    FQDN+" - "+SID+
                    "|HTTP Response codes|"+
                    method.toUpperCase()+"|"+
                    HTTPCode+" Count,aggregator=AVERAGE,value=1");
            System.out.println("name=Custom Metrics|"+
                    MetricRootProperty+
                    "|General Metrics|"+
                    FQDN+" - "+SID+
                    "|HTTP Response codes|All Methods|"+
                    HTTPCode+" Count,aggregator=AVERAGE,value=1");
            //printing reponse times for the FQDN
            System.out.println("name=Custom Metrics|"+
                    MetricRootProperty+
                    "|General Metrics|"+
                    FQDN+" - "+SID+"|"+
                    method.toUpperCase()+" Response time,aggregator=AVERAGE,value="+RequestResponseTime.replace("ms", ""));
            System.out.println("name=Custom Metrics|"+
                    MetricRootProperty+
                    "|General Metrics|"+
                    FQDN+" - "+SID+
                    "|All Methods Response time,aggregator=AVERAGE,value="+RequestResponseTime.replace("ms", ""));
            //printing request size for the FQDN
            System.out.println("name=Custom Metrics|"+
                    MetricRootProperty+
                    "|General Metrics|"+
                    FQDN+" - "+SID+
                    "|"+method.toUpperCase()+" Request size,aggregator=AVERAGE,value="+RequestSize);
            System.out.println("name=Custom Metrics|"+
                    MetricRootProperty+
                    "|General Metrics|"+
                    FQDN+" - "+SID+
                    "|All Methods Request size,aggregator=AVERAGE,value="+RequestSize);
            
            //printing response code for each URL request
            System.out.println("name=Custom Metrics|"+
                    MetricRootProperty+
                    "|General Metrics|"+
                    FQDN+" - "+SID+
                    "|Requests|"+URL[0]+
                    "|HTTP Response codes|"+method.toUpperCase()+"|"+HTTPCode+" Count,aggregator=AVERAGE,value=1");
            System.out.println("name=Custom Metrics|"+
                    MetricRootProperty+
                    "|General Metrics|"+
                    FQDN+" - "+SID+
                    "|Requests|"+URL[0]+
                    "|HTTP Response codes|All Methods|"+HTTPCode+" Count,aggregator=AVERAGE,value=1");            
            //printing requestsize for each URL request
            System.out.println("name=Custom Metrics|"+
                    MetricRootProperty+
                    "|General Metrics|"+
                    FQDN+" - "+SID+
                    "|Requests|"+URL[0]+
                    "|"+method.toUpperCase()+" Request size,aggregator=AVERAGE,value="+RequestSize);
            System.out.println("name=Custom Metrics|"+
                    MetricRootProperty+
                    "|General Metrics|"+
                    FQDN+" - "+SID+
                    "|Requests|"+URL[0]+
                    "|All Methods Request size,aggregator=AVERAGE,value="+RequestSize);
            //printing response time for each URL request
            System.out.println("name=Custom Metrics|"+
                    MetricRootProperty+
                    "|General Metrics|"+
                    FQDN+" - "+SID+
                    "|Requests|"+URL[0]+
                    "|"+method.toUpperCase()+" Response time,aggregator=AVERAGE,value="+RequestResponseTime.replace("ms", ""));
            System.out.println("name=Custom Metrics|"+
                    MetricRootProperty+
                    "|General Metrics|"+
                    FQDN+" - "+SID+
                    "|Requests|"+URL[0]+
                    "|All Methods Response time,aggregator=AVERAGE,value="+RequestResponseTime.replace("ms", ""));
                try {
                    Statement stm = h2con.createStatement();
                    try {
                        String query="select * from distribution where method ='"+log_fields[4]+"' and url = '"+log_fields[11]+"' and target_host = '"+log_fields[15]+"'";
                    }
                    catch (StringIndexOutOfBoundsException e){
                        LOGGER.log(Level.WARNING, "Error manipulating log file fields, log file format might have changed. Please fix that to have metrics properly collected: {0}", e.getMessage());
                    }
                    ResultSet rs = stm.executeQuery("select * from distribution where method ='"+log_fields[4]+"' and url = '"+log_fields[11]+"' and target_host = '"+log_fields[15]+"'");
                    if (rs.next()) {
                        int updateReqCount = rs.getInt("request_number")+1;
                        stm.execute("update distribution set request_number = "+updateReqCount+" where (method = '"+log_fields[4]+"' and url = '"+log_fields[11]+"' and target_host = '"+log_fields[15]+"')");
                        LOGGER.log(Level.INFO, "Processed line: update {0}", newLine);
                    }
                    else{
                        stm.execute("insert into distribution values('"+log_fields[4]+"','"+log_fields[11]+"','"+log_fields[15]+"',1)");
                        LOGGER.log(Level.INFO, "Processed line: insert {0}", newLine);
                    }
                }
                catch (SQLException ex){
                    LOGGER.log(Level.WARNING, "Could not insert/update the distribution table: "+ ex.getMessage());
                }
                //h2con.notify();
            //}
            //bd.addData(newLine, newLine, newLine, newLine, 0)
        }
        
        public void processFile(Path path, Long initialPosition, ConfigReader configuration, boolean rotated, balancingData bd){
            
            //System.out.println();
            //System.out.println("Process file...");
            //System.out.println("Processing file: "+path.getFileName());
            LOGGER.log(Level.INFO, "Processing file: {0}", path.getFileName());
            boolean moreLineExist;
            String newLine="";
            Long finalPosition=initialPosition;
            moreLineExist=true;
            Long i=0L;
            String[] log_fields;
            try{
                if ((rotated == true)&&(!configuration.getPropValues("read.properties").get("last_position").equals("0"))){
                    try (Stream<String> lineChecker = Files.lines(path)) {
                        newLine = lineChecker.skip(initialPosition).findFirst().get();
                        if (!newLine.equals(configuration.getPropValues("read.properties").get("last_line_contents"))){
                            return;
                        } 
                        else{
                            i=0L;
                        }
                    }
                    catch (IOException e){
                        LOGGER.log(Level.INFO, "Line does no exist on this file, this file will not be processed.");
                        return;
                    }
                }
            }
            catch (IOException f){
                LOGGER.log(Level.WARNING, "Exception while reading read.properties: {0}", f.getMessage());
            }
            while(moreLineExist) {
                    try (Stream<String> lines = Files.lines(path)) {
                            //i++;
                            newLine = lines.skip(initialPosition+i).findFirst().get();
                            i++;
                            processLine(newLine, bd);
                            
                    }
                    catch (Exception e) {
                            moreLineExist=false;
                            //System.out.println("Initial position: "+initialPosition+" Exception: "+e.getMessage());
                            LOGGER.log(Level.INFO, "Reached end of file while trying to read position: {0}", initialPosition+i+1);
                            if (rotated == true) finalPosition=0L;
                            else finalPosition=initialPosition+i;
                            i=0L;
                            try{
                                Properties writeProperties = new Properties();
                                writeProperties.setProperty("last_position", Long.toString(finalPosition));
                                writeProperties.setProperty("last_file_length", Long.toString(Files.size(path)));
                                writeProperties.setProperty("last_line_contents", newLine);
                                //System.out.println("Writing properties: "+writeProperties.toString().replace("{", "").replace("}", ""));
                                LOGGER.log(Level.INFO, "Writing read.properties: {0}", writeProperties.toString().replace("{", "").replace("}", ""));
                                configuration.setPropValues("read.properties", writeProperties);
                            }
                            catch (IOException e2){
                                //System.out.println("Unable to write properties: "+e2.getMessage());
                                LOGGER.log(Level.WARNING, "Unable to write properties: {0}", e2.getMessage());
                                
                            }
                            //return finalPosition;
                    }
            }
            //return finalPosition;
        }
        
        public void processFile(Path path, Long initialPosition, ConfigReader configuration, boolean rotated, balancingData bd, Connection h2con){

            if (rotated == true) LOGGER.log(Level.INFO, "Processing rotated file: {0}", path.getFileName());
            else LOGGER.log(Level.INFO, "Processing NON rotated file: {0}", path.getFileName());
            boolean moreLineExist;
            String newLine="";
            Long finalPosition=initialPosition;
            moreLineExist=true;
            Long i=0L;
            String[] log_fields;
            try{
                if ((rotated == true)&&(!configuration.getPropValues("read.properties").get("last_position").equals("0"))){
                    try (Stream<String> lineChecker = Files.lines(path)) {
                        /*boolean empty_line = true;
                        int empty_line_counter = 0;
                        while (empty_line){*/
                            try {
                                newLine = lineChecker.skip(initialPosition).findFirst().get();
                                //empty_line = false;
                                
                            }
                            catch (NoSuchElementException nse){
                                LOGGER.log(Level.INFO, "Found an empty line, ignoring and moving on.");
                                /*empty_line_counter++;
                                initialPosition+=empty_line_counter;
                                empty_line = true;*/
                            }
                        //}
                        if (!newLine.equals(configuration.getPropValues("read.properties").get("last_line_contents"))){
                            return;
                        } 
                        else{
                            i=0L;
                        }
                    }
                    catch (IOException e){
                        LOGGER.log(Level.INFO, "Line does no exist on this file, this file will not be processed.");
                        
                        return;
                    }
                }
            }
            catch (IOException f){
                LOGGER.log(Level.WARNING, "Exception while reading read.properties: {0}", f.getMessage());
            }
            LOGGER.log(Level.INFO, "Starting to read file at position: {0}", initialPosition+i);
            while(moreLineExist) {
                    try (Stream<String> lines = Files.lines(path)) {
                            //i++;
                            //try{
                                newLine = lines.skip(initialPosition+i+1).findFirst().get();
                            //}
                            //catch(NoSuchElementException nse){
                            //    LOGGER.log(Level.INFO, "Found an empty line, checking nexy line");
                            //}
                            i++;
                            processLine(newLine, bd, h2con);
                            
                    }
                    catch (Exception e) {
                            moreLineExist=false;
                            LOGGER.log(Level.INFO, "Reached end of file while trying to read position: {0}", initialPosition+i+1);
                            if (rotated == true) finalPosition=0L;
                            else finalPosition=initialPosition+i;
                            i=0L;
                            try{
                                Properties writeProperties = new Properties();
                                writeProperties.setProperty("last_position", Long.toString(finalPosition));
                                writeProperties.setProperty("last_file_length", Long.toString(Files.size(path)));
                                writeProperties.setProperty("last_line_contents", newLine);
                                //System.out.println("Writing properties: "+writeProperties.toString().replace("{", "").replace("}", ""));
                                LOGGER.log(Level.INFO, "Writing read.properties: {0}", writeProperties.toString().replace("{", "").replace("}", ""));
                                configuration.setPropValues("read.properties", writeProperties);
                            }
                            catch (IOException e2){
                                //System.out.println("Unable to write properties: "+e2.getMessage());
                                LOGGER.log(Level.WARNING, "Unable to write read.properties: {0}", e2.getMessage());
                                
                            }
                            //return finalPosition;
                    }
            }
            //return finalPosition;
        }
        
        public String GetMostRecentLogFile(String log_file_location, String log_file_name){
            try (Stream<Path> selectMostRecentLogWalk = Files.walk(Paths.get(log_file_location))) {
                                            List<String> selectMostRecentLogResult;
                                            
                                            LOGGER.log(Level.INFO, "Search file filter: {0}", log_file_name.replace(".log", ""));
                                            selectMostRecentLogResult = selectMostRecentLogWalk
                                                    .filter(Files::isRegularFile)
                                                    .filter(p->(p.getFileName().toString().startsWith(log_file_name.replace(".log", "")) && (p.getFileName().toString().contains(".log"))))
                                            
                                                    //.filter(p -> p.getFileName().startsWith(most_recent_log_file_name.replace(".log", "")))
                                                    .map(x -> x.toString()).collect(Collectors.toList());
                                            selectMostRecentLogResult.sort(new Comparator<String>(){//sorting the stream by file modified time from newest to oldest
                                                @Override
                                                public int compare(String f1, String f2){
                                                    try {
                                                        return Files.getLastModifiedTime(Paths.get(f2)).compareTo(Files.getLastModifiedTime(Paths.get(f1)));//from newest to oldest
                                                    }
                                                    catch (IOException e){
                                                        //System.out.println("There was an error while sorting files, file list will not be sorted: "+e.getMessage());
                                                        LOGGER.log(Level.WARNING, "There was an error while sorting files, file list will not be sorted: {0}", e.getMessage());
                                                        return 0;
                                                    }
                                                }
                                            });
                                            
                                            int k=0;
                                            AtomicReference<String> temp_log_file_name = new AtomicReference();
                                            temp_log_file_name.set("");
                                            
                                            if (selectMostRecentLogResult.size()>0) temp_log_file_name.set(selectMostRecentLogResult.get(0));//return selectMostRecentLogResult.get(0);
                                            
                                             String result = temp_log_file_name.get();
                                             result = result.replace(log_file_location+"/", "");
                                             return result;
                                            
                        }
                        catch (IOException ll){
                            LOGGER.log(Level.WARNING, "There was an error reading the list of log files: "+ll.getMessage());
                            return "";
                        }
        }
        
	public void ReadFile(int readIntervalSeconds, String log_file_location, String log_file_name, balancingData bd) {
		ConfigReader configuration = new ConfigReader();
               // setUpLogger();
                
                if (clearReadProperties(configuration)) LOGGER.log(Level.WARNING, "Brand new execution, clearing read.properties");
                else LOGGER.log(Level.WARNING, "There was a problem clearing the read.properties file, please check that and restart the machine agent.");
		try {
			int iterations = 0;
			Long previousFileSize = 0L;
			Properties configProperties;
			long fileLength;
			String log_line = null;
			Long finalPosition, initialPosition; 
			initialPosition = 0L;
			finalPosition = 0L;
                        String most_recent_log_file_name=log_file_name;
                        
                        //getting most recent file name based on the file name patter
                        //Files.getLastModifiedTime(Paths.get(log_file_location)).
                        //DateFormat df= new SimpleDateFormat("dd");
                        String single_file_name;
                        do {
                            single_file_name=GetMostRecentLogFile(log_file_location, log_file_name);
                            if (single_file_name.equals("")) {
                                LOGGER.log(Level.WARNING, "There is no log file present. Extension will keep waiting for a log file. Please check the log generation or the configuration and restart the machine agent if necessary.");
                                //System.out.println("There is no log file present. Extension will not work properly. Please check the configuration and restart the machine agent.");
                                System.out.println("name=Custom Metrics|"+MetricRootProperty+"|log_file_not_present,aggregator=AVERAGE,value=1");
                                Thread.sleep(1000);
                            }
                            else{
                                LOGGER.log(Level.INFO, "Log file location: "+log_file_location+"/"+single_file_name);
                                System.out.println("name=Custom Metrics|"+MetricRootProperty+"|log_file_not_present,aggregator=AVERAGE,value=0");
                            }
                        } while (single_file_name.equals(""));
                        log_file_name = log_file_location+"/"+single_file_name;
			
			while (true) {
				//System.out.println("New loo: "+iterations);
                                 LOGGER.log(Level.INFO, "New file read loop: {0}", iterations);
				configProperties = configuration.getPropValues("read.properties");
				String position = configProperties.getProperty("last_position");
                                 String lastLineContents = configProperties.getProperty("last_line_contents");
				previousFileSize = Long.parseLong(configProperties.getProperty("last_file_length"));
				//System.out.println("Position: "+ position);
                                LOGGER.log(Level.INFO, "Position: {0}", position);
				File log_file = new File(log_file_name);
				fileLength  = log_file.length();
				//checking with have already read the file. If we haven't on the first interation of the loop all available lines should be read.
				RandomAccessFile readWriteFileAccess = new RandomAccessFile(log_file_name, "rw");
				
				initialPosition = Long.parseLong(position);
				Path path = Paths.get(log_file_name);
				Long fileSize = Files.size(path);
                            
				if (fileSize<previousFileSize) {//file has rotated, we need to read the previous file before reading the new one.
                                    final Long currentPosition = initialPosition;
                                        LOGGER.log(Level.INFO, "Parent from log file: {0}", log_file.getParent());
                                        try (Stream<Path> walk = Files.walk(Paths.get(log_file.getParent()))) {
                                            List<String> result;
                                            result = walk.filter(Files::isRegularFile)
                                                    .map(x -> x.toString()).collect(Collectors.toList());
                                            result.sort(new Comparator<String>(){//sorting the stream by file modified time from newest to oldest
                                                @Override
                                                public int compare(String f1, String f2){
                                                    try {
                                                        return Files.getLastModifiedTime(Paths.get(f1)).compareTo(Files.getLastModifiedTime(Paths.get(f2)));//from newest to oldest
                                                    }
                                                    catch (IOException e){
                                                        //System.out.println("There was an error while sorting files, file list will not be sorted: "+e.getMessage());
                                                        LOGGER.log(Level.WARNING, "There was an error while sorting files, file list will not be sorted: {0}", e.getMessage());
                                                        return 0;
                                                    }
                                                }
                                            });
                                            
                                                result.forEach(i->{
                                                    int index = i.lastIndexOf(File.separator);
                                                    String relative_file_name = i.substring(index+1);
                                                    if ((relative_file_name.startsWith(log_file.getName().replace(".log", "")))&&(relative_file_name.contains(".log"))){
                                                    LOGGER.log(Level.INFO, "Rotated file to process: {0}", relative_file_name);
                                                        LOGGER.log(Level.INFO, "Complete path to rotated file to process: {0}", i);
                                                        try {
                                                            long time_difference = Files.getLastModifiedTime(Paths.get(i)).toMillis()-System.currentTimeMillis();
                                                            if (time_difference<=1500){//file is within time range, lets read an see we find the line stored
                                                              //System.out.println("We found a file: "+i+"FileTime: "+ Files.getLastModifiedTime(Paths.get(i)).toString());
                                                              LOGGER.log(Level.INFO, "Found a file to process {0}: ", i);
                                                              //result.
                                                              
                                                                processFile(Paths.get(i), currentPosition, configuration, true, bd);
                                                            }
                                                        }
                                                        catch (IOException e){
                                                            //System.out.println("Error reading rotated files: "+e.getMessage());
                                                            LOGGER.log(Level.WARNING, "Error reading rotated files: ", e.getMessage());
                                                        }
                                                        
                                                    }
                                                    
                                                        });
                                         } catch (IOException e) {
                                             LOGGER.log(Level.WARNING, "Error while walking through files: ", e.getMessage());
                                             e.printStackTrace();
                                         }
				}
				else {
					processFile(path, initialPosition, configuration, false, bd);
				}
				readWriteFileAccess.close();
				Thread.sleep(readIntervalSeconds*1000);
				iterations++;
			}
		}
		catch (Exception e) {
			//System.out.println("There was an error reading the file: "+e.getMessage());
                        LOGGER.log(Level.WARNING, "There was an error reading the file: ", e.getMessage());
			e.printStackTrace();
		}
	}
        
        public void ReadFile(int readIntervalSeconds, String log_file_location, String log_file_name, balancingData bd, Connection h2con) {
		ConfigReader configuration = new ConfigReader();
                //setUpLogger();
                if (clearReadProperties(configuration)) LOGGER.log(Level.WARNING, "Brand new execution, clearing read.properties");
                else LOGGER.log(Level.WARNING, "There was a problem clearing the read.properties file, please check that and restart the machine agent.");
		try {
			int iterations = 0;
			Long previousFileSize = 0L;
			Properties configProperties;
			long fileLength;
			String log_line = null;
			Long finalPosition, initialPosition; 
			initialPosition = 0L;
			finalPosition = 0L;
                        String most_recent_log_file_name=log_file_name;
                        
                        //getting most recent file name based on the file name patter
                        //Files.getLastModifiedTime(Paths.get(log_file_location)).
                        //DateFormat df= new SimpleDateFormat("dd");
                        String single_file_name;
                        do {
                            single_file_name=GetMostRecentLogFile(log_file_location, log_file_name);
                            if (single_file_name.equals("")) {
                                LOGGER.log(Level.WARNING, "There is no log file present. Extension will keep waiting for a log file. Please check the log generation or the configuration and restart the machine agent if necessary.");
                                //System.out.println("There is no log file present. Extension will not work properly. Please check the configuration and restart the machine agent.");
                                System.out.println("name=Custom Metrics|"+MetricRootProperty+"|log_file_not_present,aggregator=AVERAGE,value=1");
                                Thread.sleep(1000);
                            }
                            else{
                                LOGGER.log(Level.INFO, "Log file location: "+log_file_location+"/"+single_file_name);
                                System.out.println("name=Custom Metrics|"+MetricRootProperty+"|log_file_not_present,aggregator=AVERAGE,value=0");
                            }
                        } while (single_file_name.equals(""));
                        log_file_name = log_file_location+"/"+single_file_name;
			
			while (true) {
				//System.out.println("New loo: "+iterations);
                                 LOGGER.log(Level.INFO, "New file read loop: {0}", iterations);
				configProperties = configuration.getPropValues("read.properties");
				String position = configProperties.getProperty("last_position");
                                 String lastLineContents = configProperties.getProperty("last_line_contents");
				previousFileSize = Long.parseLong(configProperties.getProperty("last_file_length"));
				//System.out.println("Position: "+ position);
                                LOGGER.log(Level.INFO, "Position: {0}", position);
				File log_file = new File(log_file_name);
				fileLength  = log_file.length();
				//checking with have already read the file. If we haven't on the first interation of the loop all available lines should be read.
				RandomAccessFile readWriteFileAccess = new RandomAccessFile(log_file_name, "rw");
				
				initialPosition = Long.parseLong(position);
				Path path = Paths.get(log_file_name);
				Long fileSize = Files.size(path);
                            
				if (fileSize<previousFileSize) {//file has rotated, we need to read the previous file before reading the new one.
                                    final Long currentPosition = initialPosition;
                                        //System.out.println("Todo###############################################################...");
                                        //System.out.println(log_file.getParent());
                                        LOGGER.log(Level.INFO, "Parent from log file: {0}", log_file.getParent());
                                        try (Stream<Path> walk = Files.walk(Paths.get(log_file.getParent()))) {
                                            List<String> result;
                                            result = walk.filter(Files::isRegularFile)
                                                    .map(x -> x.toString()).collect(Collectors.toList());
                                            result.sort(new Comparator<String>(){//sorting the stream by file modified time from newest to oldest
                                                @Override
                                                public int compare(String f1, String f2){
                                                    try {
                                                        return Files.getLastModifiedTime(Paths.get(f1)).compareTo(Files.getLastModifiedTime(Paths.get(f2)));//from newest to oldest
                                                    }
                                                    catch (IOException e){
                                                        //System.out.println("There was an error while sorting files, file list will not be sorted: "+e.getMessage());
                                                        LOGGER.log(Level.WARNING, "There was an error while sorting files, file list will not be sorted: {0}", e.getMessage());
                                                        return 0;
                                                    }
                                                }
                                            });
                                            
                                                result.forEach(i->{
                                                    int index = i.lastIndexOf(File.separator);
                                                    String relative_file_name = i.substring(index+1);
                                                    if ((relative_file_name.startsWith(log_file.getName().replace(".log", "")))&&(relative_file_name.contains(".log"))){
                                                    LOGGER.log(Level.INFO, "Rotated file to process: {0}", relative_file_name);
                                                    LOGGER.log(Level.INFO, "Complete path to rotated file to process: {0}", i);
                                                        try {
                                                            long time_difference = Files.getLastModifiedTime(Paths.get(i)).toMillis()-System.currentTimeMillis();
                                                            if (time_difference<=1500){//file is within time range, lets read an see we find the line stored
                                                              LOGGER.log(Level.INFO, "Found a file to process {0}: ", i);
                                                                synchronized(h2con){
                                                                    processFile(Paths.get(i), currentPosition, configuration, true, bd, h2con);
                                                                    h2con.notify();
                                                                }
                                                            }
                                                        }
                                                        catch (IOException e){
                                                            LOGGER.log(Level.WARNING, "Error reading rotated files: ", e.getMessage());
                                                        }
                                                        
                                                    }
                                                    
                                                        });
                                         } catch (IOException e) {
                                             LOGGER.log(Level.WARNING, "Error while walking through files: ", e.getMessage());
                                             e.printStackTrace();
                                         }
				}
				else {
					synchronized(h2con){
                                            processFile(path, initialPosition, configuration, false, bd, h2con);
                                            h2con.notify();
                                        }
				}
				readWriteFileAccess.close();
				Thread.sleep(readIntervalSeconds*1000);
				iterations++;
			}
		}
		catch (Exception e) {
			//System.out.println("There was an error reading the file: "+e.getMessage());
                        LOGGER.log(Level.WARNING, "There was an error reading the file: ", e.getMessage());
			e.printStackTrace();
		}
	}
}
