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
import java.util.logging.Level;
import java.util.logging.Logger;

public class WebDispatcherLogRead{
	
        Logger LOGGER;// = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);  
        
        private void setUpLogger(){
            try{
                FileHandler fileTxt = new FileHandler("webDispatcher.log", 1000000, 5, true);
                fileTxt.setFormatter(new SimpleFormatter());
                LOGGER.addHandler(fileTxt);
            } 
            catch (IOException e){
                LOGGER.log(Level.WARNING, "Unable to write log: {0}", e.getMessage());
            }
        }
        
        public WebDispatcherLogRead(){
            LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
        }
        
        private boolean clearReadProperties(ConfigReader configuration){
            try{
                Properties writeProperties = new Properties();
                writeProperties.setProperty("last_position", "0");
                writeProperties.setProperty("last_file_length", "0");
                writeProperties.setProperty("last_line_contents", "");
                //System.out.println("Writing properties: "+writeProperties.toString().replace("{", "").replace("}", ""));
                LOGGER.log(Level.WARNING, "Writing properties: {0}", writeProperties.toString().replace("{", "").replace("}", ""));
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
            System.out.println("Processed line: "+newLine);
            System.out.println("name=Custom Metrics|WebDispatcher|"+log_fields[14]+"|"+log_fields[4]+"|"+log_fields[11]+"|HTTP Response codes|"+log_fields[7]+",aggregator=AVERAGE,value=1");
            System.out.println("name=Custom Metrics|WebDispatcher|"+log_fields[14]+"|"+log_fields[4]+"|"+URL[0]+"|HTTP Response code count|"+log_fields[7]+",aggregator=AVERAGE,value=1");
            System.out.println("name=Custom Metrics|WebDispatcher|"+log_fields[14]+"|"+log_fields[4]+"|"+log_fields[11]+"|Response time,aggregator=AVERAGE,value="+log_fields[10].replace("ms", ""));
            System.out.println("name=Custom Metrics|WebDispatcher|"+log_fields[14]+"|"+log_fields[4]+"|"+URL[0]+"|Response time,aggregator=AVERAGE,value="+log_fields[10].replace("ms", ""));
            System.out.println("name=Custom Metrics|WebDispatcher|"+log_fields[14]+"|"+log_fields[4]+"|"+log_fields[11]+"|Request size,aggregator=AVERAGE,value="+log_fields[9]);
            System.out.println("name=Custom Metrics|WebDispatcher|"+log_fields[14]+"|"+log_fields[4]+"|"+URL[0]+"|Request size,aggregator=AVERAGE,value="+log_fields[9]);
        }
        
        private void processLine(String newLine, balancingData bd, Connection h2con){
            String[] log_fields;
            log_fields=newLine.toLowerCase().split(" ");
            String[] URL = log_fields[5].split("\\?");
            //System.out.println("Processed line: "+newLine);
            System.out.println("name=Custom Metrics|WebDispatcher|"+log_fields[14]+"|"+log_fields[4]+"|"+log_fields[11]+"|HTTP Response codes|"+log_fields[7]+",aggregator=AVERAGE,value=1");
            System.out.println("name=Custom Metrics|WebDispatcher|"+log_fields[14]+"|"+log_fields[4]+"|"+URL[0]+"|HTTP Response code count|"+log_fields[7]+",aggregator=AVERAGE,value=1");
            System.out.println("name=Custom Metrics|WebDispatcher|"+log_fields[14]+"|"+log_fields[4]+"|"+log_fields[11]+"|Response time,aggregator=AVERAGE,value="+log_fields[10].replace("ms", ""));
            System.out.println("name=Custom Metrics|WebDispatcher|"+log_fields[14]+"|"+log_fields[4]+"|"+URL[0]+"|Response time,aggregator=AVERAGE,value="+log_fields[10].replace("ms", ""));
            System.out.println("name=Custom Metrics|WebDispatcher|"+log_fields[14]+"|"+log_fields[4]+"|"+log_fields[11]+"|Request size,aggregator=AVERAGE,value="+log_fields[9]);
            System.out.println("name=Custom Metrics|WebDispatcher|"+log_fields[14]+"|"+log_fields[4]+"|"+URL[0]+"|Request size,aggregator=AVERAGE,value="+log_fields[9]);
                try {
                    Statement stm = h2con.createStatement();
                    try {
                        String query="select * from distribution where method ='"+log_fields[4]+"' and url = '"+log_fields[11]+"' and target_host = '"+log_fields[15]+"'";
                    }
                    catch (StringIndexOutOfBoundsException e){
                        LOGGER.log(Level.SEVERE, "Error manipulating log file fields, log file format might have changed. Please fix that to have metrics properly collected: {0}", e.getMessage());
                    }
                    ResultSet rs = stm.executeQuery("select * from distribution where method ='"+log_fields[4]+"' and url = '"+log_fields[11]+"' and target_host = '"+log_fields[15]+"'");
                    if (rs.next()) {
                        int updateReqCount = rs.getInt("request_number")+1;
                        stm.execute("update distribution set request_number = "+updateReqCount+" where (method = '"+log_fields[4]+"' and url = '"+log_fields[11]+"' and target_host = '"+log_fields[15]+"')");
                        LOGGER.log(Level.WARNING, "Processed line: update {0}", newLine);
                    }
                    else{
                        stm.execute("insert into distribution values('"+log_fields[4]+"','"+log_fields[11]+"','"+log_fields[15]+"',1)");
                        LOGGER.log(Level.WARNING, "Processed line: insert {0}", newLine);
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
            LOGGER.log(Level.WARNING, "Processing file: {0}", path.getFileName());
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
                        LOGGER.log(Level.WARNING, "Line does no exist on this file, this file will not be processed.");
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
                            LOGGER.log(Level.WARNING, "Reached end of file while trying to read position: {0}", initialPosition+i+1);
                            if (rotated == true) finalPosition=0L;
                            else finalPosition=initialPosition+i;
                            i=0L;
                            try{
                                Properties writeProperties = new Properties();
                                writeProperties.setProperty("last_position", Long.toString(finalPosition));
                                writeProperties.setProperty("last_file_length", Long.toString(Files.size(path)));
                                writeProperties.setProperty("last_line_contents", newLine);
                                //System.out.println("Writing properties: "+writeProperties.toString().replace("{", "").replace("}", ""));
                                LOGGER.log(Level.WARNING, "Writing read.properties: {0}", writeProperties.toString().replace("{", "").replace("}", ""));
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

            if (rotated == true) LOGGER.log(Level.WARNING, "Processing rotated file: {0}", path.getFileName());
            else LOGGER.log(Level.WARNING, "Processing NON rotated file: {0}", path.getFileName());
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
                        LOGGER.log(Level.WARNING, "Line does no exist on this file, this file will not be processed.");
                        
                        return;
                    }
                }
            }
            catch (IOException f){
                LOGGER.log(Level.WARNING, "Exception while reading read.properties: {0}", f.getMessage());
            }
            LOGGER.log(Level.WARNING, "Starting to read file at position: {0}", initialPosition+i);
            while(moreLineExist) {
                    try (Stream<String> lines = Files.lines(path)) {
                            //i++;
                            newLine = lines.skip(initialPosition+i+1).findFirst().get();
                            i++;
                            processLine(newLine, bd, h2con);
                            
                    }
                    catch (Exception e) {
                            moreLineExist=false;
                            LOGGER.log(Level.WARNING, "Reached end of file while trying to read position: {0}", initialPosition+i+1);
                            if (rotated == true) finalPosition=0L;
                            else finalPosition=initialPosition+i;
                            i=0L;
                            try{
                                Properties writeProperties = new Properties();
                                writeProperties.setProperty("last_position", Long.toString(finalPosition));
                                writeProperties.setProperty("last_file_length", Long.toString(Files.size(path)));
                                writeProperties.setProperty("last_line_contents", newLine);
                                //System.out.println("Writing properties: "+writeProperties.toString().replace("{", "").replace("}", ""));
                                LOGGER.log(Level.WARNING, "Writing read.properties: {0}", writeProperties.toString().replace("{", "").replace("}", ""));
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
                                            
                                            LOGGER.log(Level.WARNING, "Search file filter: {0}", log_file_name.replace(".log", ""));
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
                setUpLogger();
                
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
                                LOGGER.log(Level.WARNING, "There is no log file present. Extension will not work properly. Please check the configuration and restart the machine agent.");
                                //System.out.println("There is no log file present. Extension will not work properly. Please check the configuration and restart the machine agent.");
                                System.out.println("name=Custom Metrics|WebDispatcher|log_file_not_present,aggregator=AVERAGE,value=1");
                                Thread.sleep(1000);
                            }
                            else{
                                System.out.println("name=Custom Metrics|WebDispatcher|log_file_not_present,aggregator=AVERAGE,value=0");
                            }
                        } while (single_file_name.equals(""));
                        log_file_name = log_file_location+"/"+single_file_name;
			
			while (true) {
				//System.out.println("New loo: "+iterations);
                                 LOGGER.log(Level.WARNING, "New loop: {0}", iterations);
				configProperties = configuration.getPropValues("read.properties");
				String position = configProperties.getProperty("last_position");
                                 String lastLineContents = configProperties.getProperty("last_line_contents");
				previousFileSize = Long.parseLong(configProperties.getProperty("last_file_length"));
				//System.out.println("Position: "+ position);
                                LOGGER.log(Level.WARNING, "Position: {0}", position);
				File log_file = new File(log_file_name);
				fileLength  = log_file.length();
				//checking with have already read the file. If we haven't on the first interation of the loop all available lines should be read.
				RandomAccessFile readWriteFileAccess = new RandomAccessFile(log_file_name, "rw");
				
				initialPosition = Long.parseLong(position);
				Path path = Paths.get(log_file_name);
				Long fileSize = Files.size(path);
                            
				if (fileSize<previousFileSize) {//file has rotated, we need to read the previous file before reading the new one.
                                    final Long currentPosition = initialPosition;
                                        LOGGER.log(Level.WARNING, "Parent from log file: {0}", log_file.getParent());
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
                                                    LOGGER.log(Level.WARNING, "Rotated file to process: {0}", relative_file_name);
                                                        LOGGER.log(Level.WARNING, "Complete path to rotated file to process: {0}", i);
                                                        try {
                                                            long time_difference = Files.getLastModifiedTime(Paths.get(i)).toMillis()-System.currentTimeMillis();
                                                            if (time_difference<=1500){//file is within time range, lets read an see we find the line stored
                                                              //System.out.println("We found a file: "+i+"FileTime: "+ Files.getLastModifiedTime(Paths.get(i)).toString());
                                                              LOGGER.log(Level.WARNING, "Found a file to process {0}: ", i);
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
                setUpLogger();
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
                                LOGGER.log(Level.WARNING, "There is no log file present. Extension will not work properly. Please check the configuration and restart the machine agent.");
                                //System.out.println("There is no log file present. Extension will not work properly. Please check the configuration and restart the machine agent.");
                                System.out.println("name=Custom Metrics|WebDispatcher|log_file_not_present,aggregator=AVERAGE,value=1");
                                Thread.sleep(1000);
                            }
                            else{
                                LOGGER.log(Level.WARNING, "Log file location: "+log_file_location+"/"+single_file_name);
                                System.out.println("name=Custom Metrics|WebDispatcher|log_file_not_present,aggregator=AVERAGE,value=0");
                            }
                        } while (single_file_name.equals(""));
                        log_file_name = log_file_location+"/"+single_file_name;
			
			while (true) {
				//System.out.println("New loo: "+iterations);
                                 LOGGER.log(Level.WARNING, "New loop: {0}", iterations);
				configProperties = configuration.getPropValues("read.properties");
				String position = configProperties.getProperty("last_position");
                                 String lastLineContents = configProperties.getProperty("last_line_contents");
				previousFileSize = Long.parseLong(configProperties.getProperty("last_file_length"));
				//System.out.println("Position: "+ position);
                                LOGGER.log(Level.WARNING, "Position: {0}", position);
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
                                        LOGGER.log(Level.WARNING, "Parent from log file: {0}", log_file.getParent());
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
                                                    LOGGER.log(Level.WARNING, "Rotated file to process: {0}", relative_file_name);
                                                    LOGGER.log(Level.WARNING, "Complete path to rotated file to process: {0}", i);
                                                        try {
                                                            long time_difference = Files.getLastModifiedTime(Paths.get(i)).toMillis()-System.currentTimeMillis();
                                                            if (time_difference<=1500){//file is within time range, lets read an see we find the line stored
                                                              LOGGER.log(Level.WARNING, "Found a file to process {0}: ", i);
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
