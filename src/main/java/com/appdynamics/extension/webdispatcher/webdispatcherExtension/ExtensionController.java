package com.appdynamics.extension.webdispatcher.webdispatcherExtension;
import java.io.IOException;
import java.util.*;
import java.lang.Math;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
//import java.util.ArrayList;

/**
 * Monitors WebDispatcher logs and reports metrics to controller
 *
 */

class CoreActivities extends Thread implements Runnable 
{ 
    balancingData CoreActivities_bd;
    Connection h2connection;
    String test_value;
    Logger LOGGER;
	public CoreActivities(balancingData bd, Connection h2con) {
		CoreActivities_bd = bd;
                h2connection = h2con;
	}
        
        public CoreActivities(balancingData bd) {
		CoreActivities_bd = bd;
	}
	
	public void run() { 
            //System.out.println("This is a test: "+test_value);
            ConfigReader configuration = new ConfigReader();
            WebDispatcherLogRead WDLogRead = new WebDispatcherLogRead();
            try {
                    Properties configProperties = configuration.getPropValues("config.properties");
                    String fileName = configProperties.getProperty("file_pattern").replace("*", "");
                    //System.out.println(configProperties.getProperty("file_location")+"/"+fileName);
                    if (h2connection.isValid(0)) WDLogRead.ReadFile(Integer.parseInt(configProperties.getProperty("read_interval_seconds")), configProperties.getProperty("file_location"),fileName, CoreActivities_bd, h2connection);
                    else WDLogRead.ReadFile(Integer.parseInt(configProperties.getProperty("read_interval_seconds")), configProperties.getProperty("file_location"),fileName, CoreActivities_bd);
                    
            }
            catch (Exception e) {
                    LOGGER.log(Level.WARNING, "Could not read configuration file: {0}", e.getMessage());
                    e.printStackTrace();
            }
        } 
}

class DistributionCoeficient extends Thread 
{
	balancingData DistributionCoeficient_bd;
        Connection h2connection;
        private static Logger LOGGER;
	Dictionary<String, String> URL_total_reqs;
	Dictionary<String, String> URL_host_total_reqs;
	public DistributionCoeficient(balancingData bd, Connection h2con) {
            DistributionCoeficient_bd = bd;
            h2connection = h2con;
            LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
        }
	
        public DistributionCoeficient(balancingData bd) {
            DistributionCoeficient_bd = bd;
            LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
        }
	
    public void run() 
    { 
    	Dictionary<String, String> getDataResult;
    	Dictionary<String, String> URLTotals;
    	Dictionary<String, String> URLHostTotals;
    	Dictionary<String, String> FinalDistribution;
        Connection newH2connection;
    	try {
	        while (true) {
                LOGGER.log(Level.WARNING, "Starting loop will wait 1 min before calculating balance distribution...");
	        	Thread.sleep(60000);
                    
                    try{
                        synchronized (h2connection){
                        h2connection.wait();
                        Statement stm = h2connection.createStatement();
                                ResultSet rs_grand_total = stm.executeQuery("select method, url, sum(request_number) from distribution group by method, url");
                                //getting host quantity
                                int host_quantity = 0;
                                while (rs_grand_total.next()){

                                    Statement stm2 = h2connection.createStatement();
                                    String method = rs_grand_total.getString(1);
                                    String url = rs_grand_total.getString(2);
                                    long request_count = rs_grand_total.getLong(3);

                                    String query = "select count(target_host) from distribution where method = '"+ method +"' and url = '"+ url +"'";
                                    ResultSet rs_host_quantity = stm2.executeQuery(query);
                                    rs_host_quantity.next();
                                    host_quantity = rs_host_quantity.getInt(1);

                                    query = "select method, url, target_host, request_number from distribution where method = '"+ method +"' and url = '"+ url +"'";
                                    ResultSet rs_total_by_host = stm2.executeQuery(query);

                                    long expectedValue = request_count/host_quantity;
                                    float distributionCoeficient=0;        
                                    while (rs_total_by_host.next()){
                                        distributionCoeficient+=Math.abs(1-rs_total_by_host.getInt(4)/expectedValue);
                                    }

                                    System.out.println("name=Custom Metrics|WebDispatcher|Load Distribution|"+
                                            url+"|"+
                                            method+
                                            "|Distribution coeficient,aggregator=AVERAGE,value="+
                                            (distributionCoeficient/host_quantity)*100);
                                 }
                                stm.execute("delete from distribution");
                        h2connection.notify();
                        }
                    }
                    catch(SQLException ex){
                        LOGGER.log(Level.WARNING, "Connection to h2 no possible: {0}", ex.getMessage());
                        ex.printStackTrace();
                    }
	        }
    	}
    	catch (Exception e) {
                LOGGER.log(Level.WARNING, "Error on thread for calculating distribution statistics: {0}", e.getMessage());
    	}
    }
}


public class ExtensionController 
{
    final private static Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    //final static Logger TEST_LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
        
    private static void setUpLogger(){
        try{
            FileHandler fileTxt = new FileHandler("webDispatcher.log", 10000000, 5, true);
            fileTxt.setFormatter(new SimpleFormatter());
            LOGGER.addHandler(fileTxt);
        } 
        catch (IOException e){
            LOGGER.log(Level.WARNING, "Unable to write log: {0}", e.getMessage());
        }
    }
    
    public static void main( String[] args )
    {
    	balancingData balancingDataInstance = new balancingData();
        Connection con;
    	
        String url = "jdbc:h2:mem:distribution_coeficient";
        
        setUpLogger();

        try {
            con = DriverManager.getConnection(url);
            Statement stm = con.createStatement();
            stm.execute("create table distribution (method varchar(10) not null, url varchar(255) not null, target_host varchar(255) not null, request_number int not null)");
            
            Runnable CoreActivitiesRunnable = new CoreActivities(balancingDataInstance, con);
            new Thread(CoreActivitiesRunnable).start();
            
            Runnable DistributionCoeficient = new DistributionCoeficient(balancingDataInstance, con);
            new Thread(DistributionCoeficient).start();

        } catch (SQLException ex) {

            //lgr = Logger.getLogger(JavaSeH2Memory.class.getName());
            //lgr.log(Level.SEVERE, ex.getMessage(), ex);
            LOGGER.log(Level.WARNING, "H2 Database could not be initialized, balance distribution will not be possible: ", ex.getMessage());
            
            Runnable CoreActivitiesRunnable = new CoreActivities(balancingDataInstance);
            new Thread(CoreActivitiesRunnable).start();
            
            Runnable DistributionCoeficient = new DistributionCoeficient(balancingDataInstance);
            new Thread(DistributionCoeficient).start();
        }
    }
}
