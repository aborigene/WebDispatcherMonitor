package com.appdynamics.extension.webdispatcher.webdispatcherExtension;
import java.util.*;

public class balancingData {

	Dictionary<String, String> distribution = new Hashtable<>();
	
	public int addData(String method, String FQDN, String URL, String target_host, int number_of_requests) {
		System.out.println("Keys: "+method+"_"+FQDN+"_"+URL+"_"+target_host);
                String keys=method+"_"+FQDN+"_"+URL+"_"+target_host;
		String recordValue = null;
		try {
			recordValue = distribution.get(keys);//searchData(method, FQDN, URL, target_host);
			System.out.println("recordValue: "+ recordValue);
		}
		catch (Exception e) {
			System.out.println("There was an exception while retrieving value from dictionary. recordValue does not exist on the Dictionary, execution will continue normally");
		}
		/*if (!distribution.isEmpty()) {
			System.out.println("Size: "+distribution.size());
			System.out.println("RecordValue BEFORE inserting into var: "+distribution.get(method+"_"+FQDN+"_"+URL+"_"+target_host));//searchData(method, FQDN, URL, target_host));
			recordValue = distribution.get(method+"_"+FQDN+"_"+URL+"_"+target_host).toString();//searchData(method, FQDN, URL, target_host);
			System.out.println("RecordValue AFTER inserting into var: "+recordValue);
		}
		else System.out.println("recordValue is empty...");*/
		int returnStatus = 0;
		synchronized (this) {
			if (recordValue!=null) {//pair found lets update the value on the requests
				distribution.put(keys, Integer.toString(Integer.parseInt(distribution.get(method+"_"+FQDN+"_"+URL+"_"+target_host))+number_of_requests));
				returnStatus = 1;
			}
			else {//pair not found, we'll have to create it
				distribution.put(method+"_"+FQDN+"_"+URL+"_"+target_host, Integer.toString(number_of_requests));
				returnStatus = 2;
			}
		}
		return returnStatus;
	}
	
	public Dictionary<String, String> getData() {
		Dictionary<String, String> resultList = new Hashtable<String, String>();
		synchronized (this){
			resultList = distribution;
			for (Enumeration<String> k = distribution.keys(); k.hasMoreElements();) 
	        { 
                    String nextElement=k.nextElement();
	            distribution.put(nextElement, "");
                    System.out.println();
                    System.out.println();
                    System.out.println("Keys in Dictionary : " + nextElement); 
	        } 
		}
		 
		return resultList;
	}

}
