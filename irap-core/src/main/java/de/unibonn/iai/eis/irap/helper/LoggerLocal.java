package de.unibonn.iai.eis.irap.helper;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


public class LoggerLocal {
	private static boolean configured = false; 
	
	
	public static Logger getLogger(String name) {
		
		if(!configured) {
			PropertyConfigurator.configure("log4j.properties");
			configured = true;
		}
		return Logger.getLogger(name);
	}
}
