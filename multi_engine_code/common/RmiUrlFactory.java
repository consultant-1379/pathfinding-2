package com.distocraft.dc5000.common;
/**
 * @author eanguan
 */
import java.io.File;
import java.io.FileInputStream;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.distocraft.dc5000.common.ServicenamesHelper.ServiceHostDetails;
import com.distocraft.dc5000.etl.engine.main.ITransferEngineRMI;

public class RmiUrlFactory {
	private final static String defaultRMIPortS = "1200" ;
	private final static int defaultRMIPortI = 1200 ;
	private final static String defaultSchedulerRefName = "Scheduler" ;
	private final static String defaultEngineRefName = "TransferEngine" ;
	private final static String defaultLicRefName = "LicensingCache" ;
	private final static String defaultMultiESRefName = "MultiESController";
	private final static String defaultEniqHost = "localhost" ;
	private final static String engineServiceName = "engine";
	private final static String schedulerServiceName = "scheduler";
	private final static String licServiceName = "licenceservice";
	
	private static final Logger log = Logger.getLogger("com.distocraft.dc5000.etl.gui.common.RmiUrlFactory");
	private Properties appProps ;
	private List<Engine> engines = new ArrayList<>();
	private Engine lastServedEngine;
			

	/**
	 * private Constructor
	 */
	private RmiUrlFactory(){
		getStarted();
	}
	
	private static class Holder {
		static RmiUrlFactory instance = new RmiUrlFactory();
		private Holder() {
			
		}
	}
	
	/**
	 * Public method to get the singleton object of class RmiUrlFactory
	 * @return RmiUrlFactory
	 */
	public static RmiUrlFactory getInstance(){
		return Holder.instance ;
	}
	
	
	/**
	 * Method to load the properties defined in ETLC properties file
	 */
	private void getStarted() {
		String confDir = System.getProperty("dc5000.config.directory","/eniq/sw/conf/");
		if (!confDir.endsWith(File.separator)) {
			confDir += File.separator;
		}
		appProps = new Properties();
		try (FileInputStream file = new  FileInputStream(confDir + "ETLCServer.properties");){
			appProps.load(file);
			
		}catch(final Exception e){
			log.log(Level.SEVERE, "Failed to read the ETLC properties file. ", e);
		}
		initEngines();
	}
	
	private void initEngines() {
		Map<String, ServiceHostDetails> enginesFrmServiceNames = null;
		try {
			enginesFrmServiceNames = ServicenamesHelper.getEngines(log);
		} catch (final Exception e) {
			log.log(Level.WARNING, "not able to get engines from Servicenames",e);
		}
		if (enginesFrmServiceNames != null && !enginesFrmServiceNames.isEmpty()) {
			enginesFrmServiceNames.forEach((key, value) -> {
				Engine engine = new Engine(key, getEngineRmiUrl(value.getServiceHostname()));
				engines.add(engine);
			});
		} else {
			Engine engine = new Engine(defaultEniqHost, getEngineRmiUrl(defaultEniqHost));
			engines.add(engine);
		}

	}
	
	public List<String> getAllEngineRmiUrls() {
		return engines.stream().map(Engine::getUrl).collect(Collectors.toList());
	}
	
	public String getEngineRmiUrl() {
		return getByRoundRobin().getUrl();
	}
	
	public String getEngineUrlByRoundRobin() {
		return getByRoundRobin().getUrl();
	}
	
	public String getEngineUrlByFreeSlots() {
		return getByFreeSlots();
	}
	
	public String getEngineUrlByFreeSlots(String setType) {
		return getByFreeSlots(setType);
	}
	
	private String getByFreeSlots(String setType) {
		Engine freeEngine;
		if (engines.isEmpty()) {
			return "";
		}
		/*if (engines.size() == 1) {
			freeEngine = engines.get(0);
		}*/ else {
			for (Engine engine : engines) {
				try {
					ITransferEngineRMI trEngine = (ITransferEngineRMI) Naming.lookup(engine.getUrl());
					int freeSlots = trEngine.getFreeExecutionSlots(setType);
					engine.setFreeSlots(freeSlots);
					log.log(Level.INFO,"free " + setType + " slots available in :" + engine.getName()+" = "+freeSlots);
				} catch (MalformedURLException | RemoteException | NotBoundException e) {
					log.log(Level.WARNING, "Exception while connecting to engine : "+engine.getName(), e);
					engine.setFreeSlots(0);
				}
			}
			Collections.sort(engines, Collections.reverseOrder((e1,e2) -> e1.getFreeSlots() - e2.getFreeSlots()));
			log.log(Level.INFO, "order by free slots : " + engines);
			freeEngine = engines.get(0);
			lastServedEngine = freeEngine;
		}
		log.log(Level.INFO, "returning Engine : " + freeEngine.getName() + " based on freeslots available, available Engines = " + engines);
		return freeEngine.getUrl();
	}
	
	private String getByFreeSlots() {
		for (Engine engine : engines) {
			try {
				ITransferEngineRMI trEngine = (ITransferEngineRMI) Naming.lookup(engine.getUrl());
				int freeSlots = trEngine.getFreeExecutionSlots();
				engine.setFreeSlots(freeSlots);
				log.log(Level.INFO,"free slots in : "+engine.getName()+" = "+freeSlots);
			} catch (MalformedURLException | RemoteException | NotBoundException e) {
				log.log(Level.WARNING, "Exception while connecting to engine : "+engine.getName(), e);
				engine.setFreeSlots(0);
			}
		}
		Collections.sort(engines, Collections.reverseOrder((e1,e2) -> e1.getFreeSlots() - e2.getFreeSlots()));
		//engines.sort((e1,e2) -> e1.getFreeSlots() - e2.getFreeSlots());
		log.log(Level.INFO, "order by free slots : " + engines);
		Engine freeEngine = engines.get(0);
		lastServedEngine = freeEngine;
		log.log(Level.INFO, "returning Engine : " + freeEngine.getName() + " based on freeslots available, available Engines = " + engines);
		return freeEngine.getUrl();
	}
	
	private Engine getByRoundRobin() {
		Engine engine;
		if (lastServedEngine == null || engines.size() == 1
				|| engines.indexOf(lastServedEngine) == engines.size() - 1) {
			engine = engines.get(0);
		} else {
			log.log(Level.INFO, "returning next Engine in the list");
			engine = engines.get(engines.indexOf(lastServedEngine) + 1);
		}
		lastServedEngine = engine;
		log.log(Level.INFO, "returning Engine : " + engine.getName() + " by round robin, available Engines = " + engines);
		return engine;
	}


	/**
	 * To get the RMI URL for engine
	 * @return String: engine RMI URL
	 */
	public String getEngineRmiUrl(String engineServiceHostName){
		log.finest("Begin: getEngineRmiUrl");
		String rmiURL = null ;
		String engineHostName = appProps.getProperty("ENGINE_HOSTNAME");
		if(engineHostName == null || engineHostName.isEmpty()){
			log.finest("Engine host name is not defined in ETLC properties file.");
			//Get from service name of engine
			try{
				log.finest("Getting Engine host name via service name: " + engineServiceName);
				engineHostName = engineServiceHostName;
			}catch(final Exception e){
				log.finest("Exception comes while getting Engine host name via service name. Setting it to default hostname: " + defaultEniqHost);
				engineHostName = defaultEniqHost ;
			}
		}
		log.finest("Engine host name set as: " + engineHostName);
		
		final String enginePortS = appProps.getProperty("ENGINE_PORT", defaultRMIPortS);
		int enginePortI = defaultRMIPortI ;
		try {
			enginePortI = Integer.parseInt(enginePortS);
		}catch (final Exception e) {
			enginePortI = defaultRMIPortI ;
		}
		log.finest("Engine RMI port set as: " + enginePortI);

		final String engineRefName = appProps.getProperty("ENGINE_REFNAME", defaultEngineRefName);
		log.finest("Engine Refrence Name set as: " + enginePortI);
		
		rmiURL = "//" + engineHostName + ":" + enginePortI + "/" + engineRefName ;
		log.info("Created RMI URL for engine : " + rmiURL);
		log.finest("End: getEngineRmiUrl");
		return rmiURL;
	}
	
	/**
	 * To get the RMI URL for scheduler
	 * @return String: scheduler RMI URL
	 */
	public String getSchedulerRmiUrl(){
		log.finest("Begin: getSchedulerRmiUrl");
		String rmiURL = null ;
		String schedulerHostName = appProps.getProperty("SCHEDULER_HOSTNAME");
		if(schedulerHostName == null || schedulerHostName.isEmpty()){
			log.finest("Scheduler host name is not defined in ETLC properties file.");
			//Get from service name of engine
			try{
				log.finest("Getting Scheduler host name via service name: " + schedulerServiceName);
				schedulerHostName = ServicenamesHelper.getServiceHost(schedulerServiceName, defaultEniqHost);
			}catch(final Exception e){
				log.finest("Exception comes while getting Scheduler host name via service name. Setting it to default hostname: " + defaultEniqHost);
				schedulerHostName = defaultEniqHost ;
			}
		}
		log.finest("Scheduler host name set as: " + schedulerHostName);
		
		final String schedulerPortS = appProps.getProperty("SCHEDULER_PORT", defaultRMIPortS);
		int schedulerPortI = defaultRMIPortI ;
		try {
			schedulerPortI = Integer.parseInt(schedulerPortS);
		}catch (final Exception e) {
			schedulerPortI = defaultRMIPortI ;
		}
		log.finest("Scheduler RMI port set as: " + schedulerPortI);

		final String schedulerRefName = appProps.getProperty("SCHEDULER_REFNAME", defaultSchedulerRefName);
		log.finest("Scheduler Refrence Name set as: " + schedulerRefName);
		
		rmiURL = "//" + schedulerHostName + ":" + schedulerPortI + "/" + schedulerRefName ;
		log.info("Created RMI URL for scheduler : " + rmiURL);
		log.finest("End: getSchedulerRmiUrl");
		return rmiURL;
	}
	
	/**
	 * To get the RMI URL for licmgr
	 * @return String: licmgr RMI URL
	 */
	public String getLicmgrRmiUrl(){
		log.finest("Begin: getLicmgrRmiUrl");
		String rmiURL = null ;
		String licHostName = appProps.getProperty("LICENSING_HOSTNAME");
		if(licHostName == null || licHostName.isEmpty()){
			log.finest("Licensing host name is not defined in ETLC properties file.");
			//Get from service name of engine
			try{
				log.finest("Getting Licensing host name via service name: " + licServiceName);
				licHostName = ServicenamesHelper.getServiceHost(licServiceName, defaultEniqHost);
			}catch(final Exception e){
				log.finest("Exception comes while getting Licensing host name via service name. Setting it to default hostname: " + defaultEniqHost);
				licHostName = defaultEniqHost ;
			}
		}
		log.finest("Licensing host name set as: " + licHostName);
		
		final String licPortS = appProps.getProperty("LICENSING_PORT", defaultRMIPortS);
		int licPortI = defaultRMIPortI ;
		try {
			licPortI = Integer.parseInt(licPortS);
		}catch (final Exception e) {
			licPortI = defaultRMIPortI ;
		}
		log.finest("Licensing RMI port set as: " + licPortI);

		final String licRefName = appProps.getProperty("LICENSING_REFNAME", defaultLicRefName);
		log.finest("Licensing Refrence Name set as: " + licRefName);
		
		rmiURL = "//" + licHostName + ":" + licPortI + "/" + licRefName ;
		log.info("Created RMI URL for Licensing : " + rmiURL);
		log.finest("End: getLicmgrRmiUrl");
		return rmiURL;
	}
	
	/**
	 * To get the RMI URL for RAT/NAT Slave servers
	 * @return String: MultiES RMI URL
	 */
	public String getMultiESRmiUrl(String ipAdd){
		Logger log1 = Logger.getLogger("symboliclinkcreator.rat");
//		log1.finest("Begin: getMultiESRmiUrl");
		String rmiURL = null ;
		String multiEsHost = ipAdd;
		
//		log1.finest("Slave server IP Address set as: " + multiEsHost);
		
		final String multiESPortS = appProps.getProperty("MULTI_ES_PORT", defaultRMIPortS);
		int multiESPortI = defaultRMIPortI ;
		try {
			multiESPortI = Integer.parseInt(multiESPortS);
		}catch (final Exception e) {
			multiESPortI = defaultRMIPortI ;
		}
//		log1.finest("Multi ES RMI port set as: " + multiESPortI);

		final String multiESRefName = appProps.getProperty("MULTI_ES_REFNAME", defaultMultiESRefName);
//		log1.finest("Slave Refrence Name set as: " + multiESRefName);
		
		rmiURL = "//" + multiEsHost + ":" + multiESPortI + "/" + multiESRefName ;
//		log1.finest("Created RMI URL for Slave : " + rmiURL);
//		log1.finest("End: getMultiESRmiUrl");
		return rmiURL;
	}
	
	private class Engine {
		private String name;
		private String url;
		private int freeSlots;
		
		public Engine(String name) {
			this.name = name;
		}
		
		public Engine(String name, String url) {
			this.name = name;
			this.url = url;
		}
		
		public String getName() {
			return name;
		}

		public String getUrl() {
			return url;
		}
		public void setUrl(String url) {
			this.url = url;
		}
		public int getFreeSlots() {
			return freeSlots;
		}
		public void setFreeSlots(int freeSlots) {
			this.freeSlots = freeSlots;
		}

		
		
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + freeSlots;
			result = prime * result + ((name == null) ? 0 : name.hashCode());
			result = prime * result + ((url == null) ? 0 : url.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Engine other = (Engine) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (freeSlots != other.freeSlots)
				return false;
			if (name == null) {
				if (other.name != null)
					return false;
			} else if (!name.equals(other.name))
				return false;
			if (url == null) {
				if (other.url != null)
					return false;
			} else if (!url.equals(other.url))
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "Engine [name=" + name + ", url=" + url + ", freeSlots=" + freeSlots + "]";
		}

		private RmiUrlFactory getOuterType() {
			return RmiUrlFactory.this;
		}
			
		
		
		
	}

}
