package com.distocraft.dc5000.etl.engine.main;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.Date;
import java.util.Enumeration;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import org.apache.velocity.app.Velocity;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeConstants;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.ENIQRMIRegistryManager;
import com.distocraft.dc5000.common.Properties;
import com.distocraft.dc5000.common.ServicenamesHelper;
import com.distocraft.dc5000.common.SessionHandler;
import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.common.monitor.ServiceMonitor;
import com.distocraft.dc5000.etl.binaryformatter.BinFormatter;
import com.distocraft.dc5000.etl.engine.common.EngineCom;
import com.distocraft.dc5000.etl.engine.common.EngineConnect;
import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.common.ExceptionHandler;
import com.distocraft.dc5000.etl.engine.common.Share;
import com.distocraft.dc5000.etl.engine.common.Util;
import com.distocraft.dc5000.etl.engine.executionslots.ExecutionSlot;
import com.distocraft.dc5000.etl.engine.executionslots.ExecutionSlotProfile;
import com.distocraft.dc5000.etl.engine.executionslots.ExecutionSlotProfileHandler;
import com.distocraft.dc5000.etl.engine.main.exceptions.InvalidSetParametersException;
import com.distocraft.dc5000.etl.engine.main.exceptions.InvalidSetParametersRemoteException;
import com.distocraft.dc5000.etl.engine.plugin.PluginLoader;
import com.distocraft.dc5000.etl.engine.priorityqueue.DBConnectionMonitor;
import com.distocraft.dc5000.etl.engine.priorityqueue.PersistenceHandler;
import com.distocraft.dc5000.etl.engine.priorityqueue.PriorityQueue;
import com.distocraft.dc5000.etl.engine.sql.Loader;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.engine.system.ETLCEventHandler;
import com.distocraft.dc5000.etl.engine.system.SetListener;
import com.distocraft.dc5000.etl.engine.system.SetListenerManager;
import com.distocraft.dc5000.etl.engine.system.SetManager;
import com.distocraft.dc5000.etl.engine.system.SetStatusTO;
import com.distocraft.dc5000.etl.rock.Meta_collection_sets;
import com.distocraft.dc5000.etl.rock.Meta_collection_setsFactory;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_collectionsFactory;
import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.distocraft.dc5000.etl.rock.Meta_errors;
import com.distocraft.dc5000.etl.rock.Meta_schedulings;
import com.distocraft.dc5000.etl.rock.Meta_schedulingsFactory;
import com.distocraft.dc5000.etl.rock.Meta_transfer_batches;
import com.distocraft.dc5000.etl.rock.Meta_transfer_batchesFactory;
import com.distocraft.dc5000.etl.scheduler.ISchedulerRMI;
import com.distocraft.dc5000.etl.scheduler.SchedulerConnect;
import com.distocraft.dc5000.repository.cache.ActivationCache;
import com.distocraft.dc5000.repository.cache.AggregationRuleCache;
import com.distocraft.dc5000.repository.cache.AggregationStatus;
import com.distocraft.dc5000.repository.cache.AggregationStatusCache;
import com.distocraft.dc5000.repository.cache.BackupConfigurationCache;
import com.distocraft.dc5000.repository.cache.CountingManagementCache;
import com.distocraft.dc5000.repository.cache.DBLookupCache;
import com.distocraft.dc5000.repository.cache.DataFormatCache;
import com.distocraft.dc5000.repository.cache.MetaDataRockCache;
import com.distocraft.dc5000.repository.cache.PhysicalTableCache;
import com.distocraft.dc5000.repository.dwhrep.Configuration;
import com.distocraft.dc5000.repository.dwhrep.ConfigurationFactory;
import com.distocraft.dc5000.repository.dwhrep.Dwhtype;
import com.distocraft.dc5000.repository.dwhrep.DwhtypeFactory;
import com.distocraft.dc5000.repository.dwhrep.Tpactivation;
import com.distocraft.dc5000.repository.dwhrep.TpactivationFactory;
import com.ericsson.eniq.backuprestore.restore.TriggerDataRestoreProcess;
import com.ericsson.eniq.exception.LicensingException;
import com.ericsson.eniq.licensing.cache.DefaultLicenseDescriptor;
import com.ericsson.eniq.licensing.cache.DefaultLicensingCache;
import com.ericsson.eniq.licensing.cache.LicenseDescriptor;
import com.ericsson.eniq.licensing.cache.LicenseInformation;
import com.ericsson.eniq.licensing.cache.LicensingCache;
import com.ericsson.eniq.licensing.cache.LicensingResponse;
import com.ericsson.eniq.repository.ETLCServerProperties;
import com.ericsson.eniq.common.Constants;

/**
 * The main class of ETL Engine.
 *
 * @author Jarno Savinen
 * @author Tuomas Lemminkainen
 */

public class TransferEngine extends UnicastRemoteObject implements ITransferEngineRMI {

  private static final String ENABLED_FLAG = "Y";

  private static final long serialVersionUID = 1055137567126712402L;

  /** Starter and capacity license definitions for Stats */
  public final static String ENIQ_STARTER_LICENSE = Constants.ENIQ_STARTER_LICENSE;

  public final static String ENIQ_CAPACITY_LICENSE = Constants.ENIQ_CAPACITY_LICENSE;

  /** Starter license definitions for Events */
  public final static String EVENTS_STARTER_LICENSE = "CXC4012080";

  /** Starter license definitions for SONV */
  public final static String SONV_STARTER_LICENSE = "CXC4011617";

  private static final String SERVICE_NODE_NOT_SET_YET = "";

  private String activeStarterlicense = "";

  private long startedAt = 0L;

  private PluginLoader pluginLoader;

  private String pluginPath = "/work/dagger/plugins";

  private int serverPort;
  
  private String serverHostName;

  private String serverRefName;

  private PriorityQueue priorityQueue;

  private ExecutionSlotProfileHandler executionSlotHandler;

  private long priorityQueuePollIntervall = 0;

  private int maxPriorityLevel = 0;

  private boolean usePriorityQueue = true;

  private boolean useDefaultExecutionSlots = false;

  private int NumberOfDefaulrExecutionSlots = 1;

  // this is used when starting set directly...
  private static ExecutionSlot staticExSlot = null;

  private String etlrep_url = "";
  private String etlrep_usr = "";
  private String etlrep_pwd = "";
  private String etlrep_drv = "";

  private String dwhrep_url = "";
  private String dwhrep_usr = "";
  private String dwhrep_pwd = "";
  private String dwhrep_drv = "";

  private Logger log;

  private boolean isInitialized = false;
  
  private boolean isCacheRefreshed = false;

  private EngineCom eCom = null;

  RockFactory rdwhdb = null;

  private ScheduledThreadPoolExecutor executor = null ;

  private String LICENCE_HOSTNAME = null;
  private String LICENCE_PORT = null;
  private String LICENCE_REFNAME = null;

  private final String INACTIVE_EXE_PROFILE = "InActive" ;

  //new class variables for code refactoring
  String dwh_url = null;
  String dwh_usr = null;
  String dwh_pwd = null;
  String dwh_drv = null;
  String dwh_dba_url = null;
  String dwh_dba_usr = null;
  String dwh_dba_pwd = null;
  String dwh_dba_drv = null;
  RockFactory etlRock = null;

  private boolean isEngineInActive = false ;
  Thread strThr ;

  private boolean isEngStopCalled = false;

  private boolean isLicAlreadyChecked = false ;
  private boolean isRMIAlredayCreated = false ;
  
  private final SimpleDateFormat timeStampFormat = new SimpleDateFormat ("yyyy.MM.dd_HH:mm:ss");
  
  private int timeout = 100;
  
  private static int RMIEngineUserPort;
  
  private static final String ENGINE_RMI_PROCESS_PORT = "ENGINE_RMI_PROCESS_PORT";
  private static final String ENGINE_RMI_PROCESS_PORT_DEFAULT = "60003";
  
  private static final String CONF_DIR = "CONF_DIR";
  private static final String CONF_DIR_DEFAULT = "/eniq/sw/conf";
  private static final String ETLCPROPERTIES = "ETLCServer.properties";

  /**
   * Constructor for starting the transfer
   *
   * @param usePQ
   * @param useDefaultEXSlots
   * @param EXSlots
   * @throws RemoteException
   */
  public TransferEngine(final boolean usePQ, final boolean useDefaultEXSlots, final int EXSlots, final Logger log)
      throws RemoteException {
    super();

    this.usePriorityQueue = usePQ;
    this.useDefaultExecutionSlots = useDefaultEXSlots;
    this.NumberOfDefaulrExecutionSlots = EXSlots;

    this.log = log;
  }

  /**
   * Constructor for starting the transfer
   */
  public TransferEngine(final Logger log) throws RemoteException {
    this(RMIEngineUserPort);

    this.log = log;
  }
  
  public TransferEngine(final int RMIEnigineProcessPort) throws RemoteException {
	  super(RMIEnigineProcessPort);
  }

  /**
   * Initializes the server - binds the server object to RMI registry -
   * initializes the omi connection - instantiates the access controller
   *
   * @param name
   *          String Name of server in registry
   * @return boolean true if initialisation succeeds otherwise false
   * @exception
   */
  	@Override
	public final boolean init() {
		log.fine("Initializing ETLC Engine...");
    startedAt = System.currentTimeMillis();

		getProperties();
		//License check
		try{
			if(!isLicAlreadyChecked){
				String flagFile = "/eniq/sw/conf/.licenseHoldFlag";
				File licnseFlag = new File(flagFile);
				if (licnseFlag.exists()) {
					log.info("License flag file exists, hence skipping the licenseCheck");
				} else {
					checkLicense();
				}
				isLicAlreadyChecked = true ;
			}
		}catch(final LicensingException licExp){
			log.log(Level.SEVERE, "Exception comes while checking license for engine. ", licExp);
			log.severe("Can not initialize engine. Please check starter and capacity licenses. Exiting....");
			System.exit(1);
		}catch(final Exception e){
			log.log(Level.SEVERE, "Exception comes while checking license for engine. ", e);
			log.severe("Can not initialize engine. Please check starter and capacity licenses. Exiting....");
			System.exit(1);
		}
		
		String adapter_temp_dir = "/eniq/data/etldata/adapter_tmp";
		File directory = new File(adapter_temp_dir);
		// make sure directory exists
		if (!directory.exists()) {
			log.severe("Directory does not exist.");
		} else {
			try {
				delete(directory);
			} catch (IOException e) {
				log.log(Level.WARNING,"Exception comes while deleting the adapter_temp dir",e);
			}
		}
		
		//RMI connection check
		if(!isRMIAlredayCreated){
			if(!createRMIConnection()){
				log.severe("Can not connect to RMI service. Exiting....");
			    System.exit(1);
			}else{
				log.info("Successfully created connection to RMI service.");
			}
			isRMIAlredayCreated = true ;
		}

		//PluginLoader
		try{
			if (this.pluginLoader == null) {
				this.pluginLoader = new PluginLoader(this.pluginPath);
			}
		}catch (final Exception exp) {
			log.log(Level.WARNING, "PluginLoader initialization failed", exp);
			return false;
		}

		EngineStarterThread engStarThr = new EngineStarterThread();
		strThr = new Thread(engStarThr);
		strThr.start();

    // Perform some static object initialization...

		log.fine("TransferEngine initialized.");
		return true;
  } // init


  	/**
  	 * Function to create the connection to RMI service
  	 * @return True if successful, false otherwise
  	 */
  	private boolean createRMIConnection(){
  		boolean returnStatus = false ;
  		final String rmiRef = "//" +"127.0.0.1" + ":" + this.serverPort + "/" + this.serverRefName;
	    ENIQRMIRegistryManager rmiRgty = new ENIQRMIRegistryManager("localhost", this.serverPort);
		try {
			Registry rmi = rmiRgty.getRegistry();
	    	log.fine("Engine server is rebinding to rmiregistry on host: " + this.serverHostName + " on port: " + this.serverPort +
	    			" with name: " + this.serverRefName);
			rmi.rebind(this.serverRefName, this);
			log.info("Engine Server binds successfully to already running rmiregistry");
			returnStatus = true ;
		} catch (final Exception e) {
			//NEW HSS/SMF Stuff
	    	if(!(ServiceMonitor.isSmfEnabled())){
	    		//SMF is disabled
	    		log.log(Level.SEVERE, "Unable to bind to the rmiregistry using refrence: " + rmiRef, e);
	    		returnStatus = false ;
	    	}else{
	    		//SMF is enabled
	    		try{
	    			log.info("Starting RMI-Registry on port " + this.serverPort);
	    			Registry rmi = rmiRgty.createNewRegistry();
	    			log.info("New RMI-Registry created");
	    			rmi.bind(this.serverRefName, this);
	    			log.fine("Engine Server registered to newly started rmiregistry");
	    			returnStatus = true ;
	    		}catch (Exception exception) {
	    			log.log(Level.SEVERE, "Unable to initialize LocateRegistry", exception);
	    			returnStatus = false ;
	    		}
	    	}//else
		}//catch
		return returnStatus ;
  	}// end of function
  	
  	@Override
	  public boolean isCacheRefreshed() throws RemoteException {
	    return (this.isCacheRefreshed);
	  }
  	private class EngineStarterThread implements Runnable{

		@Override
		public void run() {

			//RepDB check
			//Wait for repdb if it's not OK
			waitForRepDBToComeUp();

			if(isEngStopCalled){
				return ;
			}
			//repDB dependent tasks
			doRepDBDependentTasks();

			isInitialized = true ;

			//dwhDB check
			//Wait for dwhdb if it's not OK
			waitForDwhDBToComeUp();

			if(isEngStopCalled){
				return ;
			}

			//dwhDB dependent tasks
			doDwhDBDependentTask();
			
			isCacheRefreshed = true;
	


		}

  	}

  	/**
  	 * Function to check repDB status
  	 * @return true if OK, false otherwise
  	 */
  	private void checkRepDBConnection() throws Exception{
  	    // check connection to metadata
  		try {
  			etlRock = new RockFactory(etlrep_url, etlrep_usr, etlrep_pwd, etlrep_drv, "ETLEngInit", true);
  			final Meta_databases selO = new Meta_databases(etlRock);
  			final Meta_databasesFactory mdbf = new Meta_databasesFactory(etlRock, selO);
  			final Vector<Meta_databases> dbs = mdbf.get();
  			for (int i = 0; i < dbs.size(); i++) {
  				final Meta_databases mdb = dbs.get(i);
  				if (mdb.getConnection_name().equalsIgnoreCase("dwh_coor") && mdb.getType_name().equalsIgnoreCase("USER")) {
  					dwh_url = mdb.getConnection_string();
  					dwh_usr = mdb.getUsername();
  					dwh_pwd = mdb.getPassword();
  					dwh_drv = mdb.getDriver_name();
  				} else if (mdb.getConnection_name().equalsIgnoreCase("dwhrep") && mdb.getType_name().equalsIgnoreCase("USER")) {
  					dwhrep_url = mdb.getConnection_string();
  					dwhrep_usr = mdb.getUsername();
  					dwhrep_pwd = mdb.getPassword();
  					dwhrep_drv = mdb.getDriver_name();
  				} else if (mdb.getConnection_name().equalsIgnoreCase("dwh_coor") && mdb.getType_name().equalsIgnoreCase("DBA")) {
  					dwh_dba_url = mdb.getConnection_string();
  					dwh_dba_usr = mdb.getUsername();
  					dwh_dba_pwd = mdb.getPassword();
  					dwh_dba_drv = mdb.getDriver_name();
  				}
  			}//for
  			if (dwh_url == null || dwhrep_url == null || dwh_dba_url == null) {
  				throw new Exception("Databases dwh, dwh (DBA) and dwhrep must be defined in META_DATABASES."); // NOPMD
  			}
  		}catch (final Exception e) {
			if(etlRock != null && etlRock.getConnection() != null) {
				etlRock.getConnection().close();
			}
			throw new Exception("Exception comes while querying repdb database connection. Reason: " + e.getMessage());
  	    }
  	}// end of function

  	/**
  	 * Function to poll repdb status and sleep and wait untill it's not OK
  	 */
  	private void waitForRepDBToComeUp(){
  		try{
			checkRepDBConnection();
			log.info("Successfully queried repdb database. ");
			isEngineInActive = false ;
		}catch(final Exception e){
			log.log(Level.SEVERE," Exception caught while querying repdb database. ", e);
			log.info("Failed to query repdb database successfully. Waiting for repdb....");
			//Waiting for Repdb status
			while(true){
				if(isEngStopCalled){
					log.info(" Breaking the loop for waitForRepDBToComeUp");
					//Break the loop
					break ;
				}
				try{
					checkRepDBConnection();
					log.info("Successfully queried repdb database. ");
					isEngineInActive = false ;
					break;
				}catch(final Exception e1){
					log.log(Level.FINEST, "Failed to query repdb database successfully. Waiting for repdb....",e1);
					//Send engine in InActive State
					isEngineInActive = true ;
					//Sleeping for 10 seconds
					try {
						if(etlRock != null && etlRock.getConnection() != null) {
							etlRock.getConnection().close();
						}
					} catch (SQLException e2) {
						log.log(Level.WARNING, "Error while closing repdb connection in the catch block");
					}
					try{
						Thread.sleep(10000);
					}catch(final Exception se){
						//Ignore
					}
				}
			}//while
		}//catch
  	}// end of function

  	private void doRepDBDependentTasks(){

  		try {
      final Share sh = Share.instance();
      eCom = new EngineCom();


      // Initialize SessionHandler
      SessionHandler.init();

      // reload properties
      Properties.reload();

      // Read properties
      getProperties();

      // Reload logging configurations
      reloadLogging();

      // Init AggregationStatusCache
  			//AggregationStatusCache.init(dwh_url, dwh_usr, dwh_pwd, dwh_drv);

      // Init Parser DataFormat Cache
      DataFormatCache.initialize(etlRock);

      // Init Loader PhysicalTable Cache
      PhysicalTableCache.initialize(etlRock);

      // Init Connection for Counting Management Cache
      CountingManagementCache.initializeCache(etlRock);

      // Init Loader AggregationRule Cache
      AggregationRuleCache.initialize(etlRock);

      // Init Activation Cache
      ActivationCache.initialize(etlRock);

      // init DBLookup Cache
      DBLookupCache.initialize(etlRock);
      
      // init BackupConfiguration Cache
      
      BackupConfigurationCache.initialize(etlRock);
      
      // init MetaDataRockCache
      
      MetaDataRockCache.initalizer(etlRock);

      // create ETLC Event Handler
      final ETLCEventHandler eh = new ETLCEventHandler("LoadedTypes");
      eh.addListener(DBLookupCache.getCache());
      sh.add("LoadedTypes", eh);

      // Create Execution profile
      if (this.useDefaultExecutionSlots) {
        executionSlotHandler = new ExecutionSlotProfileHandler(this.NumberOfDefaulrExecutionSlots);
      } else {
  				executionSlotHandler = new ExecutionSlotProfileHandler(this.etlrep_url, this.etlrep_usr, this.etlrep_pwd,
  								this.etlrep_drv);
      }

      if (executionSlotHandler == null) {
  				log.severe("No executionSlots created. ExecutionSlotProfile is null.");
      } else {
        sh.add("executionSlotProfileObject", executionSlotHandler);
      }

      if (this.usePriorityQueue) {
  				final PersistenceHandler ph = new PersistenceHandler(etlRock.getDbURL(), etlRock.getUserName(),
  							etlRock.getPassword(), etlRock.getDriverName());
        // Create Priority queue
        priorityQueue = new PriorityQueue(this.priorityQueuePollIntervall, this.maxPriorityLevel, ph,
            executionSlotHandler);
        sh.add("priorityQueueObject", priorityQueue);

        // startup schedulings
        addStartupSetsToQueue(etlRock);

        // add persisted sets to queue
        final List<EngineThread> persisted = ph.getSets(eCom, pluginLoader);

        for (final EngineThread et : persisted) {
          priorityQueue.addSet(et);
        }
        //For TR HO69296. DBConnectionMonitor will defreeze the priority queue if reader and repdb is available.
        priorityQueue.setActive(false);
        this.log.log(Level.FINE, "Priority Queue is disabled.Waiting for DBConnectionMonitor to enable the queue.");
        priorityQueue.start();
      }//if (this.usePriorityQueue)

      // Monitor availability of DWH database connections
      this.executor  = new ScheduledThreadPoolExecutor(1);
      this.executor.scheduleWithFixedDelay(
    		  new DBConnectionMonitor(this, priorityQueue, executionSlotHandler, etlrep_url, etlrep_usr,
    				  etlrep_pwd, etlrep_drv),
    		  5000,
    		  Long.valueOf(StaticProperties.getProperty("DBConnectionMonitor_interval_in_milliseconds", "15000")),
    		  TimeUnit.MILLISECONDS
    	);
      } catch (final Exception e) {
  			log.log(Level.WARNING, "Initialization exception", e);
      ExceptionHandler.handleException(e);
    } finally {
  	      try {
        etlRock.getConnection().close();
      } catch (final Exception ef) {
        ExceptionHandler.handleException(ef);
  	      }
  	    }
  	}//end of function

  	public void waitForCache(){
  		doDwhDBDependentTask();
  	}
  	/**
  	 * Function to wait untill dwhDB comes online
  	 */
  	private void waitForDwhDBToComeUp(){

  		try{
			rdwhdb = new RockFactory(dwh_url, dwh_usr, dwh_pwd, dwh_drv, "ETLEngTCInit", true);
			log.info("Successfully queried dwhdb database. ");
		}catch (final Exception e) {
			log.log(Level.SEVERE,"Exception comes while connecting to DWHDB",e);
			log.severe("Cannot connect to dwhdb database. Waiting for dwhDB....");
			priorityQueue.setActive(false); // TODO (MultipleWriters: disable corresponding slots)
	        log.warning("Putting priority queue on HOLD.");
	        //Also set profile to NoLoads
	        boolean pro = EngineConnect.changeProfile("NoLoads");
	        if (pro) {
	        	log.warning("Engine profile set to NoLoads.");
	        }else {
	        	log.severe("Could not put engine to NoLoads execution profile.");
			}
			try {
				if (rdwhdb != null && rdwhdb.getConnection() != null) {
					rdwhdb.getConnection().close();
				}
			} catch (SQLException e1) {
				log.log(Level.WARNING, "Error while closing dwhdb connection in the catch block");
			}
	  		//Waiting for dwhDB status
	  		while(true){
	  			if(isEngStopCalled){
	  				log.info(" Breaking the loop for waitForDwhDBToComeUp");
					//Break the loop
					break;
				}
	  			//dwhDB check
				try{
					rdwhdb = new RockFactory(dwh_url, dwh_usr, dwh_pwd, dwh_drv, "ETLEngTCInit", true);
					log.info("Successfully queried dwhdb database. ");
					break;
				}catch (final Exception ne) {
					log.log(Level.FINEST,"Cannot connect to dwhdb database. Waiting for dwhDB....",ne);
					//Sleeping for 10 seconds
					try {
						if (rdwhdb != null && rdwhdb.getConnection() != null) {
							rdwhdb.getConnection().close();
						}
					} catch (SQLException e1) {
						log.log(Level.WARNING, "Error while closing dwhdb connection in the catch block");
					}
					try{
						Thread.sleep(10000);
					}catch(final Exception ex){
						//Ignore
					}
				}//catch
	  		}//while
		}//catch
  	}//end of function

  	
  	//cache status : not initialized during engine restart since this method is accessed by both timer as well as the thread in run() method.
  	private synchronized void doDwhDBDependentTask(){
  		try {
  			isCacheRefreshed = false;
  	    	eCom = new EngineCom();
  	    	// Init AggregationStatusCache
  	    	AggregationStatusCache.init(dwh_url, dwh_usr, dwh_pwd, dwh_drv);

  	    	RockFactory rdwh = null;
  	    	RockFactory rrep = null;
  	    	try {
  	    		rdwh = new RockFactory(dwh_url, dwh_usr, dwh_pwd, dwh_drv, "ETLEngDwhdbDependTask", true);
  				rrep = new RockFactory(dwhrep_url, dwhrep_usr, dwhrep_pwd, dwhrep_drv, "ETLEngDwhdbDependTask", true);
  				// Init TransformerCache. Reflection allows to run without parser
  				// module.
  				try {
  					final Class<?> tfCacheClass = Class.forName("com.distocraft.dc5000.etl.parser.TransformerCache");
  					final Object tfCache = tfCacheClass.newInstance();
  					final Class<?>[] argClasses = { rrep.getClass(), rdwh.getClass() };
  					final Method revalidateMet = tfCacheClass.getMethod("revalidate", argClasses);
  					final Object[] args = { rrep, rdwh };
  					revalidateMet.invoke(tfCache, args);
  				}catch (ClassNotFoundException e) {
  					log.config("Parser module has not been installed. Transformer cache initialization cancelled.");
  				}

  				// Init AlarmConfigCache. Reflection allows to without alarm module.
  				try {
  					final Class<?> raccClass = Class.forName("com.ericsson.eniq.etl.alarm.RockAlarmConfigCache");
  					final Class<?>[] parameterTypes = { rrep.getClass(), log.getClass() };
  					final Method initMet = raccClass.getMethod("initialize", parameterTypes);
  					final Object arglist[] = new Object[] {rrep, log};
  					initMet.invoke(null, arglist);
  					final Method revalidateMet = raccClass.getMethod("revalidate");
  					revalidateMet.invoke(null);
  				}catch (ClassNotFoundException e) {
  					log.config("Alarm module has not been installed. AlarmConfigCache initialization cancelled.");
  				}

  				try {
  					// Get DWHDB's collation/charset/encoding
  					getDWHDBCharsetEncoding(rdwh);
  				} catch (final Exception e) {
  					log.log(Level.WARNING, "Error in reading charset encoding from dwhdb database.", e);
  				}
  				configureWorkerLimits(rrep);

  	    	}finally {
  	    		try {
  	    			rdwh.getConnection().close();
  				} catch (final Exception e) {
  				}
  				try {
  					rrep.getConnection().close();
  				} catch (final Exception e) {
  				}
  	    	}
  	    	isCacheRefreshed = true;
  	    }catch (final Exception e) {
  	    	log.log(Level.WARNING, "Initialization exception", e);
  	    	ExceptionHandler.handleException(e);
  	    }
  	}



  private void addStartupSetsToQueue(final RockFactory rock) {

    try {

      /* get list of collection sets */
      final Meta_collection_sets whereCollection_sets = new Meta_collection_sets(rock);
      final Meta_collection_setsFactory activeCollectionSets = new Meta_collection_setsFactory(rock,
          whereCollection_sets);

      // list IDs of the active collection sets
			final List<Long> activeCollectionIDs = new ArrayList<Long>();
      for (int i = 0; i < activeCollectionSets.size(); i++) {

        final Meta_collection_sets cSet = activeCollectionSets.getElementAt(i);

        // if collection set is active add it to the list
        if (cSet.getEnabled_flag().equalsIgnoreCase("y")) {
          activeCollectionIDs.add(cSet.getCollection_set_id());
        }
      }

      // retrievs rows from schedule
      final Meta_schedulings whereSchedule = new Meta_schedulings(rock);
      whereSchedule.setExecution_type("onStartup");
      whereSchedule.setHold_flag("N");
      final Meta_schedulingsFactory schedules = new Meta_schedulingsFactory(rock, whereSchedule);

      if (activeCollectionIDs.size() > 0) {
        for (int i = 0; i < schedules.size(); i++) {

          final Meta_schedulings schedule = schedules.getElementAt(i);

          // Create only scheduling that reference active collection
          // sets
          if (activeCollectionIDs.contains(schedule.getCollection_set_id())) {

            final Long colSetID = schedule.getCollection_set_id();
            final Long colID = schedule.getCollection_id();

            /* get list of collection sets */
            final Meta_collection_sets whereCollection_sets1 = new Meta_collection_sets(rock);
            whereCollection_sets1.setCollection_set_id(colSetID);
            final Meta_collection_setsFactory mcsf = new Meta_collection_setsFactory(rock, whereCollection_sets1);

            /* get list of collections */
            final Meta_collections whereCollections = new Meta_collections(rock);
            whereCollections.setCollection_set_id(colSetID);
            whereCollections.setCollection_id(colID);
            final Meta_collectionsFactory mcf = new Meta_collectionsFactory(rock, whereCollections);

            try {

              // create engineThread (set)
							final EngineThread et = new EngineThread(etlrep_url, etlrep_usr, etlrep_pwd, etlrep_drv, mcsf
									.getElementAt(0).getCollection_set_name(), mcf.getElementAt(0).getCollection_name(),
									this.pluginLoader, "", null, log, eCom);

							et.setName(mcsf.getElementAt(0).getCollection_set_name() + "_" + mcf.getElementAt(0).getCollection_name());

              priorityQueue.addSet(et);

            } catch (final Exception e) {
              log.log(Level.WARNING, "Could not create engineThread from (techpack/set): "
                  + mcsf.getElementAt(0).getCollection_set_name() + "/" + mcf.getElementAt(0).getCollection_name(), e);

            }

          }

        }
      }
    } catch (final Exception e) {
      log.log(Level.WARNING, "Error while adding statup set to queue", e);

    }
  }

	private void executeSet(final EngineThread et) throws RemoteException {
    executeSet(et, false, false);
  }

  /**
   * @param et
   * @throws RemoteException
   */
  private void executeSet(final EngineThread et, final boolean forceFlag, final boolean setListener)
      throws RemoteException {
    while (!isInitialized()) {
      this.log.fine("Waiting for the engine to initialize before executing set " + et.getSetName() + ".");
			try {
      Thread.sleep(1000);
			} catch (final InterruptedException ie) {
			}
    }

    // Sanity checking...
    if (this.log == null) {
      this.log = Logger.getLogger("scheduler.thread");
      this.log.severe("TransferEngine.executeSet variable log was null. Created a new logger...");
    }

    if (et == null) {
      this.log.severe("TransferEngine.executeSet variable et was null.");
    }
    if (getPriorityQueue() == null) {
      this.log.severe("TransferEngine.executeSet variable this.priorityQueue was null.");
    }

    if (getExecutionSlotProfileHandler() == null) {
      this.log.severe("TransferEngine.executeSet variable this.executionSlotHandler was null.");
    }

    // put set to queue
    // If forceFlag false, the add to priorityQueue
    if (this.usePriorityQueue && !forceFlag) {
      // put set to queue
      getPriorityQueue().addSet(et);
    } else {
      runDirectlyInExecSlot(et, setListener);
    }
  }

  /**
   * Runs the set on the first available execution slot
   *
   * @param et
   */
  private void runDirectlyInExecSlot(final EngineThread et, final boolean setListener) {
    log.fine("No priority queue in use.. starting directly in execution slot");

    // put set directly to the first free execution slot
    staticExSlot = getExecutionSlotProfileHandler().getActiveExecutionProfile().getFirstFreeExecutionSlots();

    if (staticExSlot != null) {
      PriorityQueue pq = getPriorityQueue();

      if(pq!= null){
        et.setPriorityQueue(pq);
      }
      // If setListener is true, then create listener and wait for set to run
      if (setListener) {
        et.setListener = new SetListener();
        // free slot found
        staticExSlot.execute(et);
        et.setListener.listen();
      } else {
        // free slot found
        staticExSlot.execute(et);
      }
    } else {
      // NO free slot found
      log.info("No free execution slots left, unable to execute Set (" + et.getName() + ")");
    }
  }

  /**
	 * Gets the current Profile and then set the new one
	 * @param prf
	 */
	public void setNewProfileState(final String prf){
		String oldProfile = getExecutionSlotProfileHandler().getActiveExecutionProfile().name();
		log.info("setNewProfileState: Old Profile was: " + oldProfile);
		log.info("setNewProfileState: Setting new Profile: " + prf);
		getExecutionSlotProfileHandler().setActiveProfile(prf, null);
	}

  /**
   * @return
   */
  ExecutionSlotProfileHandler getExecutionSlotProfileHandler() {
    return this.executionSlotHandler;
  }

  /**
   * @return
   */
  PriorityQueue getPriorityQueue() {
    return this.priorityQueue;
  }

  /**
	 * Executes the initialized collection set / collection
	 *
	 * @param rockFact
	 *          The database connection
	 * @param collectionSetName
	 *          the name of the transfer collection set
	 * @param collectionName
	 *          the name of the transfer collection
	 * @exception RemoteException
	 */
	public void execute(final RockFactory rockFact, final String collectionSetName, final String collectionName)
			throws RemoteException {

		try {

			if (this.pluginLoader == null) {
				this.pluginLoader = new PluginLoader(this.pluginPath);
			}

			// create set
			final EngineThread et = new EngineThread(rockFact, collectionSetName, collectionName, this.pluginLoader, log,
					eCom);
			et.setName(collectionSetName + "_" + collectionName);

			this.log.info("Calling executeSet from execute(3 parameters)");

			// execute the set
			executeSet(et);

		} catch (final Exception e) {
			ExceptionHandler.handleException(e);
			throw new RemoteException("Could not start a Set", e);
		}
	}

	/**
   * Executes the initialized collection set / collection. EngineAdmin uses
   * this.
   *
   * @param url
   *          Database url
   * @param userName
   *          Database user
   * @param password
   *          Database users password
   * @param dbDriverName
   *          Database driver
   * @param collectionSetName
   *          the name of the transfer collection set
   * @param collectionName
   *          the name of the transfer collection
   * @exception RemoteException
   */
  @Override
  public void execute(final String url, final String userName, final String password, final String dbDriverName,
      final String collectionSetName, final String collectionName) throws RemoteException {

    try {

      if (this.pluginLoader == null) {
        this.pluginLoader = new PluginLoader(this.pluginPath);
      }

      final EngineThread et = new EngineThread(url, userName, password, dbDriverName, collectionSetName,
          collectionName, this.pluginLoader, null, null, log, eCom);

      et.setName(collectionSetName + "_" + collectionName);

      this.log.info("Calling executeSet from execute(6 parameters)");

      // execute the set
      executeSet(et);

    } catch (final Exception e) {
      ExceptionHandler.handleException(e);
      throw new RemoteException("Could not start a Set", e);
    }

  }

  /**
   * Executes the initialized collection set / collection. Scheduler uses this.
   *
   * @param url
   *          Database url
   * @param userName
   *          Database user
   * @param password
   *          Database users password
   * @param dbDriverName
   *          Database driver
   * @param collectionSetName
   *          the name of the transfer collection set
   * @param collectionName
   *          the name of the transfer collection
   * @param ScheduleInfo
   *          Informatin from the Scheduler
   * @exception RemoteException
   */
  @Override
  public void execute(final String url, final String userName, final String password, final String dbDriverName,
      final String collectionSetName, final String collectionName, final String ScheduleInfo) throws RemoteException {
    try {

      // Some sanity checking...

      if (this.log == null) {
        this.log = Logger.getLogger("scheduler.thread");
        this.log.severe("TransferEngine.execute(7 parameters) variable this.log was null. Created a new logger...");
      }

      if (url == null) {
        this.log.severe("TransferEngine.execute(7 parameters) variable url was null.");
      }
      if (userName == null) {
        this.log.severe("TransferEngine.execute(7 parameters) variable userName was null.");
      }
      if (password == null) {
        this.log.severe("TransferEngine.execute(7 parameters) variable password was null.");
      }
      if (dbDriverName == null) {
        this.log.severe("TransferEngine.execute(7 parameters) variable dbDriverName was null.");
      }
      if (collectionSetName == null) {
        this.log.severe("TransferEngine.execute(7 parameters) variable url collectionSetName null.");
      }
      if (collectionName == null) {
        this.log.severe("TransferEngine.execute(7 parameters) variable collectionName was null.");
      }
      if (ScheduleInfo == null) {
        this.log.severe("TransferEngine.execute(7 parameters) variable ScheduleInfo was null.");
      }

      if (this.pluginLoader == null) {
        this.pluginLoader = new PluginLoader(this.pluginPath);
      }

      final EngineThread et = new EngineThread(url, userName, password, dbDriverName, collectionSetName,
          collectionName, this.pluginLoader, ScheduleInfo, null, log, eCom);

      et.setName(collectionSetName + "_" + collectionName);

      // execute the set
      executeSet(et);

    } catch (final Exception e) {
      ExceptionHandler.handleException(e);
      throw new RemoteException("Could not start a Set", e);

    }

  }

  /**
   * Executes the initialized collection set / collection
   *
	 * @param etlrep_url
	 *          Database url
	 * @param etlrep_usr
	 *          Database user
	 * @param etlrep_pwd
	 *          Database users password
	 * @param etlrep_drv
   *          Database driver
   * @param collectionSetName
   *          the name of the transfer collection set
   * @param collectionName
   *          the name of the transfer collection
   * @param ScheduleInfo
   *          Informatin from the Scheduler
   * @exception RemoteException
   */
  @Override
  public void execute(final String collectionSetName, final String collectionName, final String ScheduleInfo)
      throws RemoteException {

    try {

      checkSetIsValidAndEnabled(collectionSetName, collectionName);

      if (this.pluginLoader == null) {
        this.pluginLoader = new PluginLoader(this.pluginPath);
      }

      final EngineThread et = createNewEngineThread(collectionSetName, collectionName, ScheduleInfo);

      et.setName(collectionSetName + "_" + collectionName);

      this.log.info("Calling executeSet from execute(3 parameters, all Strings)");

      java.util.Properties props = TransferActionBase.stringToProperties(ScheduleInfo);
      boolean isForce = Boolean.parseBoolean(props.getProperty("force", "false"));
      boolean setListener = Boolean.parseBoolean(props.getProperty("setListener", "false"));
      // execute the set
      executeSet(et, isForce, setListener);

    } catch (final InvalidSetParametersException cannotStartSetEx) {
      ExceptionHandler.handleException(cannotStartSetEx);
      throw new InvalidSetParametersRemoteException(cannotStartSetEx.getMessage());
    } catch (final Exception e) {
      ExceptionHandler.handleException(e);
      throw new RemoteException("Could not start a Set", e);
    }
  }

  /**
   * Check that both the collection set (tech pack) and the collection name
   * (set) exist in their respective tables and are enabled
   *
   * @param collectionSetName
   * @param collectionName
   * @throws RockException
   * @throws SQLException
   * @throws Exception
   */
  private void checkSetIsValidAndEnabled(final String collectionSetName, final String collectionName)
      throws InvalidSetParametersException, SQLException, RockException {

    final Long collectionSetID = checkTPExistsAndIsEnabled(collectionSetName);
    checkSetExistsAndIsEnabled(collectionName, collectionSetID);
  }

  /**
   * Check that the collection name (set) exist in the Meta_collections table
   * and is enabled
   *
   * @param collectionName
   * @param collectionSetId
   * @throws InvalidSetParametersRemoteException
   * @throws RockException
   * @throws SQLException
   */
  private void checkSetExistsAndIsEnabled(final String collectionName, final Long collectionSetId)
      throws InvalidSetParametersException, SQLException, RockException {

		RockFactory etlrep = null; 

		try {
		  etlrep = getEtlRepRockFactory("isSetEnabled");
		  if (etlrep == null) {
		    throw new RockException("Unable to connect to database");
	    }
			final Meta_collections whereMetaCollection = new Meta_collections(etlrep);
    whereMetaCollection.setCollection_name(collectionName);
    whereMetaCollection.setCollection_set_id(collectionSetId);

			final Meta_collectionsFactory metaCollectionFactory = createMetaCollectionFactory(etlrep, whereMetaCollection,
        " ORDER BY COLLECTION_NAME DESC;");
    final Vector<Meta_collections> collectionsWithThisName = metaCollectionFactory.get();
    if (collectionsWithThisName.isEmpty()) {
      throw new InvalidSetParametersException("Cannot start set, collection " + collectionName
          + " doesn't exist, or incorrect Tech Pack was supplied");
    }

    final Meta_collections metaCollection = collectionsWithThisName.get(0);
    final String enabledStatus = metaCollection.getEnabled_flag();
    if (enabledStatus.equalsIgnoreCase("n")) {
      throw new InvalidSetParametersException("Cannot start set, collection " + collectionName + " is not enabled");
    }
		} finally {
		  if (etlrep != null) {
		    etlrep.getConnection().close();
     }
		}

  }

  /**
   * refactored out in order to get methods under unit test
   *
   * @param etlRock
   * @param whereMetaCollection
   * @param orderByClause
   * @return
   * @throws SQLException
   * @throws RockException
   */
  Meta_collectionsFactory createMetaCollectionFactory(final RockFactory etlRock,
      final Meta_collections whereMetaCollection, final String orderByClause) throws SQLException, RockException {
    return new Meta_collectionsFactory(etlRock, whereMetaCollection, orderByClause);
  }

  /**
   * Check that the collection set (tech pack) exist in the Meta_Collection_sets
   * table and is enabled
   *
   * @param collectionSetName
   * @throws InvalidSetParametersRemoteException
   * @throws RockException
   * @throws SQLException
   * @return Long the collection set id of the collection set
   */
    private Long checkTPExistsAndIsEnabled(final String collectionSetName) throws InvalidSetParametersException, SQLException, RockException {
        RockFactory etlRockFact = null;
        try {
            etlRockFact = getEtlRepRockFactory("isTPEnabled");
            if (etlRockFact == null) {
              throw new RockException("Unable to connect to database");
            }
            final Meta_collection_sets whereMetaCollectionSets = new Meta_collection_sets(etlRockFact);
            whereMetaCollectionSets.setCollection_set_name(collectionSetName);
            final Meta_collection_setsFactory metaCollSetsFactory = createMetaCollectionSetsFactory(etlRockFact, whereMetaCollectionSets,
                    " ORDER BY COLLECTION_SET_NAME, ENABLED_FLAG DESC;");
            final Vector<Meta_collection_sets> setsWithThisName = metaCollSetsFactory.get();
            if (setsWithThisName.isEmpty()) {
                throw new InvalidSetParametersException("Cannot start set, collection set " + collectionSetName + " doesn't exist");
            }
            final Meta_collection_sets metaCollectionSet = setsWithThisName.get(0);
            final String isCollectionSetEnabled = metaCollectionSet.getEnabled_flag();
            if (isCollectionSetEnabled.equalsIgnoreCase("n")) {
                throw new InvalidSetParametersException("Cannot start set, collection set " + collectionSetName + " is not enabled");
            }
            return metaCollectionSet.getCollection_set_id();
        } finally {
            if (etlRockFact != null) {
                etlRockFact.getConnection().close();
            }
        }
    }

  /**
   *
   * refactored out to get methods under unit test
   *
   * @param etlRock
   * @param whereMetaCollectionSets
   * @param string
   *          orderByClause
   * @return
   * @throws SQLException
   * @throws RockException
   */
  Meta_collection_setsFactory createMetaCollectionSetsFactory(final RockFactory etlRock,
      final Meta_collection_sets whereMetaCollectionSets, final String orderByClause) throws SQLException,
      RockException {
    return new Meta_collection_setsFactory(etlRock, whereMetaCollectionSets, orderByClause);
  }

  /**
   *
   * refactored out to get under unit test
   *
   * @param collectionSetName
   * @param collectionName
   * @param ScheduleInfo
   * @return
   * @throws Exception
   */
	private EngineThread createNewEngineThread(final String collectionSetName, final String collectionName,
			final String ScheduleInfo) throws EngineMetaDataException {
		return new EngineThread(etlrep_url, etlrep_usr, etlrep_pwd, etlrep_drv, collectionSetName, collectionName,
        this.pluginLoader, ScheduleInfo, null, log, eCom);
  }

  /**
   * Executes the initialized collection set / collection
   *
	 * @param etlrep_url
	 *          Database url
	 * @param etlrep_usr
	 *          Database user
	 * @param etlrep_pwd
	 *          Database users password
	 * @param etlrep_drv
   *          Database driver
   * @param collectionSetName
   *          the name of the transfer collection set
   * @param collectionName
   *          the name of the transfer collection
   * @param ScheduleInfo
   *          Informatin from the Scheduler
   * @exception RemoteException
   */
  @Override
  public String executeAndWait(final String collectionSetName, final String collectionName, final String ScheduleInfo)
      throws RemoteException {
    try {

      if (this.pluginLoader == null) {
        this.pluginLoader = new PluginLoader(this.pluginPath);
      }

      final SetListener list = new SetListener();

			final EngineThread et = new EngineThread(etlrep_url, etlrep_usr, etlrep_pwd, etlrep_drv, collectionSetName,
          collectionName, this.pluginLoader, ScheduleInfo, list, log, eCom);

      et.setName(collectionSetName + "_" + collectionName);

      // execute the set
      executeSet(et);

      //
      final String status = list.listen();

      return status;

    } catch (final Exception e) {
      ExceptionHandler.handleException(e);
      throw new RemoteException("Could not start a Set", e);
    }

  }

  /**
   * Executes the initialized collection set / collection, and creates a
   * listener object to observe the execution. The listener id, returned by the
   * method, can be used to examine the execution's status and events with the
   * getStatusEventsWithId method.
   *
   * @param collectionSetName
   * @param collectionName
   * @param ScheduleInfo
   * @return Set listener id
   * @throws RemoteException
   */
  @Override
  public String executeWithSetListener(final String collectionSetName, final String collectionName,
      final String ScheduleInfo) throws RemoteException {
    try {

      if (this.pluginLoader == null) {
        this.pluginLoader = new PluginLoader(this.pluginPath);
      }

      final SetListener setListener = new SetListener();

      // # Add listener to SetListenerManager
      final SetListenerManager m = SetListenerManager.instance();
      final long listenerId = m.addListener(setListener);

      log.finest("Creating enginethread");
			final EngineThread et = new EngineThread(etlrep_url, etlrep_usr, etlrep_pwd, etlrep_drv, collectionSetName,
          collectionName, this.pluginLoader, ScheduleInfo, setListener, log, eCom);

      et.setName(collectionSetName + "_" + collectionName);
      log.finest("executing set");
      // execute the set
      executeSet(et);
      log.finest("returning listenerId=" + String.valueOf(listenerId));
      return String.valueOf(listenerId);

    } catch (final Exception e) {
      ExceptionHandler.handleException(e);
      throw new RemoteException("Could not start a Set", e);
    }

  }

  /**
   * Retrieves the status information and events from a set listener object. The
   * listener is identified by the statusListenerId parameter.
   *
   * @param statusListenerId
   *          Status listener's id, returned by the executeWithSetListener
   *          method.
   * @param beginIndex
   *          The index of the first retrieved status event
   * @param count
   *          The number of status events to be retrieved
   * @return A set status transfer object, containing the observed state and the
   *         status events. The returned status events are selected by the
   *         parameters beginIndex and count. If the values of both beginIndex
   *         and count are -1, then the all status events are returned.
   * @throws RemoteException
   */
  @Override
  public SetStatusTO getStatusEventsWithId(final String statusListenerId, final int beginIndex, final int count)
      throws RemoteException {
    SetStatusTO result = null;
    final SetListenerManager m = SetListenerManager.instance();
    try {
      log.finest("Trying to get setListeners with listenerId=" + statusListenerId);
      final SetListener sl = m.get(Long.parseLong(statusListenerId));
      log.finest("SetListener.status.size=" + sl.getAllStatusEvents().size());
      if (beginIndex == -1 && count == -1) {
        result = sl.getStatusAsTO();
      } else {
        result = sl.getStatusAsTO(beginIndex, count);
      }

    } catch (final NumberFormatException e) {
      log.warning("Failed to parse statusListenerId");
      e.printStackTrace();
    }
    return result;
  }

  /**
   * Writes the SQL Loader ctl file contents
   *
   * @param Vector
   *          fileContents The ctl -file description.
   * @param String
   *          fileName The ctl file name.
   * @exception RemoteException
   */
  @Override
  public void writeSQLLoadFile(final String fileContents, final String fileName) throws RemoteException {

    try {
      final File ctlFile = new File(fileName);
      final File parent = ctlFile.getParentFile();
      if (parent != null && !parent.exists()) {
        parent.mkdirs();
      }

      final FileOutputStream fileOutStream = new FileOutputStream(ctlFile);
      final PrintWriter printWriter = new PrintWriter(fileOutStream);

      printWriter.print(fileContents);

      printWriter.close();
      fileOutStream.close();
    } catch (final Exception e) {
      throw new RemoteException("Cannot write file: " + fileName, e);
    }

  }

  /**
   * Returns all available plugin names
   *
   * @return String[] plugin names
   * @throws RemoteException
   */
  @Override
  public String[] getPluginNames() throws RemoteException {
    return this.pluginLoader.getPluginNames();
  }

  /**
   * Returns the specified plugins methods
   *
   * @param String
   *          pluginName the plugin that the methods are fetched from
   * @param boolean isGetSetMethods if true, only set method names are returned
   * @param boolean isGetGetMethods if true, only get method names are returned
   * @return String[] method names
   * @throws RemoteException
   */
  @Override
  public String[] getPluginMethods(final String pluginName, final boolean isGetSetMethods, final boolean isGetGetMethods)
      throws RemoteException {
    try {
      return this.pluginLoader.getPluginMethodNames(pluginName, isGetGetMethods, isGetSetMethods);
    } catch (final Exception e) {
      throw new RemoteException("", e);
    }
  }

  /**
   * Returns the constructor parameters separated with ,
   *
   * @param String
   *          pluginName The plugin to load
   * @return String
   */
  @Override
  public String getPluginConstructorParameters(final String pluginName) throws RemoteException {
    try {
      return this.pluginLoader.getPluginConstructorParameters(pluginName);
    } catch (final Exception e) {
      throw new RemoteException("", e);
    }
  }

  // SS
  /**
   * Returns the constructor parameter info
   *
   * @param String
   *          pluginName The plugin to load
   * @return String
   */
  @Override
  public String getPluginConstructorParameterInfo(final String pluginName) throws RemoteException {
    try {
			final Class<?> pluginClass = this.pluginLoader.loadClass(pluginName);
			final Class<?>[] empty = null;
			final String methodName = "getConstructorParameterInfo";
      final java.lang.reflect.Method m = pluginClass.getDeclaredMethod(methodName, empty);
      return (String) m.invoke(null, new Object[] {});
    } catch (final Exception e) {
      throw new RemoteException("", e);
    }
  }

  @Override
  public void activateScheduler() throws RemoteException {

    try {
			final ISchedulerRMI scheduler = SchedulerConnect.connectScheduler();
			scheduler.reload();
		} catch (Exception e) {
      throw new RemoteException("Could not activate Scheduler.", e);
    }

  }

  /**
   * Returns the method parameters separated with ,
   *
   * @param String
   *          pluginName The plugin to load
   * @param String
   *          methodName The method that hold the parameters
   * @return String
   */
  @Override
  public String getPluginMethodParameters(final String pluginName, final String methodName) throws RemoteException {
    try {
      return this.pluginLoader.getPluginMethodParameters(pluginName, methodName);
    } catch (final Exception e) {
      throw new RemoteException("", e);
    }
  }

  protected void getProperties() {

	  try{
		  	//Create static properties.
			StaticProperties.reload();
	  }catch(final Exception e){
		  ExceptionHandler.handleException(e, "Cannot load StaticPropeties. Reason: " + e.getMessage());
	      System.exit(2);
	  }

	  log.info("Loaded StaticProperties successfully.");


    try {
      String etlcServerPropertiesFile = System.getProperty(CONF_DIR_DEFAULT);
      if (etlcServerPropertiesFile == null) {
        log.config("System property CONF_DIR not defined. Using default");
        etlcServerPropertiesFile = CONF_DIR_DEFAULT;
      }
      if (!etlcServerPropertiesFile.endsWith(File.separator)) {
        etlcServerPropertiesFile += File.separator;
      }

      etlcServerPropertiesFile += ETLCPROPERTIES;

      log.info("Reading server configuration from \"" + etlcServerPropertiesFile + "\"");

      final java.util.Properties appProps = new ETLCServerProperties(etlcServerPropertiesFile);

      this.pluginPath = appProps.getProperty("PLUGIN_PATH");
      if (this.pluginPath == null) {
        log.config("PLUGIN_PATH not defined. Using default.");
        this.pluginPath = "/eniq/sw/etlc/plugins/";
      }

			// get the hostname by service name and default to localhost.
		      String hostNameByServiceName = null ;
		      String ipAddress = null;
		      try{
		    	  ipAddress = InetAddress.getLocalHost().getHostAddress();
		    	  hostNameByServiceName = ServicenamesHelper.getServiceName("Engine", ipAddress, log);
		      }catch(final Exception e){
		    	  hostNameByServiceName = "localhost" ;
		      }
		      log.log(Level.INFO, "service name for ipaddress : "+ ipAddress + " is : "+hostNameByServiceName);
			this.serverHostName = appProps.getProperty("ENGINE_HOSTNAME", hostNameByServiceName);

      this.serverPort = 1200;
      final String sporttmp = appProps.getProperty("ENGINE_PORT", "1200");
      try {
        this.serverPort = Integer.parseInt(sporttmp);
      } catch (final NumberFormatException nfe) {
        log.config("Value of property ENGINE_PORT \"" + sporttmp + "\" is invalid. Using default.");
      }

      this.serverRefName = appProps.getProperty("ENGINE_REFNAME", "TransferEngine");


      //20111122 EANGUAN: Checking the hostname via ServicenamesHelper class :: Otherwise is the servername is
      // not in hosts file then engine/other services would not get license manager
      try{
    	  hostNameByServiceName = ServicenamesHelper.getServiceHost("licenceservice", "localhost");
      }catch(final Exception e){
    	  hostNameByServiceName = "localhost" ;
      }
      this.LICENCE_HOSTNAME = appProps.getProperty("LICENCE_HOSTNAME", hostNameByServiceName);

      this.LICENCE_PORT = appProps.getProperty("LICENCE_PORT", "1200");
      this.LICENCE_REFNAME = appProps.getProperty("LICENCE_REFNAME", "LicensingCache");

			this.etlrep_url = appProps.getProperty("ENGINE_DB_URL");
			this.etlrep_usr = appProps.getProperty("ENGINE_DB_USERNAME");
			this.etlrep_pwd = appProps.getProperty("ENGINE_DB_PASSWORD");
			this.etlrep_drv = appProps.getProperty("ENGINE_DB_DRIVERNAME");

			log.config("Using repository database from \"" + this.etlrep_url + "\"");

      // Priority Queue

      final String priorityQueuePollIntervall = appProps.getProperty("PRIORITY_QUEUE_POLL_INTERVALL");
      final String maxPriorityLevel = appProps.getProperty("MAXIMUM_PRIORITY_LEVEL");

      if (priorityQueuePollIntervall != null) {
        this.priorityQueuePollIntervall = Long.parseLong(priorityQueuePollIntervall);
      }

      if (maxPriorityLevel != null) {
        this.maxPriorityLevel = Integer.parseInt(maxPriorityLevel);
      }

    } catch (final Exception e) {
      ExceptionHandler.handleException(e, "Cannot read ETLCServer.properties(" + e.getMessage() + ")");
      System.exit(2);
    }

  }

  /**
   * Method to forcefully shutdown engine, priority queue and execution slots
   */
  @Override
  public void forceShutdown() throws RemoteException {
		log.info("Forcefully Shutting down engine...");
//		strThr.interrupt();
		isEngStopCalled = true ;
		//Adding the logic to avoid the exception: java.io.EOFException, while exiting the application
	    ENIQRMIRegistryManager rmi = new ENIQRMIRegistryManager(this.serverHostName, this.serverPort);
	    try {
	     	Registry registry = rmi.getRegistry();
	        registry.unbind(this.serverRefName);
	        UnicastRemoteObject.unexportObject(this, false);
	    } catch (final Exception e) {
	    	throw new RemoteException("Could not unregister Engine RMI service, quiting anyway.", e);
	    }
	    new Thread() {
	    	@Override
	    	public void run() {
	    		try {
	    			sleep(2000);
	    		} catch (InterruptedException e) {
	    			// No action to take
	    		}
	    		System.exit(0);
	    	}//run
	    }.start();
  }

  /**
   * Method to graceful shutdown engine, priority queue and execution slots this
   * lets all the sets in priority queue and all the execution slots finnis
   * their jobs and then exits.
   */
  @Override
  public void slowGracefulShutdown() throws RemoteException {

    try {
      log.info("slowGracefulShutdown: Canceling Executor thread [DBConnectionMonitor].");
      if(this.executor != null){
    	  this.executor.shutdown();
    	  log.info("Executor thread [DBConnectionMonitor] cancellation status: " + this.executor.isShutdown());
      }
      // strThr.interrupt();
      isEngStopCalled = true ;
      log.info("Gracefully Shutting down priority queue.,");

      // calculate the number of slots that have sets running..
      final int activeSlots = getExecutionSlotProfileHandler().getActiveExecutionProfile().getNumberOfExecutionSlots()
          - getExecutionSlotProfileHandler().getActiveExecutionProfile().getNumberOfFreeExecutionSlots();

      log.info(activeSlots + " slots are still running  \n" + "these slots will be allowed to finnish before shutdown");

      // put queue on hold so that no new sets are handed to execution
      // slots
      this.priorityQueue.setActive(false);

      // execution profile is locked so nobody can change profile.
      getExecutionSlotProfileHandler().lockProfile();

      log.info("\nclosing execution slots\n");

      // wait until all slots are locked..
      while (!getExecutionSlotProfileHandler().getActiveExecutionProfile().areAllSlotsLockedOrFree()) {
        System.out.print(".");
        Thread.sleep(5000);
      }

      log.info("Priority is inactive, All Execution Slots are free and Execution Profile is locked.");
      log.info("Proceed to shutdown engine using " + System.getProperty("BIN_DIR") + "/engine stop");
			log.info("Gracefully Shutting down engine...");
			//Adding the logic to avoid the exception: java.io.EOFException, while exiting the application
			ENIQRMIRegistryManager rmi = new ENIQRMIRegistryManager(this.serverHostName, this.serverPort);
		    try {
		     	Registry registry = rmi.getRegistry();
		        registry.unbind(this.serverRefName);
		        UnicastRemoteObject.unexportObject(this, false);
		    } catch (final Exception e) {
		    	throw new RemoteException("Could not unregister Engine RMI service, quiting anyway.", e);
		    }
		    new Thread() {
		    	@Override
		    	public void run() {
		    		try {
		    			sleep(2000);
		    		} catch (InterruptedException e) {
		    			// No action to take
		    		}
		    		System.exit(0);
		    	}//run
		    }.start();
    } catch (final Exception e) {
      throw new RemoteException("Could not start a graceful Shutdown", e);
    }

  }


  /**
   * Method to graceful stop engine activity
   */
  public void slowGracefulPauseEngine() throws RemoteException {

    try {
    	setNewProfileState(INACTIVE_EXE_PROFILE);
      // log.info("slowGracefulPauseEngine: Cancelling Executor thread [DBConnectionMonitor].");
      // this.executor.shutdown();

      log.info("slowGracefulPauseEngine: Gracefully Shutting down priority queue.,");

      // calculate the number of slots that have sets running..
      final int activeSlots = getExecutionSlotProfileHandler().getActiveExecutionProfile().getNumberOfExecutionSlots()
          - getExecutionSlotProfileHandler().getActiveExecutionProfile().getNumberOfFreeExecutionSlots();

			log.info(activeSlots + " slots are still running  \n" + "thease slots will be allowed to finnish before shutdown");

      // put queue on hold so that no new sets are handed to execution
      // slots
      this.priorityQueue.setActive(false);

      // execution profile is locked so nobody can change profile.
      //getExecutionSlotProfileHandler().lockProfile();

      log.info("\nslowGracefulPauseEngine: closing execution slots\n");

      // wait until all slots are locked..
      while (!getExecutionSlotProfileHandler().getActiveExecutionProfile().areAllSlotsLockedOrFree()) {
        System.out.print(".");
        Thread.sleep(5000);
      }

    } catch (final Exception e) {
      throw new RemoteException("slowGracefulPauseEngine: Could not start a graceful Pause of engine", e);
    }
  }


  /**
   * Method to query server status
   */
  @Override
  public List<String> status() throws RemoteException {

    final List<String> al = new ArrayList<String>();

    al.add("--- ETLC Server ---");
    al.add("");
    al.add("Engine host : "+this.serverHostName);
    al.add("");
    final long now = System.currentTimeMillis();
    long up = now - startedAt;
    final long days = up / (1000 * 60 * 60 * 24);
    up -= (days * (1000 * 60 * 60 * 24));
    final long hours = up / (1000 * 60 * 60);
    al.add("Uptime: " + days + " days " + hours + " hours");

    al.add("");

    if (priorityQueue != null) {
      al.add("Priority Queue");
      String qstat = "  Status: ";
      if (priorityQueue.isActive()) {
        qstat += "active";
      } else {
        qstat += "on hold";
      }
      al.add(qstat);
      al.add("  Size: " + priorityQueue.getNumberOfSetsInQueue());
      al.add("  Poll Period: " + priorityQueue.getPollIntervall());
      al.add("");
    } else {
    	if(!isEngineInActive){
      al.add("Priority Queue disabled");
    	}else{
    		al.add("Priority Queue");
    		String qstat = "  Status: ";
    		qstat += "InActive";
    		al.add(qstat);
    		al.add("  Size: " + "0");
    	    al.add("  Poll Period: " + "NA");
    	    al.add("");
    	}

    }
    if(!isCacheRefreshed){
		al.add(" cache status : not initialized ");
	}else{
		al.add(" cache status : initialized ");
	}	

    try {
      final ExecutionSlotProfile esp = getExecutionSlotProfileHandler().getActiveExecutionProfile();
      al.add("Execution Profile");
      al.add("  Current Profile: " + esp.name());
      al.add("  Execution Slots: " + esp.getNumberOfFreeExecutionSlots() + "/" + esp.getNumberOfExecutionSlots());
    } catch (final Exception e) {
    	if(!isEngineInActive){
			log.finest("Execution slots are not created\n" + e);
			al.add("Execution slots are not created");
    	}else{
    		al.add("Execution Profile");
    		al.add("  Current Profile: " + INACTIVE_EXE_PROFILE);
    		al.add("  Execution Slots: 0/0" );
    	}
    }

    al.add("Java VM");
    final Runtime rt = Runtime.getRuntime();
    al.add("  Available processors: " + rt.availableProcessors());
    al.add("  Free Memory: " + rt.freeMemory());
    al.add("  Total Memory: " + rt.totalMemory());
    al.add("  Max Memory: " + rt.maxMemory());
    al.add("");

		if (al.contains("Priority Queue disabled") || al.contains("Execution slots are not created")) {
			if(!isEngineInActive){
				throw new RemoteException("Engine initialization has not been completed yet");
			}
		}

    return al;
  }

  /**
   * clears the Counting Management Cache.
   *
   * @see com.distocraft.dc5000.etl.engine.main.ITransferEngineRMI#clearCountingManagementCache()
   */
  @Override
	public void clearCountingManagementCache(final String storageId) throws RemoteException {
    try {
      CountingManagementCache.clearCache(storageId);
    } catch (final Exception e) {
      throw new RemoteException("Could not clear the Counting Management Cache", e);
    }
  }

	/**
	 * Method to query currentProfile
	 */
  public String currentProfile() throws RemoteException {
		try {
			final ExecutionSlotProfile esp = getExecutionSlotProfileHandler().getActiveExecutionProfile();
			return esp.name();
		} catch (final Exception e) {
			return e.getMessage();
		}
	}

  @Override
  public void lockExecutionprofile() throws RemoteException {
    getExecutionSlotProfileHandler().lockProfile();
  }

  @Override
  public void unLockExecutionprofile() throws RemoteException {
    getExecutionSlotProfileHandler().unLockProfile();
  }

  /**
   * Method to graceful shutdown engine, priority queue and execution slots.
   * this lets all the execution slots finish their jobs and then exits.
   */
  @Override
  public void fastGracefulShutdown() throws RemoteException {

	  log.info("Shutting down engine...");
      log.info("fastGracefulShutdown: Cancelling Executor thread [DBConnectionMonitor].");
      isEngStopCalled = true ;
      log.info("fastGracefulShutdown: set the isEngStopCalled to true");
      if(isEngineInActive){
    	  log.info("Shutting down of engine completed.");
    	  System.exit(0);
      }

      if(this.executor != null){
    	  this.executor.shutdown();
    	  log.info("Executor thread [DBConnectionMonitor] cancellation status: " + this.executor.isShutdown());
      }

      log.info("Shutting down priority queue");

		if (getExecutionSlotProfileHandler() == null) {
			log.info("Shut down of priority queue aborted.");
			throw new RemoteException(
					"Could not start a graceful Shutdown - there is no SlotProfileHandler object available. Engine is possibly not fully started.");
		}else{
			try {

      // calculate the number of slots that have sets running..
      final int activeSlots = getExecutionSlotProfileHandler().getActiveExecutionProfile().getNumberOfExecutionSlots()
          - getExecutionSlotProfileHandler().getActiveExecutionProfile().getNumberOfFreeExecutionSlots();

      log.info(activeSlots + " slots are still running  \n" + "these slots will be stopped");
      // put queue on hold so that no new sets are handed to execution
      // slots
      this.priorityQueue.setActive(false);

      // execution profile is locked so nobody can change profile.
      getExecutionSlotProfileHandler().lockProfile();

      // broadcast a command to all the sets to be ready for a shutdown.
      eCom.setCommand("shutdown");

      log.info("\nclosing execution slots\n");
		//Commenting out the code to for JIRA EQEV-50223
      
      /*boolean ok = false;
      while (!ok) {

        ok = true;
							final Iterator<String> iter = getExecutionSlotProfileHandler().getActiveExecutionProfile()
            .getAllRunningExecutionSlotSetTypes().iterator();

        while (iter.hasNext()) {

								final String setType = iter.next();
          if (setType.equalsIgnoreCase("adapter")) {
            ok = false;
          }
        }

        System.out.print(".");
        Thread.sleep(5000);
      }*/

			    } catch (final Exception e) {
			      throw new RemoteException("Could not start a graceful Shutdown", e);
			    }
		}//else
		log.info("Shutting down of engine completed.");
	    System.exit(0);
  }
  public void delete(File directory) throws IOException {
	  if(directory.isDirectory()){
  			//directory is empty, then delete it
  			if(directory.list().length==0){
  				directory.delete();
  				log.info("Directory is deleted : "+ directory.getAbsolutePath());
  			} else {
  				//list all the directory contents
  				String files[] = directory.list();
   
  				for (String temp : files) {
  					//construct the file structure
  					File fileDelete = new File(directory, temp);
      		 
  					//recursive delete
  					delete(fileDelete);
  				}
      		
  				//check the directory again, if empty then delete it
  				if(directory.list().length==0){
  					directory.delete();
  					log.info("Directory is deleted : "+ directory.getAbsolutePath());
  				}
  			}
  	} else {
  			//if file, then delete it
  			directory.delete();
  			log.info("File is deleted : "+ directory.getAbsolutePath());
  	}
  }

/**
   * Set new active Execution profile
   * If resetConnMonitor=true and profileName="NoLoads", then DBConnectionMonitor will be disabled.
   * It will not be enabled again until this method is called with resetConnMonitor=true and profileName="Normal"
   * If you want to changeProfile put keep DBConnectionMonitor active, use one of the other setActiveExecutionProfile() methods
   * @see com.distocraft.dc5000.etl.engine.main.ITransferEngineRMI#setActiveExecutionProfile(java.lang.String)
   */
  @Override
  public boolean setActiveExecutionProfile(final String profileName, final boolean resetConnMonitor) throws RemoteException {
    try {

      if (!isInitialized()) {
				log.info("Could not change execution profile, engine is not yet initialized. Waiting engine to initialize itself before changing the profile...");
        while (!isInitialized()) {
          Thread.sleep(5000);
        }
      }

      //Flag the the dbconnection list for reset
      if(resetConnMonitor){
        if (profileName.equalsIgnoreCase("Normal")) {
          log.info("Engine profile is being set to '"+profileName+"' through CLI. DBConnectionMonitor will be re-enabled.");
          log.info("Re-enabling DbConnectionMonitor.");
        } 
        else{
          log.info("Engine profile is being set to '"+profileName+"' through CLI. DBConnectionMonitor will be disabled.");
          log.info("Disabling DbConnectionMonitor.");
        }
        
        DBConnectionMonitor.setExpectedProfile(profileName);

        if(!getPriorityQueue().isActive()){
          //Force the the priority queue active
            getPriorityQueue().setActive(true);
        }
      }

      getExecutionSlotProfileHandler().resetProfiles();
      final boolean result = getExecutionSlotProfileHandler().setActiveProfile(profileName, null);

      getExecutionSlotProfileHandler().writeProfile();
      return result;

    } catch (final Exception e) {
      throw new RemoteException("Could not activate Execution Profile", e);
    }
  }

  @Override
  public boolean setActiveExecutionProfile(final String profileName) throws RemoteException {
    return setActiveExecutionProfile(profileName, true);
  }

  /**
   * Set new active Execution profile
   *
   * @see com.distocraft.dc5000.etl.engine.main.ITransferEngineRMI#setActiveExecutionProfile(java.lang.String,
   *      java.lang.String)
   */
  @Override
  public boolean setActiveExecutionProfile(final String profileName, final String messageText) throws RemoteException {
    try {

      if (!isInitialized()) {
				log.info("Could not change execution profile, engine is not yet initialized. Waiting engine to initialize itself before changing the profile...");
        while (!isInitialized()) {
          Thread.sleep(5000);
        }
      }

      getExecutionSlotProfileHandler().resetProfiles();
      final boolean result = getExecutionSlotProfileHandler().setActiveProfile(profileName, messageText);
      getExecutionSlotProfileHandler().writeProfile();
      return result;

    } catch (final Exception e) {
      throw new RemoteException("Could not activate Execution Profile", e);
    }
  }

  /**
   * Set new active Execution profile and wait untill it has changed complitely.
   *
   * @see com.distocraft.dc5000.etl.engine.main.ITransferEngineRMI#setActiveExecutionProfile(java.lang.String)
   */
  @Override
  public boolean setAndWaitActiveExecutionProfile(final String profileName) throws RemoteException {
    try {

      getExecutionSlotProfileHandler().resetProfiles();
      final boolean result = getExecutionSlotProfileHandler().setActiveProfile(profileName, null);
      getExecutionSlotProfileHandler().writeProfile();

      // wait for clean execution slot.
      while (!getExecutionSlotProfileHandler().isProfileClean()) {
        Thread.sleep(500);
      }

      return result;

    } catch (final Exception e) {
      throw new RemoteException("Could not activate Execution Profile", e);
    }
  }

  @Override
	public Set<String> getAllActiveSetTypesInExecutionProfiles() throws RemoteException {

    try {
      return getExecutionSlotProfileHandler().getActiveExecutionProfile().getAllRunningExecutionSlotSetTypes();
    } catch (final Exception e) {
      throw new RemoteException("Could not retrieve active execution profiles execution slots set types", e);
    }
  }

  @Override
	public Set<Object> getAllRunningExecutionSlotWorkers() throws RemoteException {

    try {
      return getExecutionSlotProfileHandler().getActiveExecutionProfile().getAllRunningExecutionSlotWorkers();
    } catch (final Exception e) {
      throw new RemoteException("Could not retrieve active execution profiles execution slots set types", e);
    }
  }

  /**
   * Set new active Execution profile
   *
   * @see com.distocraft.dc5000.etl.engine.main.ITransferEngineRMI#reloadExecutionProfiles()
   */
  @Override
  public void reloadExecutionProfiles() throws RemoteException {
    try {
      getExecutionSlotProfileHandler().resetProfiles();
    } catch (final Exception e) {
      throw new RemoteException("Could not reload Execution Profiles", e);
    }

  }

  /**
   * Hold a Execution slot. This slot cant execute sets until it is restarted.
   *
   * @see com.distocraft.dc5000.etl.engine.main.ITransferEngineRMI#holdExecutionSlot(int)
   */
  @Override
  public void holdExecutionSlot(final int ExecutionSlotNumber) throws RemoteException {
    try {
      getExecutionSlotProfileHandler().getActiveExecutionProfile().getExecutionSlot(ExecutionSlotNumber).hold();
    } catch (final Exception e) {
      throw new RemoteException("Could not hold a exeution slot", e);
    }
  }

  /**
   * Restart a held up Execution slot.
   */
  @Override
  public void restartExecutionSlot(final int ExecutionSlotNumber) throws RemoteException {
    try {
      getExecutionSlotProfileHandler().getActiveExecutionProfile().getExecutionSlot(ExecutionSlotNumber).restart();
    } catch (final Exception e) {
      throw new RemoteException("Could not restart a execution slot", e);
    }
  }

  /**
   * sets priority queue in hold. No sets are executed untill restarted.
   *
   * @see com.distocraft.dc5000.etl.engine.main.ITransferEngineRMI#holdPriorityQueue()
   */
  @Override
  public void holdPriorityQueue() throws RemoteException {
    try {
      priorityQueue.setActive(false);
    } catch (final Exception e) {
      throw new RemoteException("Could not hold the Priority Queue", e);
    }

  }

  /**
   * restarts priority queue.
   *
   * @see com.distocraft.dc5000.etl.engine.main.ITransferEngineRMI#restartPriorityQueue()
   */
  @Override
  public void restartPriorityQueue() throws RemoteException {
    try {
      priorityQueue.setActive(true);
    } catch (final Exception e) {
      throw new RemoteException("Could not restart the Priority Queue", e);
    }

  }

  public void removeSetFromPriorityQueue(final Long techpackId, final Long setId) {

    final EngineThread set = this.priorityQueue.find(techpackId, setId);

    if (set != null) {
      this.priorityQueue.removeSet(set);
    }

  }

  @Override
  public boolean removeSetFromPriorityQueue(final Long queueId) {

    final EngineThread set = this.priorityQueue.find(queueId);

    if (set != null) {
      return this.priorityQueue.removeSet(set);
    } else {
      return false;
    }

  }

  /**
   * Changes the queue time limit for the set
   *
   * @param queueId
   * @param queueTimeLimit
   * @return
   */
  @Override
  public boolean changeSetTimeLimitInPriorityQueue(final Long queueId, final long queueTimeLimit) {

    final EngineThread set = this.priorityQueue.find(queueId);

    if (set != null) {
      set.setQueueTimeLimit(queueTimeLimit);
      this.priorityQueue.houseKeep();
      return true;
    } else {
      return false;
    }
  }

  @Override
  public boolean changeSetPriorityInPriorityQueue(final Long queueId, final long priority) {

    final EngineThread set = this.priorityQueue.find(queueId);

    if (set != null) {
      return this.priorityQueue.changePriority(set, priority);
    } else {
      return false;
    }

  }

  @Override
  public void holdSetInPriorityQueue(final Long queueId) throws RemoteException {

    final EngineThread set = this.priorityQueue.find(queueId);

    if (set != null) {
      set.setActive(false);
    }

  }

  @Override
  public void activateSetInPriorityQueue(final Long queueId) {

    final EngineThread set = this.priorityQueue.find(queueId);

    if (set != null) {
      set.setActive(true);
    }

  }

  /**
   * return true if given set (in given techpack) is active in exeution slots
   * (running)
   *
   * @see com.distocraft.dc5000.etl.engine.main.ITransferEngineRMI#isSetRunning(java.lang.Long,
   *      java.lang.Long)
   */
  @Override
  public boolean isSetRunning(final Long techpackID, final Long setID) throws RemoteException {
    try {
			final Iterator<ExecutionSlot> iter = getExecutionSlotProfileHandler().getActiveExecutionProfile()
					.getAllExecutionSlots();
			if (iter != null) {
				while (iter.hasNext()) {
					final ExecutionSlot slot = iter.next();
          final EngineThread set = slot.getRunningSet();
          if (set != null && set.getSetID() == setID && set.getTechpackID() == techpackID) {
            return true;
          }
        }
      }

      return false;

    } catch (final Exception e) {
      throw new RemoteException("Error while checking set status", e);
    }

  }

  @Override
  public void reloadTransformations() throws RemoteException {

    // Init Transformer Cache

    RockFactory rdwh = null;
    RockFactory rrep = null;
    RockFactory etlRock = null;

    try {

      String dwh_url = null;
      String dwh_usr = null;
      String dwh_pwd = null;
      String dwh_drv = null;

      String rep_url = null;
      String rep_usr = null;
      String rep_pwd = null;
      String rep_drv = null;

      // check connection to metadata
//      RockFactory etlRock = null;

			etlRock = new RockFactory(etlrep_url, etlrep_usr, etlrep_pwd, etlrep_drv, "ETLEngReloadTransform", true);

      final Meta_databases selO = new Meta_databases(etlRock);
      selO.setType_name("USER");

      final Meta_databasesFactory mdbf = new Meta_databasesFactory(etlRock, selO);

			final Vector<Meta_databases> dbs = mdbf.get();

      for (int i = 0; i < dbs.size(); i++) {

				final Meta_databases mdb = dbs.get(i);

        if (mdb.getConnection_name().equalsIgnoreCase("dwh")) {
          dwh_url = mdb.getConnection_string();
          dwh_usr = mdb.getUsername();
          dwh_pwd = mdb.getPassword();
          dwh_drv = mdb.getDriver_name();
        } else if (mdb.getConnection_name().equalsIgnoreCase("dwhrep")) {
          rep_url = mdb.getConnection_string();
          rep_usr = mdb.getUsername();
          rep_pwd = mdb.getPassword();
          rep_drv = mdb.getDriver_name();
        }

      }

      rdwh = new RockFactory(dwh_url, dwh_usr, dwh_pwd, dwh_drv, "ETLEngReloadTransform", true);
      rrep = new RockFactory(rep_url, rep_usr, rep_pwd, rep_drv, "ETLEngReloadTransform", true);

			final Class<?> tfCacheClass = Class.forName("com.distocraft.dc5000.etl.parser.TransformerCache");
			final Object tfCache = tfCacheClass.newInstance();

			final Class<?>[] argClasses = { rrep.getClass(), rdwh.getClass() };
			final Method revalidateMet = tfCacheClass.getMethod("revalidate", argClasses);

			final Object[] args = { rrep, rdwh };
			revalidateMet.invoke(tfCache, args);

		} catch (ClassNotFoundException e) {
			log.config("Parser module has not been installed. Transformer cache reload cancelled.");
    } catch (final Exception e) {
      throw new RemoteException("Error while revalidating transformer cache", e);
    } finally {
    	try {
            etlRock.getConnection().close();
          } catch (final Exception e) {
          }
      try {
        rdwh.getConnection().close();
      } catch (final Exception e) {
      }
      try {
        rrep.getConnection().close();
      } catch (final Exception e) {
      }
    }

  }

  @Override
  public void updateTransformation(final String tpName) throws RemoteException {

    // Init Transformer Cache

    RockFactory rdwh = null;
    RockFactory rrep = null;
    RockFactory etlRock = null;

    try {

      String dwh_url = null;
      String dwh_usr = null;
      String dwh_pwd = null;
      String dwh_drv = null;

      String rep_url = null;
      String rep_usr = null;
      String rep_pwd = null;
      String rep_drv = null;

      // check connection to metadata
      //RockFactory etlRock = null;

			etlRock = new RockFactory(etlrep_url, etlrep_usr, etlrep_pwd, etlrep_drv, "ETLEngUpdateTransform", true);

      final Meta_databases selO = new Meta_databases(etlRock);
      selO.setType_name("USER");

      final Meta_databasesFactory mdbf = new Meta_databasesFactory(etlRock, selO);

			final Vector<Meta_databases> dbs = mdbf.get();

      for (int i = 0; i < dbs.size(); i++) {

				final Meta_databases mdb = dbs.get(i);

        if (mdb.getConnection_name().equalsIgnoreCase("dwh")) {
          dwh_url = mdb.getConnection_string();
          dwh_usr = mdb.getUsername();
          dwh_pwd = mdb.getPassword();
          dwh_drv = mdb.getDriver_name();
        } else if (mdb.getConnection_name().equalsIgnoreCase("dwhrep")) {
          rep_url = mdb.getConnection_string();
          rep_usr = mdb.getUsername();
          rep_pwd = mdb.getPassword();
          rep_drv = mdb.getDriver_name();
        }

      }

      rdwh = new RockFactory(dwh_url, dwh_usr, dwh_pwd, dwh_drv, "ETLEngUpdateTransform", true);
      rrep = new RockFactory(rep_url, rep_usr, rep_pwd, rep_drv, "ETLEngUpdateTransform", true);

			final Class<?> tfCacheClass = Class.forName("com.distocraft.dc5000.etl.parser.TransformerCache");
			final Method getter = tfCacheClass.getMethod("getCache");

			final Object tfCache = getter.invoke(null);

			final Class<?>[] argClasses = { String.class, rrep.getClass(), rdwh.getClass() };
			final Method updateMet = tfCacheClass.getMethod("updateTransformer", argClasses);

			final Object[] args = { tpName, rrep, rdwh };
			updateMet.invoke(tfCache, args);

		} catch (ClassNotFoundException e) {
			log.config("Parser module has not been installed. Transformer cache reload cancelled.");
    } catch (final Exception e) {
      throw new RemoteException("Error while updating transformer cache", e);
    } finally {
    	try {
            etlRock.getConnection().close();
          } catch (final Exception e) {
          }
      try {
        rdwh.getConnection().close();
      } catch (final Exception e) {
      }
      try {
        rrep.getConnection().close();
      } catch (final Exception e) {
      }
    }

  }

  public void reloadDBLookups() throws RemoteException {

    reloadDBLookups(null);

  }

  @Override
  public void reloadDBLookups(final String tablename) throws RemoteException {

    try {

      log.info("Refreshing DBLookups");

      DBLookupCache.getCache().refresh(tablename);

    } catch (final Exception e) {
      throw new RemoteException("Refreshing DBLookups failed", e);

    }

  }

  /**
   * Refresh caches. Called only during Interface Activation
   */
  
  public void refreshCache() throws RemoteException {
	    try {

	        log.info("Refresh cache");
	  	
	        // Create static properties.
	        StaticProperties.reload();

	        // reload properties
	        Properties.reload();

	        // Read properties
	        getProperties();
	        
	        // Proprietary cache refresh
	        DataFormatCache.getCache().revalidate();
	        PhysicalTableCache.getCache().revalidate();
	        ActivationCache.getCache().revalidate();
	        AggregationRuleCache.getCache().revalidate();
	        MetaDataRockCache.getCache().revalidate();
        
	    } catch (final Exception e) {
	        throw new RemoteException("Refreshing cache failed during Interface Activation", e);

	      }

  }
  
  /**
   * Reloads ETLC config and refresh caches
   */
  @Override
  public void reloadProperties() throws RemoteException {
    try {

      log.info("Reloading configuration");

			Vector<String> loggerOutputlines = new Vector<String>();

      boolean showDetailedLoggingStatus = false;
      try {
        final String logDebugValue = StaticProperties.getProperty("log.debug", "false");
				if (logDebugValue.equalsIgnoreCase("true")) {
          showDetailedLoggingStatus = true;
        }
      } catch (final Exception e) {
        // Don't mind if the value is not found.
      }

			log.finest("--- Logging status before reload ---");
			Enumeration<String> loggerNames = LogManager.getLogManager().getLoggerNames();

			while (loggerNames.hasMoreElements()) {
				final String currentLoggerName = loggerNames.nextElement();
				final Logger currentLogger = LogManager.getLogManager().getLogger(currentLoggerName);

        if (currentLogger != null) {
          final Level loggerLevel = currentLogger.getLevel();
          if (loggerLevel != null) {
            final Logger parentLogger = currentLogger.getParent();
            if (parentLogger != null) {
              loggerOutputlines.add(currentLogger.getName() + " = " + loggerLevel.getName() + " || parent = "
                  + parentLogger.getName());
            } else {
              loggerOutputlines.add(currentLogger.getName() + " = " + loggerLevel.getName() + " || parent = null ");

            }
          } else {
						if (showDetailedLoggingStatus) {
              loggerOutputlines.add(currentLogger.getName() + " = null || parent = "
                  + currentLogger.getParent().getName());

            }
          }
        } else {
          loggerOutputlines.add(currentLoggerName + " was null.");
        }
      }

      // Sort the logger output lines and add them to the status output.
      Collections.sort(loggerOutputlines);
			Iterator<String> loggerOutputlinesIterator = loggerOutputlines.iterator();
      while (loggerOutputlinesIterator.hasNext()) {
				final String currentLoggerOutputline = loggerOutputlinesIterator.next();
				log.finest(currentLoggerOutputline);
      }

      // Create static properties.
      StaticProperties.reload();

      // reload properties
      Properties.reload();

      // Read properties
      getProperties();

      // Proprietary cache refresh
      DataFormatCache.getCache().revalidate();
      PhysicalTableCache.getCache().revalidate();
      ActivationCache.getCache().revalidate();
      AggregationRuleCache.getCache().revalidate();
      MetaDataRockCache.getCache().revalidate();
      
      log.info("Refreshing the BinFormatter");
      BinFormatter.createClasses();

      // Reload logging configurations
      reloadLogging();

			loggerOutputlines = new Vector<String>();
      loggerNames = LogManager.getLogManager().getLoggerNames();

      showDetailedLoggingStatus = false;
      try {
        final String logDebugValue = StaticProperties.getProperty("log.debug", "false");
				if (logDebugValue.equalsIgnoreCase("true")) {
          showDetailedLoggingStatus = true;
        }
      } catch (final Exception e) {
        // Don't mind if the value is not found.
      }

			log.finest("--- Logging status after reload ---");
      while (loggerNames.hasMoreElements()) {
        final String currentLoggerName = loggerNames.nextElement();
        final Logger currentLogger = LogManager.getLogManager().getLogger(currentLoggerName);
        if (currentLogger != null) {
          final Level loggerLevel = currentLogger.getLevel();
          if (loggerLevel != null) {
            final Logger parentLogger = currentLogger.getParent();
            if (parentLogger != null) {
              loggerOutputlines.add(currentLogger.getName() + " = " + loggerLevel.getName() + " || parent = "
                  + parentLogger.getName());
            } else {
              loggerOutputlines.add(currentLogger.getName() + " = " + loggerLevel.getName() + " || parent = null ");
            }
          } else {
						if (showDetailedLoggingStatus) {
              loggerOutputlines.add(currentLogger.getName() + " = null || parent = "
                  + currentLogger.getParent().getName());
            }
          }
        } else {
          loggerOutputlines.add(currentLoggerName + " was null.");
        }
      }

      // Sort the logger output lines and add them to the status output.
      Collections.sort(loggerOutputlines);
      loggerOutputlinesIterator = loggerOutputlines.iterator();
      while (loggerOutputlinesIterator.hasNext()) {
        final String currentLoggerOutputline = loggerOutputlinesIterator.next();
				log.finest(currentLoggerOutputline);
      }

      // reset prioriety queue
      priorityQueue.resetPriorityQueue(this.priorityQueuePollIntervall, this.maxPriorityLevel);

    } catch (final Exception e) {
      throw new RemoteException("Reload config failed", e);
    }

  }

  /**
   * Refreshes the aggregation cache.
   */
  @Override
  public void reloadAggregationCache() throws RemoteException {
    try {
      log.info("Reloading aggregation cache");
      AggregationRuleCache.getCache().revalidate();
    } catch (final Exception e) {
      throw new RemoteException("Reload aggregation cache failed", e);
    }
  }

	/**
	 * AlarmConfigCache contains information about all currently active alarm
	 * reports. Loaders are using this information to see if there is need to
	 * start reduced delay alarm set after loading has been performed. AdminUI
	 * class this method for refreshing the cache.
	 */
	@Override
	public void reloadAlarmConfigCache() throws RemoteException {
		log.info("Reloading alarm cache");

		RockFactory dwhrep = null;

		try {

			dwhrep = getDwhRepRockFactory("ETLEngACInit");

      final Class<?> raccClass = Class.forName("com.ericsson.eniq.etl.alarm.RockAlarmConfigCache");
      final Class<?>[] parameterTypes = { dwhrep.getClass(), log.getClass() };
      final Method initMet = raccClass.getMethod("initialize", parameterTypes);
      final Object arglist[] = new Object[] {dwhrep, log};
      initMet.invoke(null, arglist);

			final Method revalidateMet = raccClass.getMethod("revalidate");
			revalidateMet.invoke(null);
			log.finest("Alarm cache reloaded");
		} catch (final ClassNotFoundException cnfe) {
			throw new RemoteException("Alarm module is not installed.");
		} catch (final Exception e) {
			log.severe("Alarm cache reload failed. " + e.getMessage());
			throw new RemoteException("Alarm cache reload failed", e);
		} finally {
			if (dwhrep != null) {
				try {
					dwhrep.getConnection().close();
				} catch (final Exception e) {
				}
			}
		}

	}
	
	/**
	 * BackupConfigCache contains information about all currently active TECHPACK.
	 * Loaders, Aggregators are using this information to see if there is need to
	 * trigger backup for particular sets. This method for refreshing the cache.
	 */
	@Override
	public void reloadBackupConfigCache() throws RemoteException {
		log.info("Reloading backup cache!");
		try{
			BackupConfigurationCache.getCache().revalidate();
		}catch(Exception e){
			throw new RemoteException("Reload backupConfiguration Cache failed!",e);
		}
	}

  private String getTechpackName(final RockFactory rockFact, final String version, final Long techpackID)
			throws RockException, SQLException {
    // get techpack name
    final Meta_collection_sets whereCollSet = new Meta_collection_sets(rockFact);
    whereCollSet.setEnabled_flag("Y");
    whereCollSet.setVersion_number(version);
    whereCollSet.setCollection_set_id(techpackID);
    final Meta_collection_sets collSet = new Meta_collection_sets(rockFact, whereCollSet);
    final String techPackName = collSet.getCollection_set_name();

    return techPackName;
  }

  private String getSetName(final RockFactory rockFact, final String version, final Long techpackID, final Long setID)
			throws RockException, SQLException {
    // get set name
    final Meta_collections whereColl = new Meta_collections(rockFact);
    whereColl.setVersion_number(version);
    whereColl.setCollection_set_id(techpackID);
    whereColl.setCollection_id(setID);
    final Meta_collections coll = new Meta_collections(rockFact, whereColl);
    final String setName = coll.getCollection_name();

    return setName;

  }

  private String getSetType(final RockFactory rockFact, final String version, final Long techpackID, final Long setID)
			throws RockException, SQLException {
    // get set name
    final Meta_collections whereColl = new Meta_collections(rockFact);
    whereColl.setVersion_number(version);
    whereColl.setCollection_set_id(techpackID);
    whereColl.setCollection_id(setID);
    final Meta_collections coll = new Meta_collections(rockFact, whereColl);
    final String setType = coll.getSettype();

    return setType;

  }

  @Override
  public void giveEngineCommand(final String command) {

    eCom.setCommand(command);

  }

	private String getFailureReason(final RockFactory rockFact, final Long batchID) throws RockException, SQLException {

    final Meta_errors errColl = new Meta_errors(rockFact);
    errColl.setId(batchID);
    final Meta_errors err = new Meta_errors(rockFact, errColl);

    return err.getText();

  }

  /**
   * Returns the executed sets in the ETL engine.
   *
   * @return
   */
  @Override
  public List<Map<String, String>> getExecutedSets() throws java.rmi.RemoteException {

    final List<Map<String, String>> result = new ArrayList<Map<String, String>>();
    RockFactory rockFact = null;
    try {

			rockFact = new RockFactory(etlrep_url, etlrep_usr, etlrep_pwd, etlrep_drv, "ETLEngESets", true, 0);

      final Meta_transfer_batches trBSet = new Meta_transfer_batches(rockFact);
      final Meta_transfer_batchesFactory dbCollections = new Meta_transfer_batchesFactory(rockFact, trBSet);

      final Vector<Meta_transfer_batches> dbVec = dbCollections.get();

      for (Meta_transfer_batches batch : dbVec) {
        final Map<String, String> setMap = new HashMap<String, String>();

				final String setName = getSetName(rockFact, batch.getVersion_number(), batch.getCollection_set_id(),
						batch.getCollection_id());
        final String techPackName = getTechpackName(rockFact, batch.getVersion_number(), batch.getCollection_set_id());
				final String setType = getSetType(rockFact, batch.getVersion_number(), batch.getCollection_set_id(),
						batch.getCollection_id());

        String status;
        if (batch.getFail_flag().equalsIgnoreCase("y")) {
          status = "ok";
        } else {
          status = "failed";
        }

        setMap.put("techpackName", techPackName);
        setMap.put("setName", setName);
        setMap.put("setType", setType);
        setMap.put("startTime", batch.getStart_date().toString());
        setMap.put("endTime", batch.getEnd_date().toString());
        setMap.put("status", status);
        setMap.put("failureReason", "");
        setMap.put("priority", "");
        setMap.put("runningSlot", "");
        setMap.put("runningAction", "");
        setMap.put("version", batch.getVersion_number());

        result.add(setMap);

      }

      return result;

    } catch (final Exception e) {
      throw new RemoteException("Error while fetching executed sets from DB", e);

    } finally {

      try {

        if (rockFact != null && rockFact.getConnection() != null) {
          rockFact.getConnection().close();
        }

      } catch (final Exception e) {

        throw new RemoteException("Error while closing DB connection", e);

      }
    }
  }

  /**
   * Returns the failed sets in the ETL engine.
   *
   * @return
   */
  @Override
	public List<Map<String, String>> getFailedSets() throws java.rmi.RemoteException {

		final List<Map<String, String>> result = new ArrayList<Map<String, String>>();
    RockFactory rockFact = null;
    try {

			rockFact = new RockFactory(etlrep_url, etlrep_usr, etlrep_pwd, etlrep_drv, "ETLEngFSets", true, 0);

      final Meta_transfer_batches trBSet = new Meta_transfer_batches(rockFact);
      trBSet.setFail_flag("Y");
      final Meta_transfer_batchesFactory dbCollections = new Meta_transfer_batchesFactory(rockFact, trBSet);

			final Vector<Meta_transfer_batches> dbVec = dbCollections.get();
      for (int i = 0; i < dbVec.size(); i++) {

				final Meta_transfer_batches batch = dbVec.elementAt(i);
				final Map<String, String> setMap = new HashMap<String, String>();

				final String setName = getSetName(rockFact, batch.getVersion_number(), batch.getCollection_set_id(),
						batch.getCollection_id());
				final String techPackName = getTechpackName(rockFact, batch.getVersion_number(), batch.getCollection_set_id());
				final String setType = getSetType(rockFact, batch.getVersion_number(), batch.getCollection_set_id(),
						batch.getCollection_id());
        final String status = "failed";
				final String failureReason = getFailureReason(rockFact, batch.getId());

        setMap.put("techpackName", techPackName);
        setMap.put("setName", setName);
        setMap.put("setType", setType);
        setMap.put("startTime", batch.getStart_date().toString());
        setMap.put("endTime", batch.getEnd_date().toString());
        setMap.put("status", status);
        setMap.put("failureReason", failureReason);
        setMap.put("priority", "");
        setMap.put("runningSlot", "");
        setMap.put("runningAction", "");
        setMap.put("version", batch.getVersion_number());

        result.add(setMap);

      }

      return result;

    } catch (final Exception e) {
      throw new RemoteException("Error while fetching executed sets from DB", e);
    } finally {

      try {

        if (rockFact != null && rockFact.getConnection() != null) {
          rockFact.getConnection().close();
        }

      } catch (final Exception e) {

        throw new RemoteException("Error while closing DB connection", e);

      }
    }

  }

  /**
   * Returns the queued sets in the ETL engine.
   *
   * @return
   */
  @Override
  public void addWorkerToQueue(final String name, final String type, final Object wobj) throws java.rmi.RemoteException {

    try {

      final EngineThread et = new EngineThread(name, type, new Long(100), wobj, log);

      et.setName(name);

      this.priorityQueue.addSet(et);

    } catch (final Exception e) {

      throw new RemoteException("Error while adding worker set to queue", e);

    }

  }

  /**
   * Returns the queued sets in the ETL engine.
   *
   * @return
   */
  @Override
  public List<Map<String, String>> getQueuedSets() {

    final List<Map<String, String>> result = new ArrayList<Map<String, String>>();
    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    final Iterator<EngineThread> iter = this.priorityQueue.getAll();

    while (iter.hasNext()) {
      final EngineThread set = iter.next();
      final Map<String, String> setMap = new HashMap<String, String>();

      setMap.put("techpackName", set.getTechpackName());
      setMap.put("setName", set.getSetName());
      setMap.put("setType", set.getSetType());
      setMap.put("version", set.getVersion());
      setMap.put("startTime", "");
      setMap.put("endTime", "");
      setMap.put("status", "");
      setMap.put("failureReason", "");
      setMap.put("priority", set.getSetPriority().toString());
      setMap.put("runningSlot", "");
      setMap.put("runningAction", "");
      setMap.put("ID", String.valueOf(set.getQueueID()));
      setMap.put("creationDate", sdf.format(set.getCreationDate()));
      setMap.put("schedulingInfo", formatSchedulingInfo(set.getSchedulingInfo()));
      if (set.isActive()) {
        setMap.put("active", "true");
      } else {
        setMap.put("active", "false");
      }

      final Date earliestExec = set.getEarliestExecution();
      if (earliestExec != null) {
        setMap.put("earliestExec", sdf.format(earliestExec));
      } else {
        setMap.put("earliestExec", "");
      }

      final Date lastExec = set.getLatestExecution();
      if (lastExec != null) {
        setMap.put("lastExec", sdf.format(lastExec));
      } else {
        setMap.put("lastExec", "");
      }

      setMap.put("serviceNode", SERVICE_NODE_NOT_SET_YET);

      result.add(setMap);

    }

    return result;

  }

  /**
   * Returns the running sets in the ETL engine.
   *
   * @return
   */
  @Override
  public List<Map<String, String>> getRunningSets() throws java.rmi.RemoteException {
    return getRunningSets(new ArrayList<String>());
  }
  
  /**
   * Returns the running sets in the ETL engine.
   * @param techPackNames List of tech pack names to get running sets for.
   * @return
   */
  @Override
  public List<Map<String, String>> getRunningSets(final List<String> techPackNames) throws java.rmi.RemoteException {
    final List<Map<String, String>> result = new ArrayList<Map<String, String>>();
    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    try {

      final Iterator<ExecutionSlot> iter = getExecutionSlotProfileHandler().getActiveExecutionProfile()
          .getAllExecutionSlots();

      if (iter != null) {
        while (iter.hasNext()) {
          final ExecutionSlot slot = iter.next();
          final EngineThread set = slot.getRunningSet();

          if (set != null ){

            final Map<String, String> setMap = new HashMap<String, String>();
            // If the list of techPackNames is empty return the set.
            // Also if the running set's tech pack name is in list of techPackNames we're looking for, 
            // return the set: 
            if (listContainsIgnoreCase(techPackNames, set.getTechpackName()) || (techPackNames != null && techPackNames.isEmpty())) {
              setMap.put("setName", set.getSetName());
              setMap.put("techpackName", set.getTechpackName());
            setMap.put("setType", set.getSetType());
            setMap.put("version", set.getVersion());
            setMap.put("startTime", sdf.format(new Date(set.getExecStartTime())));
            setMap.put("endTime", "");
            setMap.put("status", "");
            setMap.put("failureReason", "");
            setMap.put("priority", set.getSetPriority().toString());
            setMap.put("runningSlot", slot.getName());
            setMap.put("runningAction", set.getCurrentAction());
            setMap.put("ID", String.valueOf(set.getQueueID()));
            setMap.put("creationDate", sdf.format(set.getCreationDate()));
            setMap.put("schedulingInfo", formatSchedulingInfo(set.getSchedulingInfo()));
            setMap.put("serviceNode", slot.getSlotType());
            result.add(setMap);
          }
        } else{
      	  System.out.println(timeStampFormat.format(new Date()) +" No sets running at this time.");
        }
      }
      }

    } catch (final Exception e) {
      log.warning("Error while checking sets in Execution slots " + e);
    }
    return result;
  }
  
  /**
   * Check if a list of Strings contains a String.
   * This is case insensitive.
   * @param theList     A list of strings.
   * @param theString   A string to search for.
   * @return  true if the list contains the string (ignoring case).
   */
  public boolean listContainsIgnoreCase(final List<String> theList, final String theString) {
    boolean found = false;
    if (theList == null || theString == null) {
      log.fine(" No sets running at this time.");
      found = false;
    } else {
      Iterator<String> iter = theList.iterator();
      while (iter.hasNext()) {
        final String techPackName = (String) iter.next();
        if (techPackName.trim().equalsIgnoreCase(theString.trim())) {
          found = true;
          break;
        }
      }
    }
    return found;
  }
  

  /**
   * Static method for formatting schedulingInfo string to more human readable
   * form.
   */
  public static String formatSchedulingInfo(final String schedulingInfo) {
    final StringBuilder builder = new StringBuilder();

    if (schedulingInfo != null && schedulingInfo.length() > 0) {

      try {
        final java.util.Properties schedInfo = TransferActionBase.stringToProperties(schedulingInfo);

        final String timelevel = schedInfo.getProperty("timelevel");
        if (timelevel != null) {
          builder.append("Timelevel ").append(timelevel).append(' ');
        }

        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        final String aggDate = schedInfo.getProperty("aggDate");
        if (aggDate != null) {
          builder.append("Date ").append(sdf.format(new Date(Long.valueOf(aggDate)))).append(' ');
        }

        final String lockTable = schedInfo.getProperty("lockTable");
        if (lockTable != null) {
          builder.append("Table ").append(lockTable);
        }

      } catch (EngineMetaDataException emde) {
        builder.append("ERROR: Malformed");
      }

    }

    return builder.toString();

  }

  @Override
  public void reaggregate(final String aggregation, final long datadate) throws RemoteException {
		changeAggregationStatus("MANUAL", aggregation, datadate);

  }

  /**
   *
   * Changes status of a aggregation in a specific date.
   *
   * @param status
   *          new status
   * @param aggregation
   *          aggreagtion
   * @param datadate
   *          date of the aggregation
   * @throws RemoteException
   */
  @Override
  public void changeAggregationStatus(final String status, final String aggregation, final long datadate)
      throws RemoteException {

    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    RockFactory rockFact = null;
    try {

			rockFact = new RockFactory(etlrep_url, etlrep_usr, etlrep_pwd, etlrep_drv, "ETLEngESets", true);

        final AggregationStatus aggSta = AggregationStatusCache.getStatus(aggregation, datadate);
        if (aggSta != null) {

          log.fine("Update aggregation monitorings status to " + status + ": " + aggregation);
          aggSta.STATUS = status;
				// Reset the threshold time value:
				aggSta.THRESHOLD = 0;
          aggSta.LOOPCOUNT = 0;
          AggregationStatusCache.setStatus(aggSta);

          if (status.equalsIgnoreCase("MANUAL")) {

					final Class<?> reaggClass = Class.forName("com.distocraft.dc5000.etl.monitoring.ReAggregation");

					final Class<?>[] constClasses = { log.getClass(), rockFact.getClass() };
					final Constructor<?> reaggCons = reaggClass.getConstructor(constClasses);

					final Object[] cargs = { log, rockFact };
					final Object reagg = reaggCons.newInstance(cargs);

					final Class<?>[] argClasses = { String.class, String.class, long.class };
					final Method reaggMet = reaggClass.getMethod("reAggregate", argClasses);

					final Object[] args = { "MANUAL", aggregation, new Long(datadate) };
					reaggMet.invoke(reagg, args);

				}

			}

		} catch (ClassNotFoundException e) {
			log.config("Monitoring module has not been installed. Reagg cancelled.");
			throw new RemoteException("Monitoring package is not installed.");
		} catch (final Exception e) {
			log.log(
					Level.INFO,
					"Error while setting reAggregation status to " + status + " :" + aggregation + " "
							+ sdf.format(new Date(datadate)), e);
			throw new RemoteException("Error while setting reAggregation status " + status + " :" + aggregation + " "
					+ sdf.format(new Date(datadate)), e);
		} finally {

			try {

				if (rockFact != null && rockFact.getConnection() != null) {
					rockFact.getConnection().close();
				}

			} catch (final Exception e) {
				throw new RemoteException("Error while closing DB connection", e);
      }
    }
  }

  /**
   * main interface for TransferEngine
   */
  public static void main(final String args[]) {

    System.setSecurityManager(new com.distocraft.dc5000.etl.engine.ETLCSecurityManager());
    try {
    	StaticProperties.reload();
    	Velocity.setProperty("resource.loader", "class,file");
		Velocity.setProperty("class.resource.loader.class",
				"org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
		Velocity.setProperty("file.resource.loader.class",
				"org.apache.velocity.runtime.resource.loader.FileResourceLoader");
		Velocity.setProperty("file.resource.loader.path", StaticProperties
				.getProperty("dwhm.templatePath",
						"/dc/dc5000/conf/dwhm_templates"));
		Velocity.setProperty("file.resource.loader.cache", "true");
		Velocity.setProperty("file.resource.loader.modificationCheckInterval",
				"60");
		Velocity.setProperty( RuntimeConstants.RUNTIME_LOG_LOGSYSTEM_CLASS,"org.apache.velocity.runtime.log.AvalonLogChute,org.apache.velocity.runtime.log.Log4JLogChute,org.apache.velocity.runtime.log.CommonsLogLogChute,org.apache.velocity.runtime.log.ServletLogChute,org.apache.velocity.runtime.log.JdkLogChute" );
	    Velocity.setProperty("runtime.log.logsystem.log4j.logger","/eniq/home/dcuser/velocity.log"); 
        Velocity.init(); 
      final Logger log = Logger.getLogger("etlengine.Engine");
      
      try {
      String etlcServerPropertiesFile = System.getProperty(CONF_DIR_DEFAULT);
      if (etlcServerPropertiesFile == null) {
        log.config("System property CONF_DIR not defined. Using default");
        etlcServerPropertiesFile = CONF_DIR_DEFAULT;
      }
      if (!etlcServerPropertiesFile.endsWith(File.separator)) {
        etlcServerPropertiesFile += File.separator;
      }

      etlcServerPropertiesFile += ETLCPROPERTIES;

      log.info("Reading server configuration from \"" + etlcServerPropertiesFile + "\"");

      final java.util.Properties appProps = new ETLCServerProperties(etlcServerPropertiesFile);

      RMIEngineUserPort = Integer.parseInt(appProps.getProperty(ENGINE_RMI_PROCESS_PORT,ENGINE_RMI_PROCESS_PORT_DEFAULT));
      } catch(Exception propEx) {
    	  ExceptionHandler.handleException(propEx, "Cannot read ETLCServer.properties(" + propEx.getMessage() + ")");
          System.exit(2);
      }
      log.info("---------- ETLC engine is initializing ----------");
      

      final TransferEngine tE = new TransferEngine(log);
      tE.getProperties();

			if (!tE.init()) {
        log.severe("Initialisation failed... exiting");
        System.exit(3);
      }

      log.info("ETLC Engine Running...");
      while (true) {
        try {
          synchronized (args) {
            args.wait();
          }
        } catch (final InterruptedException ie) {
        }
      }

    } catch (final Exception e) {
      ExceptionHandler.handleException(e);
    }

  }

  /**
   * This function returns true if this instance of TransferEngine is already
   * initialized.
   *
   * @return Returns true if initialized, otherwise returns false.
   */
  @Override
  public boolean isInitialized() throws java.rmi.RemoteException {
    return (this.isInitialized);
  }

  /**
   * This function reloads the logging configuration from
   * CONF_DIR/engineLogging.properties.
   *
   * @throws java.rmi.RemoteException
   */
  @Override
  public void reloadLogging() throws java.rmi.RemoteException {

    // Relaod logging configurations
    LogManager.getLogManager().reset();

    try {
      LogManager.getLogManager().readConfiguration();
    } catch (final Exception e) {
      throw new java.rmi.RemoteException(
          "IOException occurred: Reading or writing engineLogging.properties failed. Error message: " + e.getMessage());
    }

  }

  @Override
	public List<String> loggingStatus() throws java.rmi.RemoteException {

		final Enumeration<String> loggerNames = LogManager.getLogManager().getLoggerNames();

		final List<String> al = new ArrayList<String>();

    while (loggerNames.hasMoreElements()) {
      final String currentLoggerName = loggerNames.nextElement();
      final Logger currentLogger = LogManager.getLogManager().getLogger(currentLoggerName);
      if (currentLogger != null) {
        final Level loggerLevel = currentLogger.getLevel();
        if (loggerLevel != null) {
          final Logger parentLogger = currentLogger.getParent();
          if (parentLogger != null) {
            al.add(currentLogger.getName() + " = " + loggerLevel.getName() + " || parent = " + parentLogger.getName());
          } else {
            al.add(currentLogger.getName() + " = " + loggerLevel.getName() + " || parent = null ");
          }
        } else {
          al.add(currentLogger.getName() + " = null || parent = " + currentLogger.getParent().getName());
        }
      } else {
        al.add(currentLoggerName + " was null.");
      }
    }

    Collections.sort(al);

    al.add(0, "");
    al.add(0, "--- Logging status ---");

    al.add("");

    return al;
  }

	private void getDWHDBCharsetEncoding(final RockFactory rdwh) throws SQLException {
    PreparedStatement statement = null;
    ResultSet resultSet = null;
    String charsetEncoding = "";
    final String query = "SELECT SUBSTRING(collation_name, 1, CHARINDEX(',', collation_name)-1) AS collation_name FROM sys.syscollation;";

    try {
      statement = rdwh.getConnection().prepareStatement(query);
      resultSet = statement.executeQuery();
      if (resultSet.next()) {
        charsetEncoding = resultSet.getString("collation_name");
        final Share share = Share.instance();
        share.add("dwhdb_charset_encoding", charsetEncoding);
      }

    } finally {
      if (resultSet != null) {
        resultSet.close();
      }

      if (statement != null) {
        statement.close();
      }
    }
  }

  /**
   * Just a helper method to run private executeSet
   *
   * @param et
   */
  public void executeEngineThreadWithListener(final EngineThread et) {
    try {
      this.executeSet(et);
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Executes set with listener via set manager
   *
   * @return - SetStatusTO data that contains status of executed set
   */
  @Override
  public SetStatusTO executeSetViaSetManager(final String collectionSetName, final String collectionName,
      final String ScheduleInfo, final java.util.Properties props) throws RemoteException {
    log.finest("executeSetViaSetManager::colSetName=" + collectionSetName + " setName=" + collectionName);
    try {

      if (this.pluginLoader == null) {
        this.pluginLoader = new PluginLoader(this.pluginPath);
      }

      log.finest("Creating enginethread");

			final EngineThread et = new EngineThread(etlrep_url, etlrep_usr, etlrep_pwd, etlrep_drv, collectionSetName,
          collectionName, this.pluginLoader, ScheduleInfo, SetListener.NULL, log, eCom);
      et.setName(collectionSetName + "_" + collectionName);

      final SetManager setManager = SetManager.getInstance();
      final SetStatusTO status = setManager.executeSet(collectionSetName, collectionName, props, et, this);

      return status;

    } catch (final Exception e) {
      ExceptionHandler.handleException(e);
      throw new RemoteException("Could not start a Set", e);
    }

  }

  /**
   * Returns status info from a set that is executed via setmanager.
   *
   * @return - status object
   */
  @Override
  public SetStatusTO getSetStatusViaSetManager(final String collectionSetName, final String collectionName,
      final int beginIndex, final int count) throws RemoteException {
    log.finest("getSetStatusViaSetManager::colSetName=" + collectionSetName + " setName=" + collectionName);
    final SetManager sm = SetManager.getInstance();
    return sm.getSetStatus(collectionSetName, collectionName, beginIndex, count);
  }

  /**
   * This function configures concurrent worker limitations
   */
  private void configureWorkerLimits(final RockFactory rrep) {
    final Share share = Share.instance();

		try {

			final Map<String, Integer> memoryUsageFactors = getMemoryUsageFactors(rrep);

      share.add("memory_usage_factors", memoryUsageFactors);

			final Map<String, String> regexpsForWorkerLimitations = getRegexpsForWorkerLimitations(rrep);
      share.add("regexps_for_worker_limitations", regexpsForWorkerLimitations);

      final int engineMemoryNeed = getEngineMemoryNeedMB(rrep);

      final Integer maxEngineMemory = (int) getHeapSizeInMB(System.getProperty("HEAP_SIZE"));
      share.add("execution_profile_max_memory_usage_mb", maxEngineMemory - engineMemoryNeed);

    } catch (final Exception e) {
      log.log(Level.WARNING, "Error in worker limitation configuration.", e);
      share.add("max_concurrent_workers", null);
      share.add("regexps_for_worker_limitations", null);
      share.add("execution_profile_max_memory_usage_mb", null);
      log.info("The worker limitation configurations are unset.");
    }

  }

  /**
   * This function parses Engine's heap size that is given in Megabytes or
   * Gigabytes. If the no M or G used then the heap size is read from Runtime's
   * maxMemory method.
   */
  private long getHeapSizeInMB(final String mxHeapSize) {
    long longHeapSize = Long.parseLong(mxHeapSize.substring(0, mxHeapSize.length() - 1));
    final char lastChar = mxHeapSize.charAt(mxHeapSize.length() - 1);

    // if Engine's heap size is given in Gigabytes then calculate it to
    // Megabytes.
    if ('G' == lastChar || 'g' == lastChar) {
      // convert Gigabytes to Megabytes
      longHeapSize = longHeapSize * 1024L;
    } else {
      if ('M' != lastChar && 'm' != lastChar) {
        this.log.warning("Engine heap size is not given in megabytes or in gigabytes: " + mxHeapSize
            + ". Will be using Runtime's maxMemory method's return value instead.");
        // maxMemory returns memory in bytes so convert it to Megabytes.
        longHeapSize = Runtime.getRuntime().maxMemory() / 1024L / 1024L;
      }
    }

    this.log.info("HeapSizeInMegaBytes: " + longHeapSize);

    return longHeapSize;
  }

  /**
   * This function reads worker limitation regular expressions from
   * Configuration table
   *
   * @throws Exception
   */
	private Map<String, String> getRegexpsForWorkerLimitations(final RockFactory rrep) throws SQLException {
		log.info("Getting regexp configurations for worker limitations from Configuration table.");

    final String sqlStr = "SELECT PARAMNAME, PARAMVALUE FROM CONFIGURATION "
        + "WHERE PARAMNAME LIKE 'etlc.workerLimitationRegexp.%'" + " ORDER BY PARAMNAME";

    PreparedStatement statement = null;
    ResultSet resultSet = null;
    final List<Configuration> configuration = new ArrayList<Configuration>();
    try {
      statement = rrep.getConnection().prepareStatement(sqlStr);
      resultSet = statement.executeQuery();

      while (resultSet.next()) {
        final Configuration conf = new Configuration(rrep);
        conf.setParamname(resultSet.getString("PARAMNAME"));
        conf.setParamvalue(resultSet.getString("PARAMVALUE"));
        configuration.add(conf);
      }

		} catch (final SQLException e) {
      log.warning("An error occured on getting regexp configurations for worker limitations.");
      throw e;
    } finally {
      if (resultSet != null) {
        try {
          resultSet.close();
        } catch (final Exception e) {
        }
      }

      if (statement != null) {
        try {
          statement.close();
        } catch (final Exception e) {
        }
      }
    }

		final Map<String, String> regexps = new HashMap<String, String>();
		for (Configuration conf : configuration) {

      final String paramName = conf.getParamname();
      final String shortName = paramName.substring(paramName.lastIndexOf("."));
      final String regexp = conf.getParamvalue();
      regexps.put(shortName, regexp);
			log.info("Key: " + shortName + " configured to have regexp: " + regexp + " for worker limitations.");
    }

    return regexps;
  }

  /**
   * This function reads worker limitation regular expressions from
   * Configuration table
   *
   * @throws Exception
   */
	private Map<String, Integer> getMemoryUsageFactors(final RockFactory rrep) throws SQLException {
		log.info("Getting memory usage factors from Configuration table.");

    final String sqlStr = "SELECT PARAMNAME, PARAMVALUE FROM CONFIGURATION "
        + "WHERE PARAMNAME LIKE 'etlc.MemoryUsageFactor.%'" + " ORDER BY PARAMNAME";

    PreparedStatement statement = null;
    ResultSet resultSet = null;
    final List<Configuration> configuration = new ArrayList<Configuration>();
    try {
      statement = rrep.getConnection().prepareStatement(sqlStr);
      resultSet = statement.executeQuery();

      while (resultSet.next()) {
        final Configuration conf = new Configuration(rrep);
        conf.setParamname(resultSet.getString("PARAMNAME"));
        conf.setParamvalue(resultSet.getString("PARAMVALUE"));
        configuration.add(conf);
      }

		} catch (final SQLException e) {
      log.warning("An error occured on getting memory usage configurations for worker limitations.");
      throw e;
    } finally {
      if (resultSet != null) {
        try {
          resultSet.close();
        } catch (final Exception e) {
        }
      }

      if (statement != null) {
        try {
          statement.close();
        } catch (final Exception e) {
        }
      }
    }

		final Map<String, Integer> memoryUsageFactors = new HashMap<String, Integer>();

		for (Configuration conf : configuration) {

      final String paramName = conf.getParamname();
      final String shortName = paramName.substring(paramName.lastIndexOf("."));
      final String factor = conf.getParamvalue();
      memoryUsageFactors.put(shortName, Integer.valueOf(factor));
			log.info("Key: " + shortName + " configured to have memory usage factor: " + factor + " for worker limitations.");
    }

    return memoryUsageFactors;
  }

  /**
   * This function reads Engine's memory need from Configuration table
   *
   * @throws Exception
   */
	private int getEngineMemoryNeedMB(final RockFactory rrep) throws RockException, SQLException {
    this.log.info("Getting Engine's memory need from Configuration table.");

    String engMemNeed = "512 + 5%";
    try {
      engMemNeed = StaticProperties.getProperty("EngineMemoryNeedMB", "512 + 5%");
    } catch (final Exception e) {
    }

    final Configuration confCond = new Configuration(rrep);
    confCond.setParamname("etlc.EngineMemoryNeedMB");

    final ConfigurationFactory confFactory = new ConfigurationFactory(rrep, confCond);
		final List<Configuration> configuration = confFactory.get();

		for (Configuration conf : configuration) {

      engMemNeed = conf.getParamvalue();
      log.info("Found Engine's memory need from Configuration table: " + engMemNeed);
    }

    int engineMemoryNeedMB = 512;
    if (engMemNeed.contains("+")) {
      try {
        final String[] memNeedParts = engMemNeed.split("\\+");
        final int staticPart = Integer.parseInt(memNeedParts[0].trim());
        final int dynamicAddition = Integer.parseInt(memNeedParts[1].substring(0, memNeedParts[1].indexOf('%')).trim());
        final double multiplier = (double) dynamicAddition / (double) 100;
        final Integer maxEngineMemory = (int) getHeapSizeInMB(System.getProperty("HEAP_SIZE"));
        engineMemoryNeedMB = (int) (staticPart + (multiplier * maxEngineMemory));
        log.info("Calculated memory configuration for engine: " + engineMemoryNeedMB + " from " + engMemNeed);
      } catch (final Exception e) {
        log.warning("Unable to parse the memory configuration for engine. Using default: " + engineMemoryNeedMB);
      }

    } else {
      if (engMemNeed != null) {
        engineMemoryNeedMB = Integer.parseInt(engMemNeed);
      }
    }

    log.info("Engine's memory need is set to: " + engineMemoryNeedMB);
    return engineMemoryNeedMB;
  }


  /**
   * This method will print the execution slot information
   */
  @Override
  public List<Map<String, String>> slotInfo() {
    final List<Map<String, String>> result = new ArrayList<Map<String, String>>();

    try {

      final Iterator<ExecutionSlot> iter = executionSlotHandler.getActiveExecutionProfile().getAllExecutionSlots();

      while (iter.hasNext()) {
        final ExecutionSlot slot = iter.next();

        final Map<String, String> slotMap = new HashMap<String, String>();

        slotMap.put("slotId", String.valueOf(slot.getSlotId()));
        slotMap.put("slotName", slot.getName());
        slotMap.put("hold", String.valueOf(slot.isOnHold()));
        slotMap.put("serviceNode", slot.getSlotType());

        final StringBuilder builder = new StringBuilder();
        for (final Iterator<String> i = slot.getApprovedSettypes().iterator(); i.hasNext();) {
          builder.append(i.next());
          if (i.hasNext()) {
            builder.append(',');
          }
        }

        slotMap.put("setTypes", builder.toString());

        final EngineThread set = slot.getRunningSet();

        if (set != null && set.isAlive()) {
          slotMap.put("tpName", set.getTechpackName());
          slotMap.put("setName", set.getName());
          slotMap.put("setType", set.getSetType());
          slotMap.put("action", set.getCurrentAction());
          slotMap.put("memory", String.valueOf(set.getMemoryConsumptionMB()));
        } else {
          slotMap.put("tpName", "");
          slotMap.put("setName", "");
          slotMap.put("setType", "");
          slotMap.put("action", "");
          slotMap.put("memory", "");
        }

        result.add(slotMap);
      }

    } catch (Exception e) {
      log.log(Level.WARNING, "getSlotInfo failed", e);
    }

    return result;
  }
  
	/**
	 * Method to get list of dependent techpack and interface along with mount info.
	 * 
	 * @param techpackName
	 * @return List of dependent
	 * @throws RemoteException
	 */
	public List<String> getDependentList(final String techpackName)
			throws RemoteException {
		RockFactory dwhRepRock = null;
		RockFactory etlRepRock = null;
		Statement stmtDwhRep = null;
		Statement stmtEtlRep = null;
		ResultSet rSet = null;
		List<String> dependentList = new ArrayList<String>();
		dependentList.add(techpackName);
		String sqlQuery_TD = "select TECHPACKNAME from dwhrep.TechPackDependency where VERSIONID = "
				+ "(select VERSIONID from dwhrep.TPActivation where TECHPACK_NAME = '" + techpackName + "')";
		String sqlQuery_ID = "select INTERFACENAME from dwhrep.InterfaceDependency where TECHPACKNAME = '"
				+ techpackName + "'";

		try {
			// DWHREP Connection
			dwhRepRock = getDwhRepRockFactory("getDependentList_DWH");
			if (dwhRepRock == null) {
				throw new RemoteException("Unable to connect to dwhRepRock database");
			}
			stmtDwhRep = dwhRepRock.getConnection().createStatement();
			rSet = stmtDwhRep.executeQuery(sqlQuery_TD);

			// ETLREP connection
			etlRepRock = getEtlRepRockFactory("getDependentList_ETL");
			if (etlRepRock == null) {
				throw new RemoteException("Unable to connect to etlRepRock database");
			}
			stmtEtlRep = etlRepRock.getConnection().createStatement();

			// Get list of TP
			while (rSet.next()) {
				dependentList.add(rSet.getString("TECHPACKNAME"));
			}

			// rSet will have Interface list
			rSet = stmtDwhRep.executeQuery(sqlQuery_ID);
			final StringBuilder sqlQuery = new StringBuilder();
			sqlQuery.append("SELECT DISTINCT COLLECTION_SET_NAME FROM etlrep.META_COLLECTION_SETS WHERE ENABLED_FLAG = 'Y' ");
			// Build interface with OSS/ENM mount
			if (rSet.next()) {
				sqlQuery.append("AND ");
				sqlQuery.append("(");
				sqlQuery.append("COLLECTION_SET_NAME like '");
				sqlQuery.append(rSet.getString(1));
				sqlQuery.append("-%'");
				while (rSet.next()) {
					sqlQuery.append(" OR ");
					sqlQuery.append("COLLECTION_SET_NAME like '");
					sqlQuery.append(rSet.getString(1));
					sqlQuery.append("-%'");
				}
				sqlQuery.append(")");
			} else {
				log.warning("No interfaces associated with the techpack: '" + techpackName + "'");
			}
			sqlQuery.append("AND type = 'Interface' ORDER BY COLLECTION_SET_NAME");

			// Final list of interface
			rSet = stmtEtlRep.executeQuery(sqlQuery.toString());
			while (rSet.next()) {
				dependentList.add(rSet.getString("COLLECTION_SET_NAME"));
			}
		} catch (final Exception e) {
			log.warning("Cannot Disable Techpack " + techpackName);
			throw new RemoteException("Unable to get dependent list of techpack " + techpackName, e);
		} finally {
			if (rSet != null) {
				try {
					rSet.close();
				} catch (SQLException e) {
					log.warning("Error closing ResultSet during getDependentList : " + e.getMessage());
				}
			}
			if (dwhRepRock != null) {
				try {
					dwhRepRock.getConnection().close();
				} catch (SQLException e) {
					log.warning("Error closing dwhRepRock during getDependentList : " + e.getMessage());
				}
			}
			if (etlRepRock != null) {
				try {
					etlRepRock.getConnection().close();
				} catch (SQLException e) {
					log.warning("Error closing etlRepRock during getDependentList : " + e.getMessage());
				}
			}
			if (stmtDwhRep != null) {
				try {
					stmtDwhRep.close();
				} catch (SQLException e) {
					log.warning("Error closing stmtDwhRep during getDependentList : " + e.getMessage());
				}
			}
			if (stmtEtlRep != null) {
				try {
					stmtEtlRep.close();
				} catch (SQLException e) {
					log.warning("Error closing stmtEtlRep during getDependentList : " + e.getMessage());
				}
			}
		}
		return dependentList;
	}

	/**
	 * Method to disable dependent techpack or interface and reload scheduler
	 * cache.
	 * 
	 * @param dependentList
	 * @throws RemoteException
	 */
	public void disableDependentTP(final List<String> dependentList)
			throws RemoteException {
		// Disable each dependent
		for (String eachDependent : dependentList) {
			disableTechpack(eachDependent);
		}
		// Reload Scheduler cache.
		activateScheduler();
	}

	/**
	 * Method to enable dependent techpack or interface and reload scheduler
	 * cache.
	 * 
	 * @param dependentList
	 * @throws RemoteException
	 */
	public void enableDependentTP(final List<String> dependentList)
			throws RemoteException {
		// Enable each dependent
		for (String eachDependent : dependentList) {
			enableTechpack(eachDependent);
		}
		// Reload Scheduler cache.
		activateScheduler();
	}

	/**
	 * Disable specified techpack by disabling the schedules of the techpack.
	 *
	 * @param techpackName
	 *          Name of the techpack (meta_collection_sets.collection_set_name)
	 * @throws RemoteException
	 *           if disabling fails
	 */
	@Override
	public void disableTechpack(final String techpackName) throws RemoteException {
		final String techpackVersion = getTechpackVersion(techpackName);

		final String qryDisableTechpack = "UPDATE meta_schedulings SET ms.hold_flag = 'Y' "
				+ " FROM meta_collections mc, meta_collection_sets mcs, meta_schedulings ms"
				+ " WHERE mcs.collection_set_name = '" + techpackName + "'" + " AND mcs.version_number = '" + techpackVersion
				+ "'" + " AND mcs.collection_set_id = mc.collection_set_id "
				+ " AND mc.collection_set_id = ms.collection_set_id " + " and mc.collection_id = ms.collection_id ";

		RockFactory etlreprock = null;
		Statement stmtDisableTP = null;

		try {
		  etlreprock = getEtlRepRockFactory("disableTP");
		  if (etlreprock == null) {
		    throw new RemoteException("Unable to connect to database");
		  }
			stmtDisableTP = etlreprock.getConnection().createStatement();
      stmtDisableTP.executeUpdate(qryDisableTechpack);
		} catch (final Exception e) {
			log.warning("Cannot Disable Techpack " + techpackName);
			throw new RemoteException("Unable to disable techpack " + techpackName, e);
		} finally {
			try {
			  stmtDisableTP.close();
			} catch (Exception e) {
			  log.warning("Error closing disable techpack SQL statement: " + e.getMessage());
			}
			try {
			  if (etlreprock != null) {
			    etlreprock.getConnection().close();			    
			  }
			} catch (Exception e) {
			  log.warning("Error closing etlrep connection: " + e.getMessage());
			}
		}
	}

	@Override
	public void disableSet(final String techpackName, final String setName) throws RemoteException {
    final String techpackVersion = getTechpackVersion(techpackName);

    final String qryDisableSet = " update meta_schedulings " + " set ms.hold_flag = 'Y' "
        + " from meta_collections mc, meta_collection_sets mcs, meta_schedulings ms "
        + " where mcs.collection_set_name = '" + techpackName + "'" + " and mcs.version_number = '" + techpackVersion
        + "'" + " and mc.Collection_name = '" + setName + "'" + " and mcs.collection_set_id = mc.collection_set_id "
        + " and mc.collection_set_id = ms.collection_set_id " + " and mc.collection_id = ms.collection_id ";

		RockFactory etlreprock = null;
		Statement stmtDisableSet = null;

		try {
		  etlreprock = getEtlRepRockFactory("disableSet");
		  if (etlreprock == null) {
		    throw new RemoteException("Unable to connect to database");
		  }
			stmtDisableSet = etlreprock.getConnection().createStatement();

      stmtDisableSet.executeUpdate(qryDisableSet);
		} catch (final Exception e) {
			log.warning("Cannot Disable Set " + setName);
			throw new RemoteException("Unable to disable set " + setName, e);
		} finally {
			try {
			  stmtDisableSet.close();
			} catch (Exception e) {
			  log.warning("Error closing disable set SQL statement: " + e.getMessage());
			}
			try {
			  if (etlreprock != null) {
			    etlreprock.getConnection().close();			    
			  }
			} catch (Exception e) {
			  log.warning("Error closing etlrep connection : " + e.getMessage());
			}
		}

	}

	@Override
	public void disableAction(final String techpackName, final String setName, final Integer actionNumber)
			throws RemoteException {
    final String techpackVersion = getTechpackVersion(techpackName);

    final String qryDisableAction = " update meta_transfer_actions " + " set mta.enabled_flag = 'N' "
        + " from meta_collections mc, meta_collection_sets mcs, meta_transfer_actions mta "
        + " where mcs.collection_set_name = '" + techpackName + "' " + " and mcs.version_number = '" + techpackVersion
        + "'" + " and mc.Collection_name = '" + setName + "'" + " and mcs.collection_set_id = mc.collection_set_id"
        + " and mc.collection_set_id = mta.collection_set_id" + " and mc.collection_id = mta.collection_id"
        + " and mta.order_by_no = " + actionNumber + " order by mta.order_by_no asc";

		RockFactory etlreprock = null;
		Statement stmtDisableAction = null;

		try {
		  etlreprock = getEtlRepRockFactory("disableAction");
		  if (etlreprock == null) {
		    throw new RemoteException("Unable to connect to database");
		  }
			stmtDisableAction = etlreprock.getConnection().createStatement();

      stmtDisableAction.executeUpdate(qryDisableAction);
		} catch (final Exception e) {
			log.warning("Cannot Disable Action for techpack " + techpackName);
			throw new RemoteException("Unable to disable action for techpack " + techpackName, e);
		} finally {
			try {
			  stmtDisableAction.close();
			} catch (Exception e) {
			  log.warning("Error closing disable action SQL statement: " + e.getMessage());
			}
			try {
			  if (etlreprock != null) {
			    etlreprock.getConnection().close();			    
			  }
			} catch (Exception e) {
			  log.warning("Error closing etlrep connection : " + e.getMessage());
			}
		}

	}

	@Override
	public void enableTechpack(final String techpackName) throws RemoteException {
    final String techpackVersion = getTechpackVersion(techpackName);

    final String qryEnableTechpack = " update meta_schedulings " + " set ms.hold_flag = 'N' "
        + " from meta_collections mc, meta_collection_sets mcs, meta_schedulings ms "
        + " where mcs.collection_set_name = '" + techpackName + "' " + " and mcs.version_number = '" + techpackVersion
        + "' " + " and mcs.collection_set_id = mc.collection_set_id "
        + " and mc.collection_set_id = ms.collection_set_id " + " and mc.collection_id = ms.collection_id ";

		RockFactory etlreprock = null;
		Statement stmtEnableTP = null;

		try {
		  etlreprock = getEtlRepRockFactory("enableTP");
		  if (etlreprock == null) {
		    throw new RemoteException("Unable to connect to database");
		  }
			stmtEnableTP = etlreprock.getConnection().createStatement();

      stmtEnableTP.executeUpdate(qryEnableTechpack);
		} catch (final Exception e) {
			log.warning("Cannot Enable Techpack " + techpackName);
			throw new RemoteException("Unable to enable techpack " + techpackName, e);
		} finally {
			try {
			  stmtEnableTP.close();
			} catch (Exception e) {
			  log.warning("Error closing enable techpack SQL statement: " + e.getMessage());
			}
			try {
			  if (etlreprock != null) {
			    etlreprock.getConnection().close();			    
			  }
			} catch (Exception e) {
			  log.warning("Error closing etlrep connection: " + e.getMessage());
			}
		}

	}

	@Override
	public void enableSet(final String techpackName, final String setName) throws RemoteException {
    final String techpackVersion = getTechpackVersion(techpackName);

    final String qryEnableSet = " update meta_schedulings " + " set ms.hold_flag = 'N' "
        + " from meta_collections mc, meta_collection_sets mcs, meta_schedulings ms "
        + " where mcs.collection_set_name = '" + techpackName + "' " + " and mcs.version_number = '" + techpackVersion
        + "' " + " and mc.Collection_name = '" + setName + "'" + " and mcs.collection_set_id = mc.collection_set_id "
        + " and mc.collection_set_id = ms.collection_set_id " + " and mc.collection_id = ms.collection_id ";

		RockFactory etlreprock = null;
		Statement stmtEnableSet = null;

		try {
		  etlreprock = getEtlRepRockFactory("enableTP");
		  if (etlreprock == null) {
		    throw new RemoteException("Unable to connect to database");
		  }
			stmtEnableSet = etlreprock.getConnection().createStatement();

      stmtEnableSet.executeUpdate(qryEnableSet);
		} catch (final Exception e) {
			log.warning("Cannot Enable Set " + setName);
			throw new RemoteException("Unable to enable set " + setName, e);
		} finally {
			try {
			  stmtEnableSet.close();
			} catch (Exception e) {
			  log.warning("Error closing enable set SQL statement: " + e.getMessage());
			}
			try {
			  if (etlreprock != null) {
			    etlreprock.getConnection().close();			    
			  }
			} catch (Exception e) {
			  log.warning("Error closing etlrep connection: " + e.getMessage());
			}
		}
	}

	@Override
	public void enableAction(final String techpackName, final String setName, final Integer actionNumber)
			throws RemoteException {
    final String techpackVersion = getTechpackVersion(techpackName);

    final String qryEnableAction = " update meta_transfer_actions " + " set mta.enabled_flag = 'Y' "
        + " from meta_collections mc, meta_collection_sets mcs, meta_transfer_actions mta "
        + " where mcs.collection_set_name = '" + techpackName + "' " + " and mcs.version_number = '" + techpackVersion
        + "'" + " and mc.Collection_name = '" + setName + "'" + " and mcs.collection_set_id = mc.collection_set_id"
        + " and mc.collection_set_id = mta.collection_set_id" + " and mc.collection_id = mta.collection_id"
        + " and mta.order_by_no = " + actionNumber + " order by mta.order_by_no asc";

		RockFactory etlreprock = null;
		Statement stmtEnableAction = null;

		try {
		  etlreprock = getEtlRepRockFactory("enableTP");
		  if (etlreprock == null) {
		    throw new RemoteException("Unable to connect to database");
		  }
			stmtEnableAction = etlreprock.getConnection().createStatement();

      stmtEnableAction.executeUpdate(qryEnableAction);
		} catch (final Exception e) {
			log.warning("Cannot Enable Action for techpack " + techpackName);
			throw new RemoteException("Unable to enable action for techpack " + techpackName, e);
		} finally {
			try {
			  stmtEnableAction.close();
			} catch (Exception e) {
			  log.warning("Error closing enable action SQL statement: " + e.getMessage());
			}
			try {
			  if (etlreprock != null) {
			    etlreprock.getConnection().close();			    
			  }
			} catch (Exception e) {
			  log.warning("Error closing etlrep connection: " + e.getMessage());
			}
		}

	}

	/**
	 * Lists all active interfaces. Used by "engine -e showActiveInterfaces".
	 *
	 * @author esunbal
	 */
  public List<String> showActiveInterfaces() throws RemoteException {
		List<String> interfaceNames = null;

		final String query = "SELECT collection_set_name FROM etlrep.meta_collection_sets, etlrep.meta_schedulings "
				+ "WHERE etlrep.meta_collection_sets.collection_set_id = etlrep.meta_schedulings.collection_set_id "
				+ "AND etlrep.meta_collection_sets.enabled_flag = 'y' and etlrep.meta_schedulings.hold_flag = 'N' "
				//+ "AND etlrep.meta_schedulings.name like 'TriggerAdapter%' "  TR HQ60999
				+ "AND etlrep.meta_collection_sets.collection_set_name like '%-%' "
				+ "AND etlrep.meta_collection_sets.type = 'interface' GROUP BY collection_set_name, type";

		RockFactory etlreprock = null; 
		Statement statement = null;
		ResultSet resultSet = null;

		try {
		  etlreprock = getEtlRepRockFactory("showActiveIfs");
		  if (etlreprock == null) {
		    throw new RemoteException("Unable to connect to database");
		  }
			statement = etlreprock.getConnection().createStatement();
			resultSet = statement.executeQuery(query);

			interfaceNames = new ArrayList<String>();
			while (resultSet.next()) {
				interfaceNames.add(resultSet.getString("collection_set_name"));
			}
		} catch (final Exception e) {
			log.log(Level.INFO, "Listing active interfaces failed exceptionally", e);
		} finally {
			try {
				resultSet.close();
			} catch (Exception e) {
			  log.warning("Error closing result set for active interfaces:" + e.getMessage());
			}
			try {
				statement.close();
			} catch (Exception e) {
			  log.warning("Error closing SQL statement to get active interfaces: " + e.getMessage());
			}
			try {
			  if (etlreprock != null) {
			    etlreprock.getConnection().close();			    
			  }
			} catch (Exception e) {
			  log.warning("Error closing etlrep connection: " + e.getMessage());
			}

		}

		return interfaceNames;
	}

	@Override
	public List<String> showDisabledSets() throws RemoteException {

		RockFactory etlreprock = null;
		final List<String> alSetsActions = new ArrayList<String>(); // returned

		try {
		  etlreprock = getEtlRepRockFactory("showDisabledSets");
		  if (etlreprock == null) {
		    throw new RemoteException("Could not connect to etlrep");
		  }
			final List<String> alTechpackName = new ArrayList<String>();
			final List<String> alTechpackVersion = new ArrayList<String>();

    // used to find all the techpacks and their versions
    final String qryGetAllTechpacks = "select collection_set_name, version_number" + " from meta_collection_sets"
        + " where enabled_flag = 'Y' " + " order by collection_set_name";

			Statement stmtGetAllTechpacks = null;
			ResultSet rsGetAllTechpacks = null;

			try {
				stmtGetAllTechpacks = etlreprock.getConnection().createStatement();
      rsGetAllTechpacks = stmtGetAllTechpacks.executeQuery(qryGetAllTechpacks);

				while (rsGetAllTechpacks.next()) {
					alTechpackName.add(rsGetAllTechpacks.getString("collection_set_name"));
					alTechpackVersion.add(rsGetAllTechpacks.getString("version_number"));
				}
			} catch (final Exception e) {
				log.log(Level.FINE, "Listing techpacks failed", e);
			} finally {
				try {
					stmtGetAllTechpacks.close();
				} catch (Exception e) {
				}
				try {
					rsGetAllTechpacks.close();
				} catch (Exception e) {
				}
			}

      if (rsGetAllTechpacks == null) {
				alSetsActions.add("\n\nNo techpacks installed");
				return alSetsActions;
			}

			// getting the list of techpack's sets on hold
			for (int i = 0; i < alTechpackName.size(); i++) {
				final String qryGetSchedulesOnHold = " select mc.collection_name, ms.hold_flag"
						+ " from meta_collections mc, meta_collection_sets mcs, meta_schedulings ms"
						+ " where mcs.collection_set_name = '" + alTechpackName.get(i) + "'" + " and mcs.version_number = '"
						+ alTechpackVersion.get(i) + "'" + " and mcs.collection_set_id = mc.collection_set_id "
						+ " and mc.collection_set_id = ms.collection_set_id " + " and mc.collection_id = ms.collection_id "
						+ " and ms.hold_flag = 'Y'";

				alSetsActions.add("\n\nDisabled Sets");

        Statement stmtGetSets = null;
        ResultSet rsGetSets = null;

				try {
					stmtGetSets = etlreprock.getConnection().createStatement();
          rsGetSets = stmtGetSets.executeQuery(qryGetSchedulesOnHold);

            while (rsGetSets.next()) {
						alSetsActions.add("\t" + alTechpackName.get(i) + " - " + rsGetSets.getString("collection_name"));
              log.fine("Disable Set: " + alTechpackName.get(i) + " - " + rsGetSets.getString("collection_name"));
            }
				} finally {
					try {
          rsGetSets.close();
					} catch (final Exception e) {
					}
					try {
          stmtGetSets.close();
					} catch (final Exception e) {
					}
				}

				// used to find the disabled actions for the techpacks
				final String qryGetDisabledActions = " select mc.collection_name, mta.transfer_action_name, mta.order_by_no "
						+ " from meta_collections mc, meta_collection_sets mcs, meta_transfer_actions mta "
						+ " where mcs.collection_set_name = '" + alTechpackName.get(i) + "' " + " and mcs.version_number = '"
						+ alTechpackVersion.get(i) + "' " + " and mcs.collection_set_id = mc.collection_set_id "
						+ " and mc.collection_set_id = mta.collection_set_id " + " and mc.collection_id = mta.collection_id "
						+ " and mta.enabled_flag = 'N' " + " order by mta.collection_id, mta.order_by_no asc ";

				alSetsActions.add("\n\nDisabled Actions");

				Statement stmtGetActions = null;
				ResultSet rsGetActions = null;

				try {
					stmtGetActions = etlreprock.getConnection().createStatement();
					rsGetActions = stmtGetActions.executeQuery(qryGetDisabledActions);

            while (rsGetActions.next()) {
						alSetsActions.add("\t" + alTechpackName.get(i) + " - " + rsGetActions.getString("collection_name") + " - "
								+ rsGetActions.getString("transfer_action_name") + " - " + rsGetActions.getString("order_by_no"));

              log.info("Disabled Action: " + alTechpackName.get(i) + " - " + rsGetActions.getString("collection_name")
                  + " - " + rsGetActions.getString("transfer_action_name") + " - "
                  + rsGetActions.getString("order_by_no"));
            }
				} finally {
					try {
          rsGetActions.close();
					} catch (final Exception e) {
					}
					try {
          stmtGetActions.close();
					} catch (final Exception e) {
        }
      }

			} // foreach techpack

		} catch (final Exception e) {
			log.info(e.getMessage());
		} finally {
			try {
			  if (etlreprock != null) {
			    etlreprock.getConnection().close();			    
			  }
			} catch (final Exception e) {
			  log.warning("Error closing etlrep connection" + e.getMessage());
			}
		}

		return alSetsActions;

  }

  @Override
  public void releaseSet(final long queueId) {
    if (this.priorityQueue != null) {
      this.priorityQueue.releaseSet(queueId);
    }
  }

  @Override
  public void releaseSets(final String techpackName, final String setType) {
    if (this.priorityQueue != null && setType != null && techpackName != null) {
      this.priorityQueue.releaseSets(techpackName, setType);
    }
  }

	private String getTechpackVersion(final String techpackName) throws RemoteException {

		String tpVersion = null;

		RockFactory etlreprock = null;

    final String qryGetTechpackVersion = "select version_number" + " from meta_collection_sets"
        + " where enabled_flag = 'Y' " + " and collection_set_name = '" + techpackName + "'";

		Statement stmtGetTechpackVersion = null;
		ResultSet rsGetTechpackVersion = null;

		try {
		  etlreprock = getEtlRepRockFactory("GetTPVers");
		  if (etlreprock == null) {
		    throw new RemoteException("Unable to connect to database");
		  }
			stmtGetTechpackVersion = etlreprock.getConnection().createStatement();
      rsGetTechpackVersion = stmtGetTechpackVersion.executeQuery(qryGetTechpackVersion);

      if (rsGetTechpackVersion != null) {
        try {
          while (rsGetTechpackVersion.next()) {
						tpVersion = rsGetTechpackVersion.getString("version_number");
					}
				} catch (final Exception e) {
					log.log(Level.INFO, "Could not retieve information about the techpack version", e);
				}

			}
		} catch (final Exception e) {
			log.log(Level.INFO, "Could not run query to retrieve techpack version", e);
		} finally {
			try {
			  rsGetTechpackVersion.close();
			} catch (final Exception e) {
			  log.warning("Error closing result set: " + e.getMessage());
			}

			try {
			  stmtGetTechpackVersion.close();
			} catch (final Exception e) {
			  log.warning("Error closing SQL statement: " + e.getMessage());
			}
			
			try {
			  if (etlreprock != null) {
			    etlreprock.getConnection().close();			    
			  }
			} catch (Exception e) {
			  log.warning("Error closing etlrep connection: " + e.getMessage());
			}
		}

		return tpVersion;
	}

	/**
	 * Creates RockFactory object for dwhrep. Creating RockFactory creates an
	 * active database connection. Database connection in the RockFactory object
	 * needs an explicit close when object is no longer needed.
	 *
	 * @param conName
	 *          Name of database connection
	 * @return RockFactory for DWHREP database, null if database connection failed
	 */
	RockFactory getDwhRepRockFactory(final String conName) {
		RockFactory dwhrep = null;

		try {
			dwhrep = new RockFactory(dwhrep_url, dwhrep_usr, dwhrep_pwd, dwhrep_drv, conName, true);
		} catch (final Exception e) {
			log.log(Level.INFO, "Connect to DWHREP failed exceptionally", e);
		}

		return dwhrep;
	}

	/**
	 * Creates RockFactory object for etlrep. Creating RockFactory creates an
	 * active database connection. Database connection in the RockFactory object
	 * needs an explicit close when object is no longer needed.
	 *
	 * @param conName
	 *          Name of database connection
	 * @return RockFactory for ETLREP database, null if database connection failed
	 */
	RockFactory getEtlRepRockFactory(final String conName) {
		RockFactory etlrep = null;

		try {
			etlrep = new RockFactory(etlrep_url, etlrep_usr, etlrep_pwd, etlrep_drv, conName, true);
		} catch (final Exception e) {
			log.log(Level.INFO, "Connect to ETLREP failed exceptionally", e);
		}

		return etlrep;

	}

 @Override
  public void restore(final String techpackName, final List<String> measurementTypes, final String restoreStartDate,
      final String restoreEndDate) throws RemoteException {

    final String RESTORE_SET_PREFIX = "Restore_Count_";


    final Set<String> viewNames = new TreeSet<String>();
    for (String measurementType : measurementTypes) {
      viewNames.add(measurementType.replace(":", "_"));
    }
    
    RockFactory etlrepRockFactory = null;
    try {
      etlrepRockFactory = getEtlRepRockFactory("Restore");
    } catch (Exception exc) {
      log.warning("Error getting etlrep connection: " + exc.getMessage());      
    }
    
    if (etlrepRockFactory == null) {
      throw new RemoteException("Unable to connect to database");
    }
    
    for (String view : viewNames) {
      String restoreSetName = RESTORE_SET_PREFIX + view;
      try {
        // Check restore set exists
        if (isRestoreSetInEtlrep(etlrepRockFactory, restoreSetName)) {
          final java.util.Properties schedinfo = new java.util.Properties();
          schedinfo.setProperty(EngineConstants.LOCK_TABLE, view);
          schedinfo.setProperty(EngineConstants.RESTORE_FROM_TO_DATES, restoreStartDate + ", " + restoreEndDate);
          EngineThread set;

          set = new EngineThread(etlrepRockFactory, techpackName, restoreSetName, pluginLoader, log, eCom);
          set.setSchedulingInfo(TransferActionBase.propertiesToString(schedinfo));
          set.setSetPriority(0L);
          set.setQueueTimeLimit(720L);
          set.setSetType("Restore");
          set.addSetTable(view);
          set.addSetTable(getStorageId(view));
          set.setPersistent(true);
          executeSet(set);
        }
      } catch (EngineMetaDataException e) {
        log.info("Could not execute SQL towards Dwh rep database.");
        log.fine(e.getMessage());
        e.printStackTrace();
      } catch (Exception e) {
        log.info("Could not execute SQL towards Dwh rep database.");
        log.fine(e.getMessage());
      }
    }
  }

  private String getStorageId(final String viewName) {
    String tempFirstPart = viewName.substring(0, viewName.lastIndexOf('_'));
    String tempLastPart = viewName.substring(viewName.lastIndexOf('_') + 1, viewName.length());
    return tempFirstPart + ":" + tempLastPart;
  }

  /**
   * Check restore set exists
   *
   * @param etlrepRockFactory
   * @param restoreSetName
   * @return
   * @throws SQLException
   * @throws RockException
   */
  private boolean isRestoreSetInEtlrep(final RockFactory etlrepRockFactory, final String restoreSetName)
      throws SQLException, RockException {
    boolean isRestoreSetInEtlrep = true;
    final Meta_collections whereMetaCollections = new Meta_collections(etlrepRockFactory);
    whereMetaCollections.setCollection_name(restoreSetName);
    whereMetaCollections.setEnabled_flag(ENABLED_FLAG);
    Meta_collectionsFactory metaCollectionsFactory = new Meta_collectionsFactory(etlrepRockFactory,
        whereMetaCollections);

    if (metaCollectionsFactory.get().isEmpty()) {
      isRestoreSetInEtlrep = false;
      log.info("No set exists for: " + restoreSetName);
    }

    return isRestoreSetInEtlrep;
  }

  /**
   * This method return a list of active tables names based on measurement types
   * for a particular from & to date.
   *
   * @param measurementTypes
   * @param fromDate
   * @param toDate
   * @return List<String>
   * @throws RemoteException
   */
  public List<String> getActiveTables(final List<String> measurementTypes, final String fromDate, final String toDate)
      throws RemoteException {
		final List<String> tables = new ArrayList<String>();
		final long from = Util.dateToMilli(fromDate);
		final long to = Util.dateToMilli(toDate);
    for (String storageID : measurementTypes) {
      tables.addAll(PhysicalTableCache.getCache().getTableName(storageID, from, to));
    }
    return tables;
  }

	/**
   * This method determines if a TechPack is enabled.
	 */
	@Override
	public boolean isTechPackEnabled(final String techPackName, final String techPackType) throws RemoteException {

		RockFactory etlrep = null;
		RockFactory dwhrep = null;

		try {
			dwhrep = getDwhRepRockFactory("isTPEnabled");

			final Tpactivation whereTpActivation = new Tpactivation(dwhrep);
      whereTpActivation.setTechpack_name(techPackName);
      whereTpActivation.setType(techPackType);
      whereTpActivation.setStatus("ACTIVE");
			final TpactivationFactory tpActivationFact = new TpactivationFactory(dwhrep, whereTpActivation);

			final Vector<Tpactivation> tpActivationsVect = tpActivationFact.get();
      if (tpActivationsVect.isEmpty()) {
        log.warning("No TechPack found in TpActivation with Teck Pack Name " + techPackName + ".");
        return false;
      } else {
				final Tpactivation tpActivation = tpActivationsVect.get(0);
        //TECHPACK:VERSION
				final String teckpack_version = tpActivation.getVersionid().split(":")[1];

				etlrep = getEtlRepRockFactory("isTPEnabled");
				final Meta_collection_sets whereMetaCollectSet = new Meta_collection_sets(etlrep);
        whereMetaCollectSet.setEnabled_flag("Y");
        whereMetaCollectSet.setType("Techpack");
        whereMetaCollectSet.setCollection_set_name(techPackName);
        whereMetaCollectSet.setVersion_number(teckpack_version);
				final Meta_collection_setsFactory mcsFact = new Meta_collection_setsFactory(etlrep, whereMetaCollectSet);

				final Vector<Meta_collection_sets> res = mcsFact.get();

				return (res.size() > 0);
			}
		} catch (SQLException e) {
			log.info("Could not execute SQL towards Dwh rep database.");
			log.fine(e.getMessage());
		} catch (RockException e) {
			log.info("Could not connect to Dwh rep database.");
			log.fine(e.getMessage());
		} finally {
			try {
			  if (etlrep != null) {
			    etlrep.getConnection().close();			    
			  }
			} catch (final Exception e) {
			  log.warning("Error closing etlrep connection: " + e.getMessage());
			}
			try {
			  if (dwhrep != null) {
			    dwhrep.getConnection().close();			    
			  }
			} catch (final Exception e) {
			  log.warning("Error closing dwhrep connection: " + e.getMessage());
			}

		}

		return false;
	}

	/**
   * This method get the measurement types for a particular TechPack based on a
   * Regular expression.
	 */
  @Override
  public List<String> getMeasurementTypesForRestore(final String techPackName, final String measurementType)
      throws RemoteException {
    final List<String> result = new ArrayList<String>();
    try {
      final RockFactory dwhrepRockFactory = getDwhRepRockFactory("GetMTypes");
      final Dwhtype whereDWHtype = new Dwhtype(dwhrepRockFactory);
      whereDWHtype.setTechpack_name(techPackName);
      final String tableLevelWhereClause = getTableLevelWhereClause(techPackName);

      final DwhtypeFactory dwhtypeFactory;
      if (measurementType.equalsIgnoreCase("ALL")) {
        dwhtypeFactory = new DwhtypeFactory(dwhrepRockFactory, whereDWHtype, "AND TYPENAME LIKE '%' "
            + tableLevelWhereClause);
      } else {
        whereDWHtype.setTypename(measurementType);
        dwhtypeFactory = new DwhtypeFactory(dwhrepRockFactory, whereDWHtype, tableLevelWhereClause);
      }
      final Vector<Dwhtype> dwhtypeVec = dwhtypeFactory.get();
      if (dwhtypeVec.isEmpty()) {
        log.warning("No MeasurementType(s) found with Regular expression:" + measurementType + ".");
      } else {
        for (Dwhtype dwhType : dwhtypeVec) {
          result.add(dwhType.getStorageid());
        }
      }
    } catch (SQLException e) {
      log.info("Could not execute SQL towards Dwh rep database.");
      log.fine(e.getMessage());
    } catch (RockException e) {
      log.info("Could not connect to Dwh rep database.");
      log.fine(e.getMessage());
    }
    return result;
  }

  /**
   * Get table level where clause, default table level is DAY
   *
   * @param techPackName
   * @return
   */
  private String getTableLevelWhereClause(final String techPackName) {
    // Default is DAY can be a list of tableLevels, e.g. DAY, 15MIN, etc.
    final String tableLevelsToRestore = StaticProperties.getProperty(techPackName + ".tablelevels_to_restore", "DAY");

    final StringBuilder strBuilder = new StringBuilder();
    strBuilder.append("and TABLELEVEL  in ('");

    // Need single quotes around each string for where clause, e.g. and
    // TABLELEVEL in ('DAY','15MIN',)
    strBuilder.append(tableLevelsToRestore.replace(",", "','"));
    strBuilder.append("')");
    return strBuilder.toString();
  }

  @Override
  public List<String> getTableNamesForRawEvents(final String viewName, final Timestamp startTime,
      final Timestamp endTime) throws java.rmi.RemoteException {
		final List<String> storageIds = new ArrayList<String>();
		final List<String> tableNames = new ArrayList<String>();

    if(viewName.endsWith("_ERR_RAW")){
      // Change EVENT_E_SGEH_ERR_RAW to EVENT_E_SGEH_ERR:RAW
      storageIds.add(viewName.replace("_RAW", ":RAW"));
    } else if (viewName.endsWith("_SUC_RAW")) {
      // Change EVENT_E_SGEH_SUC_RAW to EVENT_E_SGEH_SUC:RAW
      storageIds.add(viewName.replace("_RAW", ":RAW"));
    } else if (viewName.endsWith("_RAW")) {
      // Change EVENT_E_SGEH_RAW to EVENT_E_SGEH_SUC:RAW
      storageIds.add(viewName.replace("_RAW", "_SUC:RAW"));
      // Change EVENT_E_SGEH_RAW to EVENT_E_SGEH_ERR:RAW
      storageIds.add(viewName.replace("_RAW", "_ERR:RAW"));
      // Change EVENT_E_TERM_PDP_SESSION_RAW to EVENT_E_TERM_PDP_SESSION:RAW
      storageIds.add(viewName.replace("_RAW", ":RAW"));
    } else {
      throw new RemoteException("Invalid viewName" + viewName);
    }

    for (String storageID : storageIds) {
			final List<String> tableNamesFromCache = PhysicalTableCache.getCache().getTableNamesForRawEvents(storageID,
          startTime.getTime(), endTime.getTime());
      if (!tableNamesFromCache.isEmpty()) {
        tableNames.addAll(tableNamesFromCache);
      }
    }

    return tableNames;
  }

  @Override
  public List<String> getLatestTableNamesForRawEvents(final String viewName) throws java.rmi.RemoteException {
		final List<String> storageIds = new ArrayList<String>();
		final List<String> tableNames = new ArrayList<String>();

    if (viewName.endsWith("_ERR_RAW")) {
      // Change EVENT_E_SGEH_ERR_RAW to EVENT_E_SGEH_ERR:RAW
      storageIds.add(viewName.replace("_RAW", ":RAW"));
    } else if (viewName.endsWith("_SUC_RAW")) {
      // Change EVENT_E_SGEH_SUC_RAW to EVENT_E_SGEH_SUC:RAW
      storageIds.add(viewName.replace("_RAW", ":RAW"));
    } else if (viewName.endsWith("_RAW")) {
      // Change EVENT_E_SGEH_RAW to EVENT_E_SGEH_SUC:RAW
      storageIds.add(viewName.replace("_RAW", "_SUC:RAW"));
      // Change EVENT_E_SGEH_RAW to EVENT_E_SGEH_ERR:RAW
      storageIds.add(viewName.replace("_RAW", "_ERR:RAW"));
      // Change EVENT_E_TERM_PDP_SESSION_RAW to EVENT_E_TERM_PDP_SESSION:RAW
      storageIds.add(viewName.replace("_RAW", ":RAW"));
    } else {
      throw new RemoteException("Invalid viewName " + viewName);
    }

    for (String storageID : storageIds) {
			final String tableName = PhysicalTableCache.getCache().getLatestTableNameForRawEvents(storageID);
      if (!tableName.isEmpty()) {
        tableNames.add(tableName);
      }
    }

    return tableNames;
  }

  @Override
  public void manualCountReAgg(final String techPackName, final Timestamp minTimestamp, final Timestamp maxTimestamp,
      final String intervalName, final boolean isScheduled) throws RemoteException {
		final java.util.Properties schedinfo = new java.util.Properties();
    schedinfo.setProperty("minTimestamp", minTimestamp.toString());
    schedinfo.setProperty("maxTimestamp", maxTimestamp.toString());
    schedinfo.setProperty("intervalName", intervalName);
    schedinfo.setProperty("isScheduled", Boolean.toString(isScheduled));

    EngineThread set;
    RockFactory etlrep = null;
    try {
      etlrep = getEtlRepRockFactory("manCountReAgg");
      if (etlrep == null) {
        throw new RemoteException("Unable to connect to database");
      }
			set = new EngineThread(etlrep, techPackName, "CountingReAgg_" + techPackName,
					pluginLoader, log, eCom);
      set.setSchedulingInfo(TransferActionBase.propertiesToString(schedinfo));
      set.setSetPriority(0L);
      set.setQueueTimeLimit(720L);
      set.setSetType("Count");
      set.setPersistent(true);
      executeSet(set);
    } catch (EngineMetaDataException e) {
      log.info("Could not execute SQL towards Dwh rep database.");
      log.fine(e.getMessage());
      e.printStackTrace();
    } catch (Exception e) {
      log.info("Could not execute SQL towards Dwh rep database.");
      log.fine(e.getMessage());
    }
  }

  @Override
  public boolean isIntervalNameSupported(final String intervalName) {
    boolean isSupported = false;
		final String supportedIntervals = StaticProperties.getProperty("SUPPORTED_INTERVALS", "DAY");

    for (String supportedInterval : supportedIntervals.split(",")) {
      if (supportedInterval.equalsIgnoreCase(intervalName)) {
        log.finest("Interval name = '" + intervalName + "' is supported.");
        isSupported = true;
        break;
      }
    }
    return isSupported;
  }

  @Override
  public long getOldestReAggTimeInMs(final String techPackName) {
		final long MS_IN_DAY = 1000 * 60 * 24 * 60;

		final long currentTime = System.currentTimeMillis();
		final long reAggBufferInDays = Long.parseLong((StaticProperties.getProperty("REAGG_BUFFER_IN_DAYS", "2")));
		final long reAggBufferInMs = reAggBufferInDays * MS_IN_DAY;
		final long earliestTimeForRawEvents = getEarliestTimeForRawEvents(techPackName, currentTime);

    long earliestTime = earliestTimeForRawEvents + reAggBufferInMs;

    // If (earliestTime + 1 day) is greater than or equal to current time, then
    // just use the earliestTimeForRawEvents
    if (earliestTime >= currentTime) {
      earliestTime = -1;
    }
    return earliestTime;
  }

  @Override
  public Map<String, String> serviceNodeConnectionInfo() throws RemoteException {
    return DBConnectionMonitor.getConnectionMonitorDetails();
  }

  private long getEarliestTimeForRawEvents(final String techPackName, final long currentTime) {
    long earliestTime = currentTime;
    try {
			final RockFactory dwhrepRockFactory = getDwhRepRockFactory("EarliestRaw");
      final Dwhtype whereDWHtype = new Dwhtype(dwhrepRockFactory);
      whereDWHtype.setTechpack_name(techPackName);
      whereDWHtype.setTablelevel("RAW");
      final DwhtypeFactory dwhtypeFactory = new DwhtypeFactory(dwhrepRockFactory, whereDWHtype);
			final Vector<Dwhtype> dwhtypeVec = dwhtypeFactory.get();

      if (dwhtypeVec.isEmpty()) {
        log.warning("No MeasurementType(s) found for Table Level equal to RAW.");
      } else {
        for (Dwhtype dwhType : dwhtypeVec) {
					final String storageId = dwhType.getStorageid();
					final long currentEarliestTime = PhysicalTableCache.getCache().getEarliestTimeForRawEvents(storageId);
          if (currentEarliestTime < earliestTime) {
            earliestTime = currentEarliestTime;
          }
        }
      }
    } catch (SQLException e) {
      log.info("Could not execute SQL towards Dwh rep database.");
      log.fine(e.getMessage());
    } catch (RockException e) {
      log.info("Could not connect to Dwh rep database.");
      log.fine(e.getMessage());
    }
    return earliestTime;
  }

	// ------>>>>------ LICENSING STUFF ------>>>>------

	/**
	 * Checks if the system has valid license to start.
	 *
	 * @exception Exception
	 *              is thrown is no valid license available
	 */
	private final void checkLicense() throws LicensingException {
		log.info("Performing licensing check. LicenseManager:" + LICENCE_HOSTNAME + ")");

		final LicensingCache cache = connectLicensingCache();

		if (cache == null) {
			throw new LicensingException("Unable to connect to LicenseManager @ " + LICENCE_HOSTNAME);
		}

		final int cpus = DefaultLicensingCache.getNumberOfPhysicalCPUs(false/*Use the lwp helper if available i.e dont spawn the process from engines JVM*/);

		// Check the starter and capacity of stats licenses first as there is no concept of capacity in events
		
		
		
		if (checkStarterLicense(cache, ENIQ_STARTER_LICENSE)) {
			if (checkCapacityLicense(cache, ENIQ_CAPACITY_LICENSE, cpus)) {
				log.info("Proceeding with valid ENIQ Statistics Starter and Capacity license");
			} else {
				throw new LicensingException("Valid Capacity license dose not exist");
			}
		} else if (checkStarterLicense(cache, EVENTS_STARTER_LICENSE)) {
			log.info("Proceeding with ENIQ Events Starter license");
		} else if (checkStarterLicense(cache, SONV_STARTER_LICENSE)) {
			log.info("Proceeding with SONV Starter license");
		} else {
			throw new LicensingException("Valid Starter license dose not exist");
		}
		
	}

	/**
	 * Check validity of specified capacity license with specified capacity.
	 *
	 * @return true if license is valid
	 */
	private final boolean checkCapacityLicense(final LicensingCache cache, final String capacityLicense,
			final int capacity) {

		boolean isValid = false;

		try {
			final LicenseDescriptor license = new DefaultLicenseDescriptor(capacityLicense);
	        /*
	         For Eniq 11, check the capacity with number of physical CPUs:
		     setCapacity is called to set the actual capacity of server.
		     if setCapacity method is not called. By default capacity is -1
		     and no capacity check will be done in DefaultLicensingCache
		     */

			license.setCapacity(capacity);
			final LicensingResponse response = cache.checkCapacityLicense(license, capacity);

			isValid = response.isValid();

		} catch (Exception e) {
			log.log(Level.WARNING, "Capacity license validation failed", e);
		}

		return isValid;

	}

	/**
	 * Checks validity of specified starter licence
	 *
	 * @return true if license if valid
	 */
	private boolean checkStarterLicense(final LicensingCache cache, final String starterLicense) {

		boolean isValid = false;

		try {
			final LicenseDescriptor license = new DefaultLicenseDescriptor(starterLicense);
			final LicensingResponse response = cache.checkLicense(license);

			isValid = response.isValid();

			if (isValid) {

				log.fine("Checking if starter license is time-restricted");

				for (LicenseInformation li : cache.getLicenseInformation()) {
					if (li.getFeatureName().equalsIgnoreCase(starterLicense)) {
						if (li.getDeathDay() > 0) {
							log.fine("Expiry date in long of starter license: " + li.getDeathDay());
							if (li.getDeathDay() < System.currentTimeMillis()) {
								log.severe("Capacity license " + starterLicense + " is expired.");
							} else {
								log.info("Starter license is not an unlimited expiry license. License validity will rechecked be every day at 4 am.");

								final EngineLicenseCheckTask checkLicense = new EngineLicenseCheckTask();
								final Timer timer = new Timer();
								timer.scheduleAtFixedRate(checkLicense, checkLicense.getExecTime(), checkLicense.getExecInterval());

								isValid = true;
							}
						} else {
							log.info("Unlimited expiry started license successfully validated.");
							isValid = true;
						}
						break;
					}
				}

			} // not valid

		} catch (Exception e) {
			log.log(Level.WARNING, "Starter license validation failed", e);
		}

		if (isValid) {
			activeStarterlicense = starterLicense;
		}

		return isValid;
	}

	/**
	 * Returns connection to LicensingCache (licmgr).
	 *
	 * @return cache The LicensingCache
	 * @throws Exception
	 *           if connection to licensing cache fails
	 */
	private LicensingCache connectLicensingCache() throws LicensingException {
		try {
      log.fine("Looking for " + LICENCE_REFNAME + " on " + LICENCE_HOSTNAME+":"+LICENCE_PORT);
      final Registry reg = LocateRegistry.getRegistry(LICENCE_HOSTNAME, Integer.valueOf(LICENCE_PORT));
      return (LicensingCache)reg.lookup(LICENCE_REFNAME);
		} catch (Exception e) {
			throw new LicensingException("Unable to connect to LicenseManager. Pls check that licmgr is running.", e);
		}
	}

  public String getActiveStarterlicense() {
    return activeStarterlicense;
  }

  public void setActiveStarterlicense(final String activeStarterlicense) {
    this.activeStarterlicense = activeStarterlicense;
  }

	/**
	 * TimerTask implementation to check the ENIQ starter license everyday at 4
	 * a.m. and if the license expired initiate a graceful shutdown of the engine.
	 *
	 * @author esunbal
	 */
	class EngineLicenseCheckTask extends TimerTask {

		/**
		 * Main logic for the timer task.
		 */
		@Override
		public void run() { // TODO
			try {
				log.info("Getting license information from LicenseManager @ " + serverHostName);

				final LicensingCache cache = connectLicensingCache();

				if (cache == null) {
					log.severe("Could not verify Starter license");
					// TODO: Shutdown?
				} else {
					final LicenseDescriptor license = new DefaultLicenseDescriptor(activeStarterlicense);
          final LicensingResponse response;
          if(ENIQ_STARTER_LICENSE.equals(activeStarterlicense)){
            final int cpus = DefaultLicensingCache.getNumberOfPhysicalCPUs(false/*Use the lwp helper if available i.e dont spawn the process from engines JVM*/);
            response = cache.checkCapacityLicense(license, cpus);
          } else {
            response = cache.checkLicense(license);
          }

					if (response.isValid()) {
						log.info("Starter license successfully verified");
					} else {
						log.severe("Starter license '"+activeStarterlicense+"' no longer valid:  " +
              "Shutting down the engine gracefully.");
						slowGracefulShutdown();
					}
				}

			} catch (final Exception e) {
				log.log(Level.WARNING, "EngineLicenseCheckTask failed exceptionally", e);
			}
		}

		/**
		 * Returns execution period (one day) in ms
		 */
		private long getExecInterval() {
			return 1000 * 60 * 60 * 24;
		}

		/**
		 * Makes a timestamp for tomorrow morning 04:00
		 */
		private Date getExecTime() {
			final Calendar tomorrow = new GregorianCalendar();
			tomorrow.add(Calendar.DATE, 1);
			tomorrow.set(Calendar.HOUR, 4);
			tomorrow.set(Calendar.MINUTE, 0);

			return tomorrow.getTime();
		}

	}
	


	// For JUnits.
	protected void setTimer(ScheduledThreadPoolExecutor executor) {
		this.executor = executor;
	};
  
  @Override
  public void removeTechPacksInPriorityQueue(List<String> techPackNames) {

    try {
      // Get all of the sets on the priority queue:
      Iterator<EngineThread> setsInQueue = getPriorityQueue().getAll();

      // For each set in the priority queue, check if its tech pack name is one
      // we need to remove.
      while (setsInQueue.hasNext()) {
        final EngineThread set = setsInQueue.next();
        if ( set != null ) {
	        for (String techPackToRemove : techPackNames) {
	          if (set.getTechpackName().equalsIgnoreCase(techPackToRemove)) {
	            log.warning("Removing set from priority queue: " + set.getSetName() + ", techpack: "
	                + set.getTechpackName());
	            getPriorityQueue().removeSet(set);
	            // This matched so no need to check for the other tech packs in the
	            // list:
	            break;
	          }
	        }
      	}
      }
    } catch (Exception exc) {
      log.warning("Error putting sets on hold: " + exc.toString());
      System.out.println(timeStampFormat.format(new Date()) + "   Error while putting sets on hold:" + exc.toString());
    }
  }

  @Override
  public void killRunningSets(List<String> techPackNames) {

    try {
      // Get the running slots:
      ExecutionSlotProfile slotProfile = getExecutionSlotProfileHandler().getActiveExecutionProfile();
      Iterator<ExecutionSlot> runningSlots = slotProfile.getAllRunningExecutionSlots();

      boolean killedSets = killSets(techPackNames, runningSlots);
      int retries = 3;
      while (killedSets && retries >= 1) {
        runningSlots = slotProfile.getAllRunningExecutionSlots();
        // Run this again to make sure everything is killed:
        killedSets = killSets(techPackNames, runningSlots);
        Thread.sleep(1000);
        retries--;
      }
    } catch (Exception exc) {
      log.warning("Error killing sets in execution: " + exc.toString());
      System.out.println(timeStampFormat.format(new Date()) + "   Error while killing sets in Execution slots:" + exc.toString());
    }
  }

  /**
   * Kill list of running sets that match a list of tech pack names.
   * @param techPackNames
   * @param runningSlots
   * @return true if sets were killed
   */
  private boolean killSets(List<String> techPackNames, Iterator<ExecutionSlot> runningSlots) {
    // boolean to keep track of whether any sets were found to kill: 
    boolean killedSets = false;
    
    // For each running set, check if the tech pack name is one we need to
    // kill.
    // If it is, call interrupt on the thread (EngineThread):
    while (runningSlots.hasNext()) {
      final EngineThread runningSet = getRunningSet(runningSlots.next());
      if (runningSet != null) {
      final String techpackNameRunning = runningSet.getTechpackName();
        for (String techPackToKill : techPackNames) {
          if (techpackNameRunning.equalsIgnoreCase(techPackToKill)) {
            log.warning("Killing set : " + runningSet.getSetName() + ", techpack: " + runningSet.getTechpackName());
            runningSet.interrupt();
            killedSets = true;
            break;
          }
        }
      }
    }
    return killedSets;
  }
  
  /* (non-Javadoc)
   * @see com.distocraft.dc5000.etl.engine.main.ITransferEngineRMI#lockEventsUIusers(boolean)
   */
  @Override
  public void lockEventsUIusers(final boolean lock) throws RemoteException {

    final String SQL_MODIFY = "UPDATE ENIQ_EVENTS_ADMIN_PROPERTIES SET PARAM_VALUE=? WHERE PARAM_NAME='ENIQ_EVENTS_LOCK_USERS'";


    Connection repCon = null;
    PreparedStatement stmnt = null;

    try {
      // Create also the connection to dwhrep.
      final RockFactory dwhreprock = getDwhRepRockFactory("lockEventsUIusers");
      repCon = dwhreprock.getConnection();
      stmnt = repCon.prepareStatement(SQL_MODIFY);

      if (lock) {
        stmnt.setString(1, "true");
      } else {
        stmnt.setString(1, "false");
      }
      log.info("Switching lock for Events UI users in ENIQ_EVENTS_ADMIN_PROPERTIES: " + lock);
      stmnt.executeUpdate();
    } catch (SQLException exc) {
      log.warning("Locking/unlocking Events UI users failed.");
      exc.printStackTrace();
    } finally {
      try {
        if (repCon != null) {
          repCon.commit();
        }
        if (stmnt != null) {
          stmnt.close();
        }
      } catch (Exception exc) {
        log.warning("Exception: " + exc.getMessage());
      }
    }
  }
  
  /**
   * Get the running set from an execution slot.
   * @param slot
   * @return The running set.
   */
  protected EngineThread getRunningSet(ExecutionSlot slot) {
    return slot.getRunningSet();
  }

  @Override
  public void triggerRestoreOfData() throws RemoteException {
	
	  System.out.println("creating flags");
	  new TriggerDataRestoreProcess();
  }

	@Override
	public int getFreeExecutionSlots() throws RemoteException {
		return getExecutionSlotProfileHandler().getActiveExecutionProfile().getNumberOfFreeExecutionSlots();

	}

	@Override
	public int getFreeExecutionSlots(String setType) throws RemoteException {
		return getExecutionSlotProfileHandler().getActiveExecutionProfile().getNumberOfFreeSlotsforSetType(setType);
	}
}
