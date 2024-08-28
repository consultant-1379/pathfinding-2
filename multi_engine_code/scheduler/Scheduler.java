package com.distocraft.dc5000.etl.scheduler;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import com.distocraft.dc5000.common.ENIQRMIRegistryManager;
import com.distocraft.dc5000.common.ServicenamesHelper;
import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.common.monitor.DependentService;
import com.distocraft.dc5000.common.monitor.PlatformServices;
import com.distocraft.dc5000.common.monitor.ServiceMonitor;
import com.ericsson.eniq.repository.ETLCServerProperties;
import com.ericsson.eniq.scheduler.exception.SchedulerException;

public class Scheduler extends UnicastRemoteObject implements ISchedulerRMI,DependentService {

  private static final long serialVersionUID = 3493067702136629615L;

  public static final String STATUS_EXECUTED = "Executed";

  public static final String STATUS_FAILED = "Exec failed!";

  private static Logger log = Logger.getLogger("scheduler");

  private String serverHostName;

  private int serverPort;

  private String serverRefName;

  private String url = "";

  private String userName = "";

  private String password = "";
  
  private static final String SCHEDULER_RMI_PROCESS_PORT = "SCHEDULER_RMI_PROCESS_PORT";
  
  private static final String SCHEDULER_RMI_PROCESS_PORT_DEFAULT = "60002";
  
  private static int RMISchedulerUserPort = 60002;

  private String dbDriverName = "";

  // Time to connect again to the database if it fails
  private long reConnectTime;

  private transient SchedulerThread schedulerThread;

  private long pollIntervall;

  private int penaltyWait;

  //private String engineURL;
  
  private boolean isRepdbOnline = true;
  
  private boolean isEngineOnline = true;
  
  private static final Object mutex = new Object();
  
  Lock lock = new ReentrantLock(true);
  
  /**
   * Constructor for starting the transfer
   */
  public Scheduler() throws RemoteException {
    this(RMISchedulerUserPort);
    

    log.info("---------- ETLC scheduler is initializing ----------");

  }
  
  /**
   * Constructor for starting the transfer
   */
  public Scheduler(int RMISchedulerProcessPort) throws RemoteException {
    super(RMISchedulerProcessPort);
    

    log.info("---------- ETLC scheduler is initializing ----------");

  }

  /**
   * (re)start scheduler thread
   */
  public void reload() throws RemoteException {
	  
	  lock.lock();
    try {
    	

      boolean reload = false;

      if (this.schedulerThread != null) {
    	// Wait till schState is completed. 
  		while(!SchedulerThread.schedulerState){
  			log.log(Level.INFO, "Another thread in the middle of processing. Waiting till it finishes...");
  			Thread.sleep(5 * 1000);
  		}
  		log.log(Level.INFO, "No thread in processing. Proceeding ahead for reload...");
  		
  		// Cancel/Kill the current scheduler thread.
          this.schedulerThread.cancel();
        reload = true;
      }

      this.schedulerThread = new SchedulerThread(this.pollIntervall, this.penaltyWait,
          this.reConnectTime, url, dbDriverName, userName, password);

      this.schedulerThread.start();
      
      if (reload) {
        log.info("Reloaded");
      } else {
        log.info("Loaded");
      }

    } catch (Exception e) {
      log.log(Level.WARNING, "Reload failed exceptionally", e);
      throw new RemoteException("Reload failed exceptionally", e);
    }
    finally {
    	   lock.unlock();
    	}

    
  }

  /**
   * Places scheduler on hold.
   * 
   * @exception RemoteException
   */
  public void hold() throws RemoteException {

    try {

      if (this.schedulerThread != null) {
    	// Wait till schState is completed. 
		while(!SchedulerThread.schedulerState){
			log.log(Level.INFO, "Another thread in the middle of processing. Waiting till it finishes...");
			Thread.sleep(5 * 1000);
		}
		log.log(Level.INFO, "No thread in processing. Proceeding ahead for hold...");
		
		// Cancel/Kill the current scheduler thread.
        this.schedulerThread.cancel();

        log.info("On hold");
      } else {
        log.info("Allready on hold");
      }

      this.schedulerThread = null;

    } catch (Exception e) {
      log.log(Level.WARNING, "Hold failed exceptionally", e);
      throw new RemoteException("Hold failed exceptionally", e);
    }

  }

  /**
   * Method to shutdown Schedule.
   */
  public void shutdown() throws RemoteException {

    try {
		// if threads are running, close em, and shut down
		if (this.schedulerThread != null) {
			// Wait till schState is completed. 
			while(!SchedulerThread.schedulerState){
				log.log(Level.INFO, "Another thread in the middle of processing. Waiting till it finishes...");
				Thread.sleep(5 * 1000);
			}
			log.log(Level.INFO, "No thread in processing. Proceeding ahead for shutdown...");
			
			// Cancel/Kill the current scheduler thread.
			this.schedulerThread.cancel();
		}
	  	//Adding the logic to avoid the exception: java.io.EOFException, while exiting the application
	  	log.info("Shuting Down Scheduler...");
	    ENIQRMIRegistryManager rmi = new ENIQRMIRegistryManager(this.serverHostName, this.serverPort);
	    try {
	     	Registry registry = rmi.getRegistry();
	        registry.unbind(this.serverRefName);
	        UnicastRemoteObject.unexportObject(this, false);
	    } catch (final Exception e) {
	    	throw new RemoteException("Could not unregister Scheduler RMI service, quiting anyway.", e);
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
    } catch (Exception e) {
      log.log(Level.WARNING, "Shutdown failed exceptionally", e);
      throw new RemoteException("Shutdown failed exceptionally", e);
    }
  }

  /**
   * Return status information
   */
  public List<String> status() {
    final List<String> al = new ArrayList<String>();

    al.add("--- ETLC Scheduler ---");
    if (this.schedulerThread != null) {
      al.add("  Status: active");
    } else {
      al.add("  Status: on hold");
    }
    al.add("  Poll interval: " + this.pollIntervall);
    al.add("  Penalty Wait: " + this.penaltyWait);

    return al;
  }

  /**
   * Triggers a set if set does not exists or set is in hold, nothing is done.
   * 
   * @param name
   *          name of the triggered set.
   */
  public void trigger(final String name) {
    trigger(name, "");
  }

  /**
   * Triggers a list of sets, if set does not exists or set is in hold, nothing
   * is done.
   * 
   * @param name
   *          name of the triggered set.
   */
  @Deprecated
  public void trigger(final List<String> list) {

    if (list != null) {
      final Iterator<String> iter = list.iterator();
      while (iter.hasNext()) {
        final Object temp = iter.next();
        final String name = (String) temp;
        log.fine("Triggering set " + name);
        trigger(name, "");
      }
    }
  }

  /*
   * (non-Javadoc)
   * @see com.distocraft.dc5000.etl.scheduler.ISchedulerRMI#trigger(java.util.Map)
   */
  public void trigger(final List<String> list, final Map<String, String> map) {
    
    final String setName = map.get("setName");
    final String setType = map.get("setType");
    final String baseTable = map.get("setBaseTable");
    
    final Properties schedulingInfo = new Properties();
    for (Entry<String, String> entry : map.entrySet()) {
      schedulingInfo.put(entry.getKey(), entry.getValue());
    }
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      schedulingInfo.store(baos, "");
    } catch (IOException e) {
      log.severe("Failed to serialize schedulingInfo map as string: " + e.getMessage());
    }

    for (String item : list) {
      log.fine("Triggering set " + item +  " with parameters " + setName + " for " + setType + "_" + baseTable);
      trigger(item, baos.toString());
    }
    
    
  }


  /**
   * Triggers a set if set does not exists or set is in hold, nothing is done.
   * 
   * 
   * @param name
   *          name of the triggered set.
   * @param command
   *          context
   */
  public void trigger(final String name, final String command) {

    if (this.schedulerThread != null) {
      if (this.schedulerThread.startThread(name, command)) {
        log.info("Trigger \"" + name + "\" triggered");
      } else {
        log.info("Trigger \"" + name + "\" NOT triggered");
      }
    } else {
      log.log(Level.INFO, "Did not trigger set \"" + name + "\" because scheduler is on hold or initializing");
    }
  }

  /**
   * Reload logging property files
   */
  public void reloadLoggingProperties() throws RemoteException {

    try {
      LogManager.getLogManager().reset();
      LogManager.getLogManager().readConfiguration();
      log.info("Logging properties reloaded");

    } catch (Exception e) {
      log.log(Level.WARNING, "Error while reloading logging properties", e);
      throw new RemoteException("Error while reloading logging properties", e);
    }

  }

  /**
   * Initializes the dagger Scheduler server - binds the server object to RMI
   * registry - initializes the omi connection - instantiates the access
   * controller
   * 
   * @param name
   *          String Name of server in registry
   * @return boolean true if initialisation succeeds otherwise false
   * @exception
   */
  public boolean init() {

    log.finer("Initializing...");
    final String rmiRef = "//" + this.serverHostName + ":" + this.serverPort + "/" + this.serverRefName;
    ENIQRMIRegistryManager rmiRgty = new ENIQRMIRegistryManager(this.serverHostName, this.serverPort);
	try {
		Registry rmi = rmiRgty.getRegistry();
    	log.fine("Scheduler server is rebinding to rmiregistry on host: " + this.serverHostName + " on port: " + this.serverPort + 
    			" with name: " + this.serverRefName);
		rmi.rebind(this.serverRefName, this);
		log.info("Scheduler Server binds successfully to already running rmiregistry");
	} catch (final Exception e) {
		//NEW HSS/SMF Stuff
    	if(!(ServiceMonitor.isSmfEnabled())){
    		//SMF is disabled
    		log.log(Level.SEVERE, "Unable to bind to the rmiregistry using refrence: " + rmiRef, e);
		    return false;
    	}else{
    		//SMF is enabled
    		try{
    			log.info("Starting RMI-Registry on port " + this.serverPort);
    			Registry rmi = rmiRgty.createNewRegistry();
    			log.info("New RMI-Registry created");
    			rmi.bind(this.serverRefName, this);
    			log.fine("Scheduler Server registered to newly started rmiregistry");
    		}catch (Exception exception) {
    			log.log(Level.SEVERE, "Unable to initialize LocateRegistry", exception);
    			return false;
    		}
    	}//else
	}//catch
    log.fine("Scheduler has been initialized.");
    return true;
  }

  /**
   * Load configuration
   */
  private void loadProperties() throws SchedulerException {
	  
	  
		// Create static properties.
	try {
			StaticProperties.reload();
		} catch (IOException e1) {

			log.log(Level.SEVERE, "IOException when loading StaticProperties",
					e1);
		}
		
    String sysPropDC5000 = System.getProperty("dc5000.config.directory");
    if (sysPropDC5000 == null) {
      log.severe("System property dc5000.config.directory not defined");
      throw new SchedulerException("System property dc5000.config.directory not defined");
    }

    if (!sysPropDC5000.endsWith(File.separator)) {
      sysPropDC5000 += File.separator;
    }
   Properties appProps = null ;
    try{	
    	appProps = new ETLCServerProperties(sysPropDC5000 + "ETLCServer.properties");
    }catch(IOException e){
    	throw new SchedulerException("failed to read ETLCServer.Properties",e);
    }
   // Properties appProps = new ETLCServerProperties();
    // Reading DB connection properties

    this.url = appProps.getProperty("ENGINE_DB_URL");
    log.config("Using DB @ " + this.url);

    this.userName = appProps.getProperty("ENGINE_DB_USERNAME");
    this.password = appProps.getProperty("ENGINE_DB_PASSWORD");
    this.dbDriverName = appProps.getProperty("ENGINE_DB_DRIVERNAME");
    RMISchedulerUserPort = Integer.parseInt(appProps.getProperty(SCHEDULER_RMI_PROCESS_PORT,SCHEDULER_RMI_PROCESS_PORT_DEFAULT));
    

    
    // get the hostname by service name and default to localhost.
    String hostNameByServiceName = null ;
    try{
  	  hostNameByServiceName = ServicenamesHelper.getServiceHost("scheduler", "localhost");
    }catch(final Exception e){
  	  hostNameByServiceName = "localhost" ;
    }
	this.serverHostName = appProps.getProperty("SCHEDULER_HOSTNAME", hostNameByServiceName);

    // Reading engine connection properties
	
    // get the engine hostname by service name and default to localhost.
	//EngineConnector.init(appProps, log);
    this.serverPort = 1200;
    final String sporttmp = appProps.getProperty("SCHEDULER_PORT", "1200");
    try {
      this.serverPort = Integer.parseInt(sporttmp);
    } catch (NumberFormatException nfe) {
      log.config("Using default SCHEDULER_PORT 1200.");
    }

    this.serverRefName = appProps.getProperty("SCHEDULER_REFNAME", null);
    if (this.serverRefName == null) {
      log.config("Using default SCHEDULER_REFNAME \"Scheduler\"");
      this.serverRefName = "Scheduler";
    }

    final String pollIntervall = appProps.getProperty("SCHEDULER_POLL_INTERVALL");
    if (pollIntervall != null) {
      this.pollIntervall = Long.parseLong(pollIntervall);
    }
    log.config("Using pollInterval " + pollIntervall);

    final String penaltyWait = appProps.getProperty("SCHEDULER_PENALTY_WAIT", "30");
    if (penaltyWait != null) {
      this.penaltyWait = Integer.parseInt(penaltyWait);
    }
    log.config("Using penaltyWait " + penaltyWait);

    try {
      this.reConnectTime = new Long(appProps.getProperty("SERVER_RECONNECT_TIME")).longValue();
    } catch (Exception e) {
      this.reConnectTime = 60000;
      log.config("Using default reconnect time " + this.reConnectTime);
    }

    log.log(Level.CONFIG, "Properties loaded");

  }
  

	@Override
	public void serviceOffline(PlatformServices service) {
		// TODO Auto-generated method stub
		synchronized (mutex) {
			log.log(Level.INFO,
					"Service offlined is "+service+" .. Putting scheduler into hold state");
			if ((PlatformServices.Engine == service)
					|| (PlatformServices.repdb == service)) {
				

				try {
					if (service == PlatformServices.Engine) {

						isEngineOnline = false;
				log.log(Level.INFO,
								"Engine offlined..scheduler will be put into hold state");


					} else {

						isRepdbOnline = false;
						log.log(Level.INFO,
						"Repdb offlined.. scheduler will be put into hold state");
	
					}
					// Set to hold as either repdb or engine is offline..
					log.log(Level.INFO,
					"Putting scheduler into hold state");
					hold();
					
					
				} catch (RemoteException e) {
					log.log(Level.INFO,
							"Hold failed exceptionally while trying to offline service",
							e);
					e.printStackTrace();
				}

			}
		}
	}

	@Override
	public void serviceAvailable(PlatformServices service) {
		synchronized (mutex) {
			log.log(Level.INFO,"Service: "+service+" has now come online..");
			
			if ((PlatformServices.Engine == service && isRepdbOnline)) {
				isEngineOnline = true;
				

				try {
					// Reload as both engine and repdb are online
					log.log(Level.INFO,
					"Engine service up...	Reloading scheduler..");
					
					reload();
				} catch (RemoteException e) {
					// TODO Auto-generated catch block
					log.log(Level.INFO,
							"Reload failed exceptionally while trying to online service",
							e);
					//e.printStackTrace();
				}

			} else if ((PlatformServices.repdb == service && isEngineOnline)) {

				isRepdbOnline = true;

				/** reload config from etlrep **/
				try {
					// Reload as both engine and repdb are online
					log.log(Level.INFO,
					"repdb service up.. reloading scheduler and loading properties..");
					loadProperties();
					reload();
				} catch (RemoteException e) {

					log.log(Level.INFO,
							"Reload failed exceptionally while trying to online service",
							e);
					e.printStackTrace();
				} catch (SchedulerException e) {
					log.log(Level.INFO,
							"Load properties failed exceptionally while trying to online service",
							e);
					//e.printStackTrace();
				}

			} // end of else if
			 else {
				// Log message saying only one of the services is online...
				if (PlatformServices.Engine == service) {
					isEngineOnline = true;
					log.log(Level.INFO,
							"Only engine is online... Waiting for repdb to come online to reload..");

				} else if (PlatformServices.repdb == service) {
					isRepdbOnline = true;
					// Reload properties...
					try {
						loadProperties();
						log.log(Level.INFO,
								"Finished loading properties..Only repdb online.. Engine still offline..");
					} catch (SchedulerException e) {
						log.log(Level.INFO,
								"Load properties failed exceptionally..", e);
					
					}

				}

			}// end of else
		}

	}

  @Override
  public void serviceRestarted(PlatformServices service) {

	  log.log(Level.INFO,
				"Engine or repdb may have been restarted..");
  	
  }

  @Override
  public String getName() {
  	
	String name = "Scheduler";
  	return name;
  }
	  
 public void startScheduler(){
try{
	 loadProperties();

     if (!init()) {
       log.severe("Initialisation failed... exiting");
       System.exit(0);
     } else {
       log.info("Scheduler Ready");
       // activate scheduler
       reload();
       
       /**Find out if SMF is being used to manage the Start/Stop/Restart process dependencies..
         	if no SMF dependency then monitor engine and repdb .. **/
       
       if(!(ServiceMonitor.isSmfEnabled())){
       
       ServiceMonitor.monitorService(PlatformServices.Engine, this);
       log.log(Level.INFO, "Started Monitoring engine service..");
            
       ServiceMonitor.monitorService(PlatformServices.repdb, this);
       log.log(Level.INFO, "Started Monitoring repdb service..");
       
       }else{
    	   log.log(Level.INFO,"SMF is enabled in the system.." );
       }
     
     }
   } catch (Exception e) {
     log.log(Level.SEVERE, "Initialization failed exceptionally", e);
     e.printStackTrace();
   }
 }
 


  public static void main(final String args[]) {

    System.setSecurityManager(new com.distocraft.dc5000.etl.engine.ETLCSecurityManager());
    try {
    String sysPropDC5000 = System.getProperty("dc5000.config.directory");
    if (sysPropDC5000 == null) {
      log.severe("System property dc5000.config.directory not defined");
      throw new SchedulerException("System property dc5000.config.directory not defined");
    }

    if (!sysPropDC5000.endsWith(File.separator)) {
      sysPropDC5000 += File.separator;
    }    
    Properties appProps = null ;
    try{	
    	appProps = new ETLCServerProperties(sysPropDC5000 + "ETLCServer.properties");
    }catch(IOException e){
    	throw new SchedulerException("failed to read ETLCServer.Properties",e);
    }
    
    int RMISchedulerUserPort = Integer.parseInt(appProps.getProperty(SCHEDULER_RMI_PROCESS_PORT,SCHEDULER_RMI_PROCESS_PORT_DEFAULT));

    
      final Scheduler dc = new Scheduler(RMISchedulerUserPort);
      dc.startScheduler();


    } catch (Exception e) {
      log.log(Level.SEVERE, "Initialization failed exceptionally", e);
      e.printStackTrace();
    }

  }
}