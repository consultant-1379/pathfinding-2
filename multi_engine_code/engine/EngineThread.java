package com.distocraft.dc5000.etl.engine.main;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineCom;
import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.common.ExceptionHandler;
import com.distocraft.dc5000.etl.engine.executionslots.ExecutionMemoryConsumption;
import com.distocraft.dc5000.etl.engine.plugin.PluginLoader;
import com.distocraft.dc5000.etl.engine.priorityqueue.PersistenceHandler;
import com.distocraft.dc5000.etl.engine.priorityqueue.PriorityQueue;
import com.distocraft.dc5000.etl.engine.structure.TrCollection;
import com.distocraft.dc5000.etl.engine.structure.TrCollectionSet;
import com.distocraft.dc5000.etl.engine.structure.TransferAction;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.engine.system.SetListener;
import com.distocraft.dc5000.etl.rock.Meta_collection_sets;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actionsFactory;
import com.distocraft.dc5000.repository.dwhrep.Alarminterface;
import com.distocraft.dc5000.repository.dwhrep.AlarminterfaceFactory;
import com.distocraft.dc5000.repository.dwhrep.Alarmreport;
import com.distocraft.dc5000.repository.dwhrep.AlarmreportFactory;
import com.distocraft.dc5000.repository.dwhrep.Alarmreportparameter;
import com.distocraft.dc5000.repository.dwhrep.AlarmreportparameterFactory;

/**
 * The class presents queuing or executing ETL Set.
 * 
 * @author Jukka Jaaheimo

 * @author savinen
 * @author lemminkainen
 */
public class EngineThread extends Thread { // NOPMD

	public static final long serialVersionUID = 1L;
	public static final String PREV_POSTFIX = "_PREV";
	public static final String RAW_POSTFIX = "_RAW";
	
  private transient String url;
  private transient String userName;
  private transient String password;
  private transient String dbDriverName;
      
  private transient String colSetName;
  private transient Long colSetId;
  private transient String colSetVersion;
  private transient String colName;

  private String schedulingInfo;
  
  private transient PersistenceHandler persHandler;
  private Long persistentID = null;
  private boolean persistent = false;
  
  private boolean active = true;
  
  private transient PluginLoader pluginLoader;
  private transient EngineCom eCom = null;

  private transient String setType;
  private transient String setName;
  private transient Long setID = null;

  /** All tablenames */
  private transient List<String> setTables = new ArrayList<String>();
  
  /** Tablenames that are explicitely inserted with addTablename()
   *  those has to be persistent because they cannot be recreated. */
  private transient final List<String> explicitTNames = new ArrayList<String>();

  private transient Long queueID = null;
  private transient Integer slotID = null;
  private String slotType = null;

  private transient Long setPriority;
  private transient Long queueTimeLimit;

  private transient boolean worker = false;
  private transient boolean shutdownSet = false;
  
  private transient Object workerObject = null;

  public transient SetListener setListener = null;

  /* When engineThread was created */
  private transient Date creationDate;

  /* When Engine Thread was last edited */
  /* This is used when timelimit checks and priority increases are done. */
  private transient Date changeDate;

  /**
   * Defines a time for this set after this set can be executed.
   * This is used for suppress set execution until set date.
   */
  private transient Date earliestExecution = null; 
  
  /**
   * Defines a time after this set will be removed from queue
   * regardless the priority.
   */
  private Date latestExecution = null;
  
  private final transient Logger log; //NOPMD
  private final transient String loggerName;
  
  private transient TrCollectionSet trCollectionSet = null;
  
  // PriorityQueue instance for triggering sets directly
  private transient PriorityQueue priorityQueue = null;

  // String presentation of this Set
  private transient String stringified;
  
  private long execStartTime = 0L;
  private long execEndTime = 0L;

  /**
   * Constructor for starting the transfer
   * 
   */
  public EngineThread(final String url, final String userName, final String password, final String dbDriverName,
  										final String collectionSetName, final String collectionName, final PluginLoader pluginLoader,
  										final String schedulingInfo, final SetListener list, final Logger log, final EngineCom eCom)
  										throws EngineMetaDataException {

  	super("Engine");

    this.eCom = eCom;
    this.url = url;
    this.userName = userName;
    this.password = password;
    this.dbDriverName = dbDriverName;
    this.colSetName = collectionSetName;
    this.colName = collectionName;
    this.pluginLoader = pluginLoader;
    this.schedulingInfo = schedulingInfo;
    this.setListener = list;
    this.log = log;
    this.loggerName = log.getName();

    RockFactory etlrep = null;
    
    try {
    	
      etlrep = getETLREP();
      init(etlrep, this.colSetName, this.colName);

    } catch (RockException re) {
    	log.log(Level.INFO,"Exception ##### stack :re:  ",re);
    	throw new EngineMetaDataException("EngineThread (" + this.colSetName + "/" + this.colName
          + ") not created. Database connection failed", re, this.getClass().getName());
    } catch (SQLException sqle) {
    	log.log(Level.INFO,"Exception ##### stack :sqle:  ",sqle);
    	throw new EngineMetaDataException("EngineThread (" + this.colSetName + "/" + this.colName
    			+ ") not created. Database connection failed", sqle, this.getClass().getName());
    } catch (Exception e) {
    	log.log(Level.INFO,"Exception ##### stack :e:  ",e);
      throw new EngineMetaDataException("EngineThread (" + this.colSetName + "/" + this.colName
          + ") not created", e, this.getClass().getName());
    } finally {

      try {
        log.finest("EngineThread.constructor: closing rockengine connection");
        etlrep.getConnection().close();
      } catch (Exception e) {
        log.log(Level.FINER, "Failed to close rock factory connection", e);
      }
    }

  }

  /**
   * Constructor for EngineThread where setName and collectionName are different.
   * Currently needed for CountingTrigger class in eventsupport module.
   * @param rockFact
   * @param collectionSetName
   * @param collectionName
   * @param setName
   * @param pluginLoader
   * @param log
   * @param eCom
   * @throws EngineMetaDataException
   */
  public EngineThread(final RockFactory rockFact, final String collectionSetName, final String collectionName,
      final String setName, final PluginLoader pluginLoader, final Logger log, final EngineCom eCom) throws EngineMetaDataException {
    this(rockFact, collectionSetName, collectionName, pluginLoader, log, eCom);
    this.setName = setName;
  }
  
  /**
   * Constructor for starting the transfer
   * 
   */
  public EngineThread(final RockFactory rockFact, final String collectionSetName, final String collectionName,
  		final PluginLoader pluginLoader, final Logger log, final EngineCom eCom) throws EngineMetaDataException {

  	super("Engine");

    this.url = rockFact.getDbURL();
    this.userName = rockFact.getUserName();
    this.password = rockFact.getPassword();
    this.dbDriverName = rockFact.getDriverName();
    this.eCom = eCom;
    this.colSetName = collectionSetName;
    this.colName = collectionName;
    this.pluginLoader = pluginLoader;
    this.log = log;
    this.loggerName = log.getName();

    // Do not cache RockFactory here. The same connection
    // could have been given to number of EngineThreads.
    
    try {
    	
      init(rockFact, this.colSetName, this.colName);

    } catch (Exception e) {
      throw new EngineMetaDataException("EngineThread (" + this.colSetName + "/" + this.colName
          + ") not created", e, this.getClass().getName());
    }

  }

  /**
   * 
   * constructor for creating a shutdown set
   * 
   * 
   * @param message
   */
  public EngineThread(final String name, final Long priority, final Logger log, final EngineCom eCom) {
    this(name, name, priority, null, log);

    this.eCom = eCom;    


    this.shutdownSet = true;
    this.worker = false;
    
    updateToString();

  }

  /**
   * 
   * constructor for creating a worker set
   * 
   * 
   * @param message
   */
  public EngineThread(final String name, final String type, final Long priority, final Object wobj, final Logger log) {
    super("Engine");

    this.log = log;
    this.loggerName = log.getName();

    this.worker = true;

    this.workerObject = wobj;
    
    this.setType = type;
    this.setName = name;
    this.setID = Long.valueOf(-1L);
    this.setPriority = priority;
    this.queueTimeLimit = Long.valueOf(10000L);
    this.creationDate = new Date();
    this.changeDate = new Date();
    
    updateToString();
  }
  
  /**
   * Fetch variables from set and init some of class variables.
   * @throws Exception
   */
  private void init(final RockFactory etlrep, final String collectionSetName, final String collectionName) throws RockException, SQLException {

    log.finest("EngineThread initializing...");

    final Meta_collection_sets whereCollSet = new Meta_collection_sets(etlrep);
    whereCollSet.setEnabled_flag("Y");
    whereCollSet.setCollection_set_name(collectionSetName);
    final Meta_collection_sets collSet = new Meta_collection_sets(etlrep, whereCollSet);
    
    final Long collectionSetId = collSet.getCollection_set_id();
    colSetVersion = collSet.getVersion_number();
    this.colSetId = collSet.getCollection_set_id();

    log.finest("EngineThread found Techpack " + collectionSetName + ": \"" + colSetVersion + "\","
        + collectionSetId.toString() + ")");

    Long collectionId = null;
    Meta_collections coll = null;
    
    if (collectionName != null) {
      final Meta_collections whereColl = new Meta_collections(etlrep);
      whereColl.setVersion_number(colSetVersion);
      whereColl.setCollection_set_id(collectionSetId);
      whereColl.setCollection_name(collectionName);
      coll = new Meta_collections(etlrep, whereColl);
      collectionId = coll.getCollection_id();
    }

    this.setType = coll.getSettype();
    this.setName = coll.getCollection_name();
    if (setType != null && setType.equalsIgnoreCase("Alarm") && this.setName.startsWith("Adapter_")) {
    	if (this.setName.endsWith("_RD")) {
        this.setTables = getReducedDelayAlarmActionTables(collectionSetId, collectionId, etlrep);
      } else {
      this.setTables = getAlarmActionTables(collectionSetId, collectionId, etlrep);
      }
    } else {
      this.setTables = getActionTables(collectionSetId, collectionId, etlrep);
    }
    setTables.addAll(explicitTNames);
    
    this.setID = coll.getCollection_id();
    this.setPriority = coll.getPriority();
    this.queueTimeLimit = coll.getQueue_time_limit();

    this.creationDate = new Date();
    this.changeDate = new Date();
    
    updateToString();
    
    log.finest("EngineThread initialized (" + collectionSetId.toString() + "," + collectionId.toString() + ")");

  }
  
  /**
   * Execution of this ETL Set.
   */
  @Override
  public void run() {

  	execStartTime = System.currentTimeMillis();
  	
    if (this.worker) {

      log.info("Starting a worker: " + setName);
      updateMemoryConsumption(true);
      ((Runnable) workerObject).run(); // NOPMD
      updateMemoryConsumption(false);
      
    } else if (!this.shutdownSet) {

    	RockFactory etlrep = null;
    	
      try {

      	etlrep = getETLREP();

        trCollectionSet = new TrCollectionSet(etlrep, this.colSetName, this.colName,
            this.pluginLoader, eCom, priorityQueue, slotID, schedulingInfo, slotType);

        // executes collection if enabled
        if (trCollectionSet.isEnabled()) {

          // If there is a setListener defined for this EngineThread, use it to 
          // observe set execution.
          if (setListener == null) {
          	trCollectionSet.executeSet(SetListener.NULL);
          } else {
          	trCollectionSet.executeSet(setListener);
          }

          if (setListener != null) {
            setListener.succeeded();
          }
        } else {
          log.fine("Execution cancelled: Package " + this.colSetName + " is disabled");
        }
      } catch (InterruptedException iexc) {                
        Thread.currentThread().interrupt(); // re-call interrupt.
      } catch (Exception e) {        
        ExceptionHandler.handleException(e, "Execution failed exceptionally");
        if (setListener != null) {
          setListener.failed();
        }
      } finally {

      	if(persHandler != null) {
      		persHandler.executedSet(this);
      	}
      	
        try {
          if (trCollectionSet != null) {
            final int count = trCollectionSet.cleanSet();

            log.finest("Closing rockengine, " + count + " connections closed from connectionpool");
          }
        } catch (Exception e) {
          ExceptionHandler.handleException(e, "Cleanup failed");

        } finally {
          try {
            if (etlrep != null && etlrep.getConnection() != null) {
              etlrep.getConnection().close();
            }
          } catch (Exception e) {
            ExceptionHandler.handleException(e);
          }
        }
      }

      if (priorityQueue != null) {
      	Logger.getLogger("etlengine.priorityqueue").finer("Interrupt setFinished");
      	priorityQueue.interrupt();
      }
      
    }
    
    execEndTime = System.currentTimeMillis();
  }

  public List<String> getSetTables() {
    return this.setTables;
  }
  
  public void addSetTable(final String tableName) {
  	explicitTNames.add(tableName);
  	this.setTables.add(tableName);  
  }

  public void removeSetTable(final String tableName) {
  	explicitTNames.remove(tableName);
  	this.setTables.remove(tableName);  
  }

  /**
   * Returns a properties object containing all non
   * transient variables of this instance.
   */
  public Properties getSerial() {
  	final Properties props = new Properties();
  	props.setProperty(PersistenceHandler.COL_SET_NAME, this.colSetName);
    props.setProperty(PersistenceHandler.SET_NAME, this.setName);
  	props.setProperty(PersistenceHandler.COL_NAME, this.colName);
  	props.setProperty(PersistenceHandler.SETTYPE, this.setType);
  	// Do not persist versionNumber or you are in trouble with TP upgrade!
  	props.setProperty(PersistenceHandler.PERSISTENT_ID, persistentID.toString());
  	props.setProperty(PersistenceHandler.ACTIVE, String.valueOf(active));
  	props.setProperty(PersistenceHandler.LOGGER_NAME, this.loggerName);
  	props.setProperty(PersistenceHandler.PRIORITY, String.valueOf(setPriority));
  	props.setProperty(PersistenceHandler.TIMELIMIT, String.valueOf(queueTimeLimit));
  	
  	if(schedulingInfo != null) {
  		props.setProperty(PersistenceHandler.SCHEDULING_INFO, this.schedulingInfo);
  	}

  	if(earliestExecution != null) {
  		props.setProperty(PersistenceHandler.EARLIEST_EXEC, String.valueOf(earliestExecution.getTime()));
  	}
  	
  	if(latestExecution != null) {
  		props.setProperty(PersistenceHandler.LATEST_EXEC, String.valueOf(latestExecution.getTime()));
  	}
  	
  	final StringBuffer buffer = new StringBuffer();
  	for(int i = 0 ; i < explicitTNames.size() ; i++) {
  		buffer.append(explicitTNames.get(i));
  		if(i+1 < explicitTNames.size()) {
  			buffer.append(',');
  		}
  	}
  	
  	if(buffer.length() > 0) {
  		props.setProperty(PersistenceHandler.XP_TABLENAMES, buffer.toString());
  	}

  	return props;
  }

  /**
   * @param collectionSetId
   * @param collectionId
   * @return
   */
  private List<String> getReducedDelayAlarmActionTables(final Long collectionSetId, final Long collectionId, final RockFactory etlrep) {
    final List<String> list = getAlarmActionTables(collectionSetId, collectionId, etlrep);
    list.add("DC_Z_ALARM_INFO");
    return list;
  }
  
  private List<String> getAlarmActionTables(final Long collectionSetId, final Long collectionId, final RockFactory etlrep) {

    final List<String> list = new ArrayList<String>();
   
    RockFactory dwhrep = null;
    
    try {
      log.finest("EngineThread getting alarm interfaces (" + collectionSetId.toString() + "," + collectionId.toString() + ")");
    	
    	dwhrep = initDwhRep(etlrep);

      final Alarminterface whereInterface = new Alarminterface(dwhrep);
      whereInterface.setCollection_set_id(collectionSetId);
      whereInterface.setCollection_id(collectionId);
      final AlarminterfaceFactory dbAlarmInterfaces = new AlarminterfaceFactory(dwhrep, whereInterface);
      final Vector<Alarminterface> dbVec = dbAlarmInterfaces.get();

      if (dbVec == null) {
        log.finest("EngineThread did not find any alarm interfaces for set");
        return list;
      }
      log.finest("EngineThread found " + dbVec.size() + " interfaces for set");

      for(Alarminterface dbInterface : dbVec) {

      	final String interface_id = dbInterface.getInterfaceid();

        log.finest("EngineThread getting alarm interface reports for " + interface_id);
        
        final Alarmreport reportCond = new Alarmreport(dwhrep);
        reportCond.setInterfaceid(interface_id);
        final AlarmreportFactory reportFact = new AlarmreportFactory(dwhrep, reportCond);
        final Vector<Alarmreport> dbReports = reportFact.get();

        if (dbReports == null) {
          log.finest("EngineThread did not find any alarm reports for interface " + interface_id);
        } else {
          log.finest("EngineThread found " + dbReports.size() + " reports for interface");
          
          for(Alarmreport dbReport : dbReports) {
            final String report_id = dbReport.getReportid();
            log.finest("EngineThread getting basetable names for report " + report_id);
            
            final Alarmreportparameter parameterCond = new Alarmreportparameter(dwhrep);
            parameterCond.setReportid(report_id);
            parameterCond.setName("eniqBasetableName");
            final AlarmreportparameterFactory paramFact = new AlarmreportparameterFactory(dwhrep,
                parameterCond);
            final Vector<Alarmreportparameter> dbParams = paramFact.get();
            
            if (dbParams == null) {
              log.finest("EngineThread did not find any alarm base tables for report " + report_id);
            } else {
              log.finest("EngineThread found " + dbParams.size() + " basetables for report");
              
              for(Alarmreportparameter dbParam : dbParams) {
                String value = dbParam.getValue();
                if (value != null && value.indexOf(RAW_POSTFIX) > 0) {
                  value = value.substring(0,value.lastIndexOf(RAW_POSTFIX));
                }
                list.add(value != null ? value.toUpperCase() : value);
                log.finest("EngineThread found " + value + " basetable for report");
              }
            }
          }
        }
      }

    } catch (RockException rock) {
    	log.log(Level.INFO, "Error getting alarmTableNames", rock);
    } catch (SQLException sql) {
    	log.log(Level.INFO, "Error getting alarmTableNames", sql);
    } finally {
      try {
        if (dwhrep != null && dwhrep.getConnection() != null) {
          dwhrep.getConnection().close();
        }
      } catch (SQLException sql) {
      	log.log(Level.WARNING, "Error cleanup connection", sql);
      }
    }
        
    return list;
  }

  private List<String> getActionTables(final Long collectionSetId, final Long collectionId, final RockFactory etlrep) {

    List<String> list = new ArrayList<String>();

    try {

      log.finest("EngineThread getting actions (" + collectionSetId.toString() + "," + collectionId.toString() + ")");

      final Meta_transfer_actions whereActions = new Meta_transfer_actions(etlrep);

      whereActions.setCollection_set_id(collectionSetId);
      whereActions.setCollection_id(collectionId);
      final Meta_transfer_actionsFactory dbTrActions = new Meta_transfer_actionsFactory(etlrep, whereActions,
          "ORDER BY ORDER_BY_NO");
      final List<Meta_transfer_actions> dbVec = dbTrActions.get();

      log.finest("EngineThread found " + dbVec.size() + " actions for set");

      list = getTablesFromMetaTransActList(dbVec);

    } catch(RockException rock) {
    	log.log(Level.WARNING, "Getting tableNames failed", rock);
    } catch(SQLException sql) {
    	log.log(Level.WARNING, "Getting tableNames failed", sql);
    }

    return list;
  }

  private List<String> getTablesFromMetaTransActList(final List<Meta_transfer_actions> dbVec) {

    final List<String> list = new ArrayList<String>();
    for (Meta_transfer_actions dbTrAction : dbVec) {

      final String act_cont = dbTrAction.getWhere_clause();

      if (act_cont != null && act_cont.length() > 0) {

        Properties properties;

        try {
          properties = TransferActionBase.stringToProperties(act_cont);
        } catch (EngineMetaDataException me) {
          log.log(Level.INFO, "Unable to read action contents for tablenames", me);
          continue;
        }
        log.finest("properties.getProperty(\"tablename\") is " + properties.getProperty("tablename"));
        String tables = properties.getProperty("tablename");
        if (tables == null) {
          // Some tablenames are with key tableName
          log.finest("properties.getProperty(\"tableName\") is " + properties.getProperty("tableName"));
          tables = properties.getProperty("tableName");
        }
        if (tables != null) {
          final String[] tablesArr = tables.split(",");
          for (String value : tablesArr) {
            if (value.length() > 0 && value.indexOf(PREV_POSTFIX) > 0) {
              list.add(value.substring(0, value.lastIndexOf(PREV_POSTFIX)));
            } else {
              list.add(value != null ? value.toUpperCase() : value);
            }
          }
        }

        final String typeName = properties.getProperty("typeName");
        if (typeName != null) {
          final String[] typesArr = typeName.split(",");
          for (String value : typesArr) {
            if (value != null && value.indexOf(PREV_POSTFIX) > 0) {
              list.add(value.substring(0, value.lastIndexOf(PREV_POSTFIX)));
            } else {
              list.add(value.toUpperCase());
            }
          }
        }
      }
    }
    return list;
  }

  /**
   * Returns collection to ETLREP database
   */
  private RockFactory getETLREP() throws RockException, SQLException {

  		log.finest("Initializing DB connection to ETLREP");
		final RockFactory cachedETLRep = new RockFactory(url, userName, password, dbDriverName, "ETLEngThr", true);
		log.finest("DB connection initialized");
  
  	return cachedETLRep;
  	
  }
  
  private RockFactory initDwhRep(final RockFactory etlrep) throws RockException, SQLException {
    
    RockFactory rockFactTmp = null;
    
    try {

      final Meta_databases md_cond = new Meta_databases(etlrep);

      md_cond.setType_name("USER");
      md_cond.setConnection_name("dwhrep");
      final Meta_databasesFactory md_fact = new Meta_databasesFactory(etlrep, md_cond);

      final Vector<Meta_databases> dbs = md_fact.get();
      if (dbs == null) {
        throw new RockException("Database dwhrep is not defined in Meta_databases?!");
      }
      
      for(Meta_databases db : dbs) {
      	if (db.getConnection_name().equalsIgnoreCase("dwhrep")) {

          log.finest("Initializing DB connection");
          rockFactTmp = new RockFactory(db.getConnection_string(), db.getUsername(), db.getPassword(), db
              .getDriver_name(), "DWHMgr", true);
          log.finest("DB connection initialized");
        }
      } // for each Meta_databases
    } catch (RockException rock) {

      log.log(Level.SEVERE, "Failed to connect to DB ", rock);

      if (rockFactTmp != null && rockFactTmp.getConnection() != null) {
        try {
          rockFactTmp.getConnection().close();
        } catch (Exception ex) {
          log.log(Level.WARNING, "Cleanup failed", ex);
        }
      }

      throw rock;
    } catch (SQLException sql) {

      log.log(Level.SEVERE, "Failed to connect to DB ", sql);

      if (rockFactTmp != null && rockFactTmp.getConnection() != null) {
        try {
          rockFactTmp.getConnection().close();
        } catch (Exception ex) {
          log.log(Level.WARNING, "Cleanup failed", ex);
        }
      }

      throw sql;
    }
    
    return rockFactTmp;
  }
  
  public String getCurrentAction() {
  	String actionName = "";
  	
    if(trCollectionSet != null) {
      final TrCollection set = trCollectionSet.getCurrentCollection();
      if(set != null) {
        final TransferAction action = set.getCurrentAction();
        if(action != null) {
          actionName = action.getActionName();
        }
      }
    }
    
    return actionName;
  }

  public int getMemoryConsumptionMB() {
  	int memory = 0;
  	
  	try {
  		final Class<?> memrestricted = Class.forName("MemoryRestrictedParser");
  		if (memrestricted.isInstance(workerObject)) {
    		final Method method = memrestricted.getMethod("memoryConsumptionMB");
    		memory = (Integer)method.invoke(workerObject);  			
  		}
  	} catch(Exception e) {
  		// Not a MemoryRestrictedParser
	  }
	  
	  return memory;
			  
  }

  private void updateMemoryConsumption(final boolean add) {
  	if (worker && workerObject != null) {
  		try {
  			final Class<?> memrestricted = Class.forName("MemoryRestrictedParser");
    		if (memrestricted.isInstance(workerObject)) {
		  final ExecutionMemoryConsumption emc = ExecutionMemoryConsumption.instance();
	      if (add){
	    	  emc.add(workerObject);
    			} else {
	    	  emc.remove(workerObject);
	      }
	  }
  			
  		} catch(Exception e) {
  			// Not a MemoryRestrictedParser
  		}
  	}
  }
  
  public void setPriorityQueue(final PriorityQueue queue) {
	  this.priorityQueue = queue;
  }
  
  public void setPersistenceHandler(final PersistenceHandler persHandler) {
  	this.persHandler = persHandler;
  }

	@Override
	public String toString() {
		return this.stringified;
	}
	
	private void updateToString() {
  	final StringBuffer buffer = new StringBuffer("Set ");
  	buffer.append(colSetName);
  	buffer.append('/');
  	buffer.append(setName);
  	
  	if(schedulingInfo != null) {
  		try {
  			final Properties props = TransferActionBase.stringToProperties(schedulingInfo);
  			final String tLevel = props.getProperty(EngineConstants.TIME_LEVEL); 
  			if(tLevel != null) {
  				buffer.append(" level ");
  				buffer.append(tLevel);
  			}
  			final String sDate = props.getProperty(EngineConstants.AGG_DATE);
  			if (sDate != null) {
  				final SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.yyyy HH:mm", Locale.getDefault());
  				buffer.append(" @ ");
  				buffer.append(sdf.format(new Date(Long.valueOf(sDate))));
  			}
  			final String lockTable = props.getProperty(EngineConstants.LOCK_TABLE);
  			if (lockTable != null) {
  				buffer.append(" locktable ").append(lockTable);
  			}
  			
  			final String restoreFromToDates = props.getProperty(EngineConstants.RESTORE_FROM_TO_DATES);
  			if(restoreFromToDates != null) {
  				buffer.append(" restore dates range ");
  				buffer.append(restoreFromToDates);
  			}
  		} catch(final EngineMetaDataException eme) {
  			log.log(Level.WARNING, "Mallformed schedulingInfo found: " + schedulingInfo, eme);
  		} 		
  	}
  	
  	this.stringified = buffer.toString();
  	
  }
	
	// Setters and Getters
	
  public boolean isActive() {
    return this.active;
  }

  public void setActive(final boolean active) {
  	this.active = active;
  }
  
  public boolean isWorker() {
    return this.worker;
  }

  public void setWorker(final boolean worker) {
    this.worker = worker;
  }
  
  public Long getQueueTimeLimit() {
    return this.queueTimeLimit;
  }
  
  public void setQueueTimeLimit(final Long queueTimeLimit) {
    this.queueTimeLimit = queueTimeLimit;
  }
	
  public void setEarliestExection(final Date earliestExec) {
	  this.earliestExecution = earliestExec;
  }
  
  public Date getEarliestExecution() {
	  return this.earliestExecution;
  }
  
  public void setLatestExecution(final Date latestExec) {
  	this.latestExecution = latestExec;
  }
  
  public Date getLatestExecution() {
  	return this.latestExecution;
  }
  
  public void setPersistent(final boolean persistent) {
	  this.persistent = persistent;
  }
    
  public boolean isPersistent() {
	  return this.persistent;
  }
  
  public void setPersistentID(final Long persistentID) {
  	this.persistentID = persistentID;
  }
  
  public Long getPersistentID() {
  	return this.persistentID;
  }
  
  public void setSetPriority(final Long priority) {
    this.setPriority = priority;
  }
  
  public Long getSetPriority() {
    return this.setPriority;
  }
  
  public void setSchedulingInfo(final String schedulingInfo) {
    this.schedulingInfo = schedulingInfo;

    if(schedulingInfo != null) {
      try {
        final Properties props = TransferActionBase.stringToProperties(schedulingInfo);
  			
        final String lockTable = props.getProperty(EngineConstants.LOCK_TABLE);
        if (lockTable != null) {
          this.setTables.add(lockTable);
        }
      } catch(final EngineMetaDataException eme) {
        log.log(Level.FINE, "Mallformed schedulingInfo found: " + schedulingInfo, eme);
      } 		
    } 

    updateToString();
  }
  
  public String getSchedulingInfo() {
    return schedulingInfo;
  }

  public void setQueueID(final Long queueId) {
    this.queueID = queueId;
  }
	
  public Long getQueueID() {
    return this.queueID;
  } 

  public void setSetType(final String setType) {
  	if(setType != null) {
  		this.setType = setType;
  	}
  }
  
  public String getSetType() {
    return this.setType;
  }
  
  public void setChangeDate(final Date date) {
    this.changeDate = date;
  }
  
  public Date getChangeDate() {
    return this.changeDate;
  }

  public void setSlotId(final int slotId) {
    this.slotID = Integer.valueOf(slotId);
  }
  
  public void setSlotType(final String slotType) {
    this.slotType = slotType;
  }
  
  // Getters for readonly members

  public String getSetName() {
    return this.setName;
  }

  public Date getCreationDate() {
    return this.creationDate;
  }

  public Long getSetID() {
    return this.setID;
  }
  
  public String getTechpackName() {
    return this.colSetName;
  }
  
  public Long getTechpackID() {
  	return this.colSetId;
  }
  
  public String getVersion() {
  	return this.colSetVersion;
  }

  public Object getWorkerObject() {
    return workerObject;
  }
  
  public boolean isShutdownSet() {
    return this.shutdownSet;
  }

	public long getExecStartTime() {
		return execStartTime;
	}

	public long getExecEndTime() {
		return execEndTime;
	}
  
}
