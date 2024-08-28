/*
 * Created on 8.2.2006
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package com.distocraft.dc5000.etl.parser;

import com.ericsson.eniq.repository.ETLCServerProperties;
import java.io.File;
import java.util.Hashtable;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.Properties;
import com.distocraft.dc5000.common.SessionHandler;
import com.distocraft.dc5000.common.StaticProperties;
//import com.distocraft.dc5000.etl.engine.common.EngineCom;
//import com.distocraft.dc5000.etl.engine.common.Share;
//import com.distocraft.dc5000.etl.engine.executionslots.ExecutionSlotProfileHandler;
import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.distocraft.dc5000.repository.cache.DBLookupCache;
import com.distocraft.dc5000.repository.cache.DataFormatCache;

/**
 * Standalone version of parser which can be executed without ETLC engine
 * 
 */
public class StandaloneMain {

  // TODO check logger name
  private static Logger log = Logger.getLogger("com.distocraft.dc5000.etl.parser.StandaloneMain");

  private String urlDwh = "";

  private String userNameDwh = "";

  private String passwordDwh = "";

  private String dbDriverNameDwh = "";

  private String urlRep = "";

  private String userNameRep = "";

  private String passwordRep = "";

  private String dbDriverNameRep = "";

  private String source = "";

  public StandaloneMain() {
    init();
  }

  @SuppressWarnings({"UnusedParameters"})
  public StandaloneMain(final boolean b) { // NOPMD : eeipca : external usage?

  }

  public StandaloneMain(final String source, final RockFactory tmpRfEtlRep, final RockFactory tmpRfDwh, final RockFactory tmpRfDwhRep) {
    setSource(source);
    init(tmpRfEtlRep, tmpRfDwh, tmpRfDwhRep);
  }

  public StandaloneMain(final String source) {
    setSource(source);
    init();
  }

  /**
   * Run interface initialized with property files
   * 
   * @param source
   *          prefix for property files
   */
  public void runInterface(final String source) {
    RockFactory rfDwh = null;
    RockFactory rfDwhRep = null;

    try {
        //Testmode in MeasurementFilwImpl, to switch this on (for PC debugging, 
    	//put the following line in the static.properties file:
    	//MeasurementFileImpl.setTestMode=true
    	final String testMode = StaticProperties.getProperty("MeasurementFileImpl.setTestMode", "false");

      MeasurementFileImpl.setTestMode(Boolean.valueOf(testMode));

      final Properties props = new com.distocraft.dc5000.common.Properties(source, new Hashtable<String, String>());

      rfDwh = new RockFactory(urlDwh, userNameDwh, passwordDwh, dbDriverNameDwh, "StandaloneMain" + source, true);

      rfDwhRep = new RockFactory(urlRep, userNameRep, passwordRep, dbDriverNameRep, "StandaloneMain" + source, true);
      //createExecutionSlots(urlRep, "etlrep", "etlrep", dbDriverNameRep);
      //final Main m = new Main(props, source, "x", "x", rfDwh, rfDwhRep, new EngineCom());
      //m.parse();

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        if (rfDwh != null) {
          rfDwh.getConnection().close();
        }

        if (rfDwhRep != null) {
          rfDwhRep.getConnection().close();
        }

      } catch (Exception ef) {
        ef.printStackTrace();

      }

    }
  }

  /**private boolean createExecutionSlots(final String urlDwh, final String userNameDwh,final  String passwordDwh, final String dbDriverNameDwh){
      //Set up the ExecutionSlotProfileHandler
	    ExecutionSlotProfileHandler executionSlotProfileHandler;
      try {
		executionSlotProfileHandler = new ExecutionSlotProfileHandler(urlDwh, userNameDwh, passwordDwh, dbDriverNameDwh);
		Share.instance().add("executionSlotProfileObject", executionSlotProfileHandler);
	} catch (Exception e) {
	       log.log(Level.SEVERE, "Error while creating ExecutionSlotProfileHandler " + e);
	        return false;
	}
	  return true;
  }*/
  
  /**public void runInterface(final String source, final Properties props, final RockFactory etl, final RockFactory dwh) throws Exception {
    final Main m = new Main(props, source, "x", "x", dwh, etl, new EngineCom());
    m.parse();
  }*/

  /**
   * Get database parameters for DWH database
   * 
   * @param rock
   *          rockfactory for ETLrep
   */
  public void getDwhParameters(final RockFactory rock) {

    try {

      log.fine("Initializing...");

      final Meta_databases selO = new Meta_databases(rock);
      selO.setConnection_name("dwh");
      selO.setType_name("USER");

      final Meta_databasesFactory mdbf = new Meta_databasesFactory(rock, selO);

      final Vector dbs = mdbf.get();

      if (dbs == null || dbs.size() != 1) {
        log.severe("dwh database not correctly defined in etlrep.Meta_databases.");
      }

      final Meta_databases dwhdb = (Meta_databases) dbs.get(0);

      dbDriverNameDwh = dwhdb.getDriver_name();
      urlDwh = dwhdb.getConnection_string();
      userNameDwh = dwhdb.getUsername();
      passwordDwh = dwhdb.getPassword();

      log.config("DWH connection: " + urlDwh);

      final Meta_databases sel1 = new Meta_databases(rock);
      sel1.setConnection_name("dwhrep");
      sel1.setType_name("USER");

      final Meta_databasesFactory mdbf1 = new Meta_databasesFactory(rock, sel1);

      final Vector dbs1 = mdbf1.get();

      if (dbs1 == null || dbs1.size() != 1) {
        log.severe("dwhrep database not correctly defined in etlrep.Meta_databases.");
      }

      final Meta_databases repdb = (Meta_databases) dbs1.get(0);

      dbDriverNameRep = repdb.getDriver_name();
      urlRep = repdb.getConnection_string();
      userNameRep = repdb.getUsername();
      passwordRep = repdb.getPassword();

      log.config("DWHREP connection: " + urlRep);

    } catch (Exception e) {
      log.log(Level.SEVERE, "Fatal initialization error", e);
    }

  }

  /**
   * Find repository parameters from ETLCServer.properties and instantiate new
   * RockFactory
   *
   * @return ETLREP Connection
   */
  @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection"})
  private RockFactory getETLrepRockFactory() {
    String url;
    String userName;
    String password;
    String dbDriverName;
    try {

      String confDir = System.getProperty("dc5000.config.directory");
      if (!confDir.endsWith(File.separator)) {
        confDir += File.separator;
      }

      final ETLCServerProperties appProps = new ETLCServerProperties(confDir + "ETLCServer.properties");

      url = appProps.getProperty("ENGINE_DB_URL");
      log.config("Using ETLREP DB @ " + url);
      userName = appProps.getProperty("ENGINE_DB_USERNAME");
      password = appProps.getProperty("ENGINE_DB_PASSWORD");
      dbDriverName = appProps.getProperty("ENGINE_DB_DRIVERNAME");

      return new RockFactory(url, userName, password, dbDriverName, "StandaloneMain", true);

    } catch (Exception e) {
      log.log(Level.SEVERE, "Unable to initialize rockengine:" + e.getMessage(), e);
      return null;
    }
  }

  /**
   * Parse can be executed without ETLC engine Action properties read from
   * property file in conf dir with source-prefix in keys Following system
   * properties must be defined -Ddc5000.config.directory=c:/dc/dc5000/conf
   * -Djava.util.logging.config.file=c:/dc/dc5000/conf/engineLogging.conf
   * 
   * @param args source =
   *          prefix for property file keys
   */
  public static void main(final String[] args) {

    if (args.length <= 0) {
      System.err.println("\"source\" must be defined as argument");
      return;
    }

    final StandaloneMain main = new StandaloneMain(args[0]);
    main.runInterface(args[0]);

  }

  /**
   * Initializes properties and dataformat cache
   * 
   * @param tmpRfEtlRep etlrep conn
   * @param tmpRfDwh dwh conn
   * @param tmpRfDwhRep dwhrep conn
   * @return boolean true if initialisation succeeds
   */
  public boolean init(final RockFactory tmpRfEtlRep, final RockFactory tmpRfDwh, final RockFactory tmpRfDwhRep) {

    log.fine("Initializing...");
    
    RockFactory rf = null;
    RockFactory rfDwh = null;
    RockFactory rfDwhRep = null;

    try {

      try {
        rf = new RockFactory(tmpRfEtlRep.getDbURL(), tmpRfEtlRep.getUserName(), tmpRfEtlRep.getPassword(), tmpRfEtlRep
            .getDriverName(), "ParserDebugger" + source, true);
      } catch (Exception e) {
        log.log(Level.SEVERE, "Error while creating rockFactory " + e);
        return false;
      }

      // Get DWH database parameters;
      getDwhParameters(rf);

      try {
        rfDwh = new RockFactory(tmpRfDwh.getDbURL(), tmpRfDwh.getUserName(), tmpRfDwh.getPassword(), tmpRfDwh
            .getDriverName(), "StandaloneMain" + getSource(), true);

        rfDwhRep = new RockFactory(tmpRfDwhRep.getDbURL(), tmpRfDwhRep.getUserName(), tmpRfDwhRep.getPassword(),
            tmpRfDwhRep.getDriverName(), "StandaloneMain" + getSource(), true);

      } catch (Exception ex) {
        log.log(Level.SEVERE, "Error while creating rockFactory " + ex);
        return false;
      }

      // Perform some static object initialization...

      try {

        // Create static properties.
        // StaticProperties.reload();

        // Initialize SessionHandler
        SessionHandler.init();

        // Relaod logging configurations
        // LogManager.getLogManager().readConfiguration();

        // Init Parser DataFormat Cache
        DataFormatCache.initialize(rf, tmpRfDwhRep.getDbURL(), tmpRfDwhRep.getUserName(), tmpRfDwhRep.getPassword());

        // Init DBLookup cache
        DBLookupCache.initialize(rf, tmpRfDwh.getDbURL(), tmpRfDwh.getUserName(), tmpRfDwh.getPassword());

        // Init Transformer Cache
        new TransformerCache().revalidate(rfDwhRep, rfDwh);

      } catch (Exception e) {
        log.log(Level.SEVERE, "Error while initializing caches " + e);
        return false;
      }

    } finally {

      try {
        if (rf != null){
          rf.getConnection().close();
        }
        if (rfDwh == null){
          rfDwh.getConnection().close();
        }
        if (rfDwhRep == null){
          rfDwhRep.getConnection().close();
        }
      } catch (Exception ef) {
        log.log(Level.SEVERE, "Error while closing connections caches " + ef);
      }
    }

    log.fine("initialized.");

    return true;

  }// init

  /**
   * Initializes properties and dataformat cache
   * 
   * @return boolean true if initialisation succeeds
   */
  public boolean init() {

    log.fine("Initializing...");

    final RockFactory rf = getETLrepRockFactory();
    if (rf == null) {
      log.log(Level.INFO, "Exiting..");
      System.exit(0);
    }

    // Get DWH database parameters;
    getDwhParameters(rf);

    RockFactory rfDwh = null;
    RockFactory rfDwhRep = null;
    try {
      rfDwh = new RockFactory(urlDwh, userNameDwh, passwordDwh, dbDriverNameDwh, "StandaloneMain" + getSource(), true);

      rfDwhRep = new RockFactory(urlRep, userNameRep, passwordRep, dbDriverNameRep, "StandaloneMain" + getSource(),
          true);

    } catch (Exception ex) {
      if (rfDwh == null || rfDwhRep == null) {
        log.log(Level.INFO, "Exiting..");
        System.exit(0);
      }
    }

    // Perform some static object initialization...

    try {

      // Create static properties.
      StaticProperties.reload();

      // Initialize SessionHandler
      SessionHandler.init();

      // Relaod logging configurations
      // LogManager.getLogManager().readConfiguration();

      // Init Parser DataFormat Cache
      DataFormatCache.initialize(rf);

      // Init DBLookup cache
      DBLookupCache.initialize(rf);

      // Init Transformer Cache
      new TransformerCache().revalidate(rfDwhRep, rfDwh);

    } catch (Exception e) {
      e.printStackTrace();
    }

    try {
      if (rf != null){
        rf.getConnection().close();
      }
      if (rfDwh == null){
        rfDwh.getConnection().close();
      }
      if (rfDwhRep == null){
        rfDwhRep.getConnection().close();
      }
    } catch (Exception ef) {
      ef.printStackTrace();
    }

    log.fine("initialized.");

    return true;

  } // init

  /**
   * Gets the source member, which is the mandatory parameter for standaloneMain
   * 
   * @return String
   */
  private String getSource() {
    return source;
  }

  /**
   * Sets the source member, which is the mandatory parameter for standaloneMain
   * 
   * @param source
   *          String
   */
  private void setSource(final String source) {
    this.source = source;
  }
}
