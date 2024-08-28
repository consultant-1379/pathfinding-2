package com.distocraft.dc5000.etl.engine.main;

import java.io.Console;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distocraft.dc5000.common.RmiUrlFactory;
import com.distocraft.dc5000.common.ServicenamesHelper;
import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.ActivateSetInPriorityQueueCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.ChangeAggregationStatusCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.ChangeProfileAndWaitCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.ChangeProfileCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.ChangeSetPriorityInPriorityQueueCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.Command;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.DisableSetCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.EnableSetCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.GetLatestTableNamesForRawEventsCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.GetProfileCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.GetTableNamesForRawEventsCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.GiveEngineCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.HoldPriorityQueueCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.HoldSetInPriorityQueueCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.LockEventsUIusersCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.RemoveTechPacksInPriorityQueueCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.InvalidArgumentsException;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.KillRunningSetsCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.LockExecutionProfileCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.LoggingStatusCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.ManualCountReAggCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.PrintServiceConnInfoCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.PrintSlotInfoCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.RefreshDBLookupsCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.RefreshTransformationsCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.ReloadAggregationCacheCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.ReloadAlarmCacheCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.ReloadBackupCacheCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.ReloadConfigCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.ReloadLoggingCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.ReloadProfilesCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.RemoveSetFromPriorityQueueCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.RestartPriorityQueueCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.RestoreCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.ShowActiveInterfaces;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.ShowDisabledSetsCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.ShowSetsInExecutionSlotsCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.ShowSetsInQueueCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.ShutdownForcefulCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.ShutdownSlowCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.StartAndWaitSetCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.StartSetCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.StartSetInEngineCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.StartSetsCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.StatusCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.StopOrShutdownFastCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.RestoreOfData;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.UnlockExecutionProfileCommand;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.UpdateThresholdLimit;
import com.distocraft.dc5000.etl.engine.main.engineadmincommands.UpdateTransformationCommand;
import com.distocraft.dc5000.etl.engine.main.exceptions.InvalidSetParametersRemoteException;
import com.distocraft.dc5000.etl.engine.main.exceptions.NoSuchCommandException;
import com.distocraft.dc5000.etl.engine.system.SetListener;
import com.ericsson.eniq.common.DatabaseConnections;
import com.ericsson.eniq.repository.ETLCServerProperties;

import ssc.rockfactory.RockFactory;

/**
 * Program to control TransferEngine.
 */
public class EngineAdmin {

	private static final Logger LOG = Logger.getLogger("etlengine.engine.EngineAdmin");

	private static final Map<String, Class<? extends Command>> CMD_TO_CLASS = new HashMap<String, Class<? extends Command>>();

	private transient int serverPort;

	private transient String serverHostName;

	private transient String serverRefName = "TransferEngine";
	
	private final SimpleDateFormat timeStampFormat = new SimpleDateFormat ("yyyy.MM.dd_HH:mm:ss");

	public EngineAdmin() {
		getProperties();
	}

	public EngineAdmin(final String serverHostName, final int serverPort) {
		this.serverHostName = serverHostName;
		this.serverPort = serverPort;
	}

	private static void showUsage() {
		System.out.println("Usage: engine -e command");
		System.out.println("  commands:");
		System.out.println("    start");
		System.out.println("    stop");
		System.out.println("    status");
		System.out.println("    shutdown_fast (=stop)");
		System.out.println("    shutdown_slow");
		System.out.println("    shutdown_forceful");
		System.out.println("    reloadConfig");
		System.out.println("    reloadAggregationCache");
		System.out.println("    reloadProfiles");
		System.out.println("    reloadAlarmCache");
		System.out.println("    reloadBackupCache");
		System.out.println("    triggerRestoreOfData");
		System.out.println("    loggingStatus");
		System.out.println("    startSet 'Techpack name' 'Set name' 'Schedule'");
		System.out.println("    changeProfile 'Profile name'");
		System.out.println("    holdPriorityQueue");
		System.out.println("    restartPriorityQueue");
		System.out.println("    showSetsInQueue");
    System.out.println("    showSetsInExecutionSlots <Techpack name (optional argument)>");
		System.out.println("    removeSetFromPriorityQueue 'ID' ");
		System.out.println("    changeSetPriorityInPriorityQueue 'ID' 'New Priority' ");
		System.out.println("    activateSetInPriorityQueue 'ID' ");
		System.out.println("    holdSetInPriorityQueue 'ID' ");
		System.out.println("    getCurrentProfile");
		System.out.println();
		System.out.println("	The following commands are not supported and shall not be used unless directed by Ericsson.");
		System.out
        .println("    engine -e disableSet [[<Techpack name> <set name>] | [<Interface name> <set name>] | <action order number>] -d (disable logging)");
    System.out.println("    engine -e enableSet [<Techpack name> <set name>] | [<Interface name> <set name>] -d (disable logging)");
		System.out.println("    engine -e showDisabledSets");
		System.out.println("    engine -e showActiveInterfaces");
    System.out.println("    engine -e removeTechPacksInPriorityQueue <Techpack name>");
    System.out.println("    engine -e killRunningSets <Techpack name>");

		System.exit(1);
	}

	public static void main(final String args[]) {
		try {
			System.setSecurityManager(new com.distocraft.dc5000.etl.engine.ETLCSecurityManager());

			if (args.length < 1) {
				showUsage();
			} else {
				final String commandName = args[0];
				final Command command = createCommand(commandName, args);
				command.validateArguments();
				command.performCommand();
			}

		} catch (final java.rmi.UnmarshalException ume) {
			// Exception, cos connection breaks, when engine is shutdown
			System.exit(3);
		} catch (final java.rmi.ConnectException rme) {
			System.err.println("Connection to engine refused.");
			System.exit(2);
		} catch (final InvalidSetParametersRemoteException invalidParam) {
			System.err.println(invalidParam.getMessage());
			showUsage();
			System.exit(1);
		} catch (final NoSuchCommandException noSuchCommandEx) {
			System.err.println(noSuchCommandEx.getMessage());
			showUsage();
			System.exit(1);
		} catch (final InvalidArgumentsException invalidArgsEx) {
			System.err.println(invalidArgsEx.getMessage());
			System.exit(1);
		} catch (final RemoteException remoteEx) {
			System.err.println(remoteEx.getMessage());
			System.exit(1);
		} catch ( final NotBoundException notBoundEx) {
			// mainly for the case of initial install where engine is stopped but was never started
			System.err.println("Connection to engine failed. (Not bound)");  // NOPMD
			System.exit(1); // NOPMD */
		} catch (final Exception e) {
      LOG.log(java.util.logging.Level.FINE, "General Exception", e);
      final String msg = e.getMessage();
      if (msg != null && msg.equals("Engine initialization has not been completed yet")) {
        System.err.println(msg);
        System.exit(4);
      }
      e.printStackTrace(System.err);
      System.exit(1);
		}
		System.exit(0);
	}

	/**
	 * 
	 * @param commandName
	 *          commandName that user has entered to CLI engine -e
	 * @param args
	 *          all arguments that user has entered to cLI engine -e must be at
	 *          least of length 1
	 * @return newly created Command to perform task
	 * 
	 * @throws IllegalArgumentException
	 *           none of these should every really be thrown from here - they
	 *           arise from using reflection to find and instantiate the class See
	 *           the Command constructor - this performs no logic, so there should
	 *           never be any exceptions coming from it
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws SecurityException
	 * @throws NoSuchMethodException
	 * 
	 * @throws NoSuchCommandException
	 *           if the user has entered an invalid command type
	 */
	static Command createCommand(final String commandName, final String[] args) throws IllegalArgumentException,
			InstantiationException, IllegalAccessException, InvocationTargetException, SecurityException,
			NoSuchMethodException, NoSuchCommandException {

		final Class<? extends Command> classToUse = CMD_TO_CLASS.get(commandName);
		if (classToUse == null) {
			throw new NoSuchCommandException("Invalid command entered: " + commandName);
		}
		final Class<? extends String[]> class1 = args.getClass();
		final Constructor<? extends Command> constructor = classToUse.getConstructor(class1);
		final Object constArguments = args;
		return constructor.newInstance(constArguments);
	}


  public void printServiceInfo() throws MalformedURLException, NotBoundException, RemoteException {
    LOG.info("Print slot info");
    getProperties();
    final ITransferEngineRMI termi = connect();
    System.out.println(timeStampFormat.format(new Date()) + "   Getting the slot info ...");
    final Map<String, String> details = termi.serviceNodeConnectionInfo();
    final Map<String, String> header = new LinkedHashMap<String, String>();
    header.put("service_node", "Name");
    header.put("status", "Status");

    final List<Map<String, String>> results = new ArrayList<Map<String, String>>(details.size());
    for(Map.Entry<String, String> serivceInfo : details.entrySet()){
      final Map<String, String> info  = new HashMap<String, String>();
      info.put("service_node", serivceInfo.getKey());
      info.put("status", serivceInfo.getValue());
      results.add(info);
    }

    System.out.println(ColumnFormat.format(header, results));

    System.out.println("Finished successfully");
  }

	/**
	 * Prints the information about executing slots. Including Calculated memory
	 * consumption.
	 */
	public void printSlotInfo() throws MalformedURLException, NotBoundException, RemoteException {
		LOG.info("Print slot info");
		
		getProperties();
		final ITransferEngineRMI termi = connect();
		
		System.out.println("Getting the slot info ...");
		
		final List<Map<String,String>> result = termi.slotInfo();
		
		final Map<String, String> header = new LinkedHashMap<String, String>();
		
		header.put("slotId", "ID");
		header.put("slotName", "Name");
		header.put("hold", "Hold");
		header.put("setTypes", "Set Types");		
		header.put("tpName", "Techpack");
		header.put("setName", "Set");
		header.put("setType", "Type");
		header.put("action", "Current Action");
		header.put("memory", "Memory usage");
		header.put("serviceNode", "Service Node");

		System.out.println(ColumnFormat.format(header, result));
		
		System.out.println("Finished successfully");
	}
	
  public void restore(final String techpackName, final String measurementType, final String fromDate,
			final String toDate, final String autoRun) throws MalformedURLException, NotBoundException, RemoteException {
	  LOG.info("restore: " + techpackName + " " + measurementType + " " + fromDate + " " + toDate );
	  getProperties();
    final ITransferEngineRMI termi = connect();
    LOG.info("Checking if Techpack(" + techpackName + ") is enabled.");
    System.out.format("Checking if Techpack(%s) is enabled for Techpack Type: %s.\n", techpackName,"ENIQ_EVENT");
    if(termi.isTechPackEnabled(techpackName, "ENIQ_EVENT")) {
      System.out.format("Techpack(%s) enabled.\n", techpackName);
      LOG.info("Techpack(" + techpackName + ") enabled.");
      System.out.println("Retrieving MeasurementTypes.");
      LOG.info("Retrieving MeasurementTypes.");
      final List<String> measurementTypes = termi.getMeasurementTypesForRestore(techpackName, measurementType);
      System.out.format("Found the following MeasuementTypes(%d).\n",measurementTypes.size());
      LOG.info("Found the following MeasuementTypes("+measurementTypes.size()+").");
      for(String type:measurementTypes) {
        System.out.println("MeasuementType:" + type);
        LOG.info("MeasuementType:" + type);
      }
      if(!measurementTypes.isEmpty()) {
        if (autoRun != null && autoRun.equals("autoRun")) {
          termi.restore(techpackName, measurementTypes, fromDate, toDate);
        } else {
          final Console console = System.console();
          final String confirmation = console.readLine("Do you wish to continue?[Yes,n]");
          if (confirmation.equals("Yes")) {
            termi.restore(techpackName, measurementTypes, fromDate, toDate);
          }
        }
      }
		} else {
      System.out.format("Techpack(%s) is disabled.\n", techpackName);
    }
	}

  public void disableTechpack(final String techpackName, boolean disableLogging) throws MalformedURLException, NotBoundException,
			RemoteException {
		LOG.info("Disabling Techpack: " + techpackName);

		getProperties();
		final ITransferEngineRMI termi = connect();

		System.out.println(timeStampFormat.format(new Date()) + "   Disabling Techpack...");
		termi.disableTechpack(techpackName);
		System.out.println(timeStampFormat.format(new Date()) + "   Techpack Disabled");
    if (!disableLogging) {
		showDisabledSets();
	}
  }

  public void disableSet(final String techpackName, final String setName, boolean disableLogging) throws MalformedURLException,
			NotBoundException, RemoteException {
		LOG.info("Disabling Set: " + techpackName + "." + setName);

		getProperties();
		final ITransferEngineRMI termi = connect();

		System.out.println(timeStampFormat.format(new Date()) + "   Disabling Set...");
		termi.disableSet(techpackName, setName);
		System.out.println(timeStampFormat.format(new Date()) + "   Set Disabled");
    if (!disableLogging) {
		showDisabledSets();
	}
  }

  public void disableAction(final String techpackName, final String setName, final Integer actionOrder, boolean disableLogging)
			throws MalformedURLException, NotBoundException, RemoteException {

		LOG.info("Disabling Action: " + techpackName + "." + setName + "." + actionOrder);
		
		getProperties();
		final ITransferEngineRMI termi = connect();

		System.out.println(timeStampFormat.format(new Date()) + "   Disabling Action...");
		termi.disableAction(techpackName, setName, actionOrder);
		System.out.println(timeStampFormat.format(new Date()) + "   Action Disabled");
    if (!disableLogging) {
		showDisabledSets();
	}
  }

  public void enableTechpack(final String techpackName, boolean disableLogging) throws MalformedURLException, NotBoundException,
			RemoteException {
		LOG.info("Enabling Techpack: " + techpackName);

		getProperties();
		final ITransferEngineRMI termi = connect();

		System.out.println(timeStampFormat.format(new Date()) + "   Enabling Techpack...");
		termi.enableTechpack(techpackName);
		System.out.println(timeStampFormat.format(new Date()) + "   Techpack Enabled");
    if (!disableLogging) {
		showDisabledSets();
	}
  }

  public void enableSet(final String techpackName, final String setName, boolean disableLogging) throws MalformedURLException,
			NotBoundException, RemoteException {
		LOG.info("Enabling Set: " + techpackName + "." + setName);

		getProperties();
		final ITransferEngineRMI termi = connect();

		System.out.println(timeStampFormat.format(new Date()) + "   Enabling Set...");
		termi.enableSet(techpackName, setName);
		System.out.println(timeStampFormat.format(new Date()) + "   Set Enabled");
    if (!disableLogging) {
		showDisabledSets();
	}
  }

  public void enableAction(final String techpackName, final String setName, final Integer actionNumber, boolean disableLogging)
			throws MalformedURLException, NotBoundException, RemoteException {
		LOG.info("Enabling Action: " + techpackName + "." + setName + "." + actionNumber);

		getProperties();
		final ITransferEngineRMI termi = connect();

		System.out.println(timeStampFormat.format(new Date()) + "   Enabling Action...");
		termi.enableAction(techpackName, setName, actionNumber);
		System.out.println(timeStampFormat.format(new Date()) + "   Action Enabled");
    if (!disableLogging) {
		showDisabledSets();
    }
	}

	public void showDisabledSets() throws MalformedURLException, NotBoundException, RemoteException {
		LOG.info("Get disabled sets");
		
		getProperties();
		final ITransferEngineRMI termi = connect();

		System.out.println(timeStampFormat.format(new Date()) + "   Getting Disabled Sets...");
		final List<String> disabledSets = termi.showDisabledSets();

		if (disabledSets == null) {
			System.out.println(timeStampFormat.format(new Date()) + "   There are no Disabled Sets");
		} else {
			for (String set : disabledSets) {
				System.out.println(set);
			}
		}
		System.out.println(timeStampFormat.format(new Date()) + "   Completed successfully");
	}

	public void showActiveInterfaces() throws MalformedURLException, NotBoundException, RemoteException {
		getProperties();
		final ITransferEngineRMI termi = connect();

		System.out.println(timeStampFormat.format(new Date()) + "   Getting active interfaces...");

		final List<String> activeInterfaces = termi.showActiveInterfaces();

		if (activeInterfaces != null) {
			for (String line : activeInterfaces) {
				System.out.println(line);
			}
		} else {
			System.out.println(timeStampFormat.format(new Date()) + "   There are no active interfaces.");
		}
	}

	public void status() throws MalformedURLException, NotBoundException, RemoteException {
		LOG.info("Get status");
		
		getProperties();
		List<String> engineUrls = RmiUrlFactory.getInstance().getAllEngineRmiUrls();
		System.out.println("Getting status...");
		for (String url : engineUrls) {
			ITransferEngineRMI termi = connectUsingUrl(url);
			for (String line : termi.status()) {
				System.out.println(line);
			}
		}
		System.out.println("Completed successfully");
	}

	/**
	 * shutdown engine forcefully.
	 */
	public void forceShutdown() throws MalformedURLException, NotBoundException, RemoteException {
		LOG.info("Shut down forceful");

		getProperties();
		final ITransferEngineRMI termi = connect();
		
		System.out.println("Shutting down...");
		termi.forceShutdown();
		System.out.println("Shutdown requested successfully");
	}

	/**
	 * shutdown engine gently.
	 */
	public void fastGracefulShutdown() throws MalformedURLException, NotBoundException, RemoteException {
		LOG.info("Shutting down fast graceful");

		getProperties();
		final ITransferEngineRMI termi = connect();

		System.out.println("Shutting down...");
		termi.fastGracefulShutdown();
		System.out.println("Shutdown requested successfully");
	}

	/**
	 * shutdown engine gently.
	 */
	public void slowGracefulShutdown() throws MalformedURLException, NotBoundException, RemoteException {
    LOG.info("Requesting a slow graceful shutdown...");

		getProperties();
		final ITransferEngineRMI termi = connect();
		
    System.out.println("Requesting a slow graceful shutdown..."); // NOPMD
		termi.slowGracefulShutdown();
    System.out.println("No sets are running now. Priority Queue is made inactive.\n"); // NOPMD
    System.out.println("Proceed to shutdown engine using " + System.getProperty("BIN_DIR") + "/engine stop");
    LOG.info("Proceed to shutdown engine using " + System.getProperty("BIN_DIR") + "/engine stop");
	}

	public boolean changeProfileWtext(final String profileName) throws MalformedURLException, NotBoundException,
			RemoteException {
		System.out.println("Changing profile to: " + profileName);
		final boolean result = changeProfile(profileName);
		if (result) {
			System.out.println("Change profile requested successfully");
		} else {
			System.out.println("Could not activate profile (" + profileName + ") ");
		}

		return result;
	}

	public String getCurrentProfile() throws MalformedURLException, NotBoundException, RemoteException {
		getProperties();
		final ITransferEngineRMI termi = connect();
		return termi.currentProfile();
	}

	public boolean changeProfileWtext(final String profileName, final String messageText) throws MalformedURLException,
			NotBoundException, RemoteException {
		System.out.println("Changing profile to: " + profileName);
		final boolean result = changeProfile(profileName, messageText);
		if (result) {
			System.out.println("Change profile (" + profileName + ") requested successfully. Reason for change: "
					+ messageText);
		} else {
			System.out.println("Could not activate profile (" + profileName + ") ");
		}

		return result;
	}

	public boolean changeProfileAndWaitWtext(final String profileName) throws MalformedURLException, NotBoundException,
			RemoteException {
		System.out.println("Changing profile to: " + profileName);
		final boolean result = changeProfileAndWait(profileName);
		if (result) {
			System.out.println("Change profile requested successfully");
		} else {
			System.out.println("Could not activate profile (" + profileName + ")");
		}

		return result;
	}

	public void addWorkerToQueue(final String name, final String type, final Object wobj) throws MalformedURLException,
			NotBoundException, RemoteException {

		getProperties();
		final ITransferEngineRMI termi = connect();
		termi.addWorkerToQueue(name, type, wobj);

	}

	public Set<Object> getAllRunningExecutionSlotWorkers() throws MalformedURLException, NotBoundException,
			RemoteException {
		getProperties();
		final ITransferEngineRMI termi = connect();
		return termi.getAllRunningExecutionSlotWorkers();

	}

	public boolean changeProfile(final String profileName) throws MalformedURLException, NotBoundException,
			RemoteException {
		return changeProfile(profileName, true);
	}

  public boolean changeProfile(final String profileName, final boolean resetConnMonitor) throws MalformedURLException, NotBoundException,
  			RemoteException {
  		LOG.info("Change profile to " + profileName);

  		getProperties();
  		final ITransferEngineRMI termi = connect();
  		return termi.setActiveExecutionProfile(profileName, resetConnMonitor);

  	}

	public boolean changeProfile(final String profileName, final String messageText) throws MalformedURLException,
			NotBoundException, RemoteException {
		LOG.info("Change profile to " + profileName + " message " + messageText);
		
		getProperties();
		final ITransferEngineRMI termi = connect();
		return termi.setActiveExecutionProfile(profileName, messageText);

	}

	public boolean changeProfileAndWait(final String profileName) throws MalformedURLException, NotBoundException,
			RemoteException {
		LOG.info("Change profile to " + profileName + " and wait");

		getProperties();
		final ITransferEngineRMI termi = connect();
		return termi.setAndWaitActiveExecutionProfile(profileName);

	}

	public void unLockExecutionprofile() throws MalformedURLException, NotBoundException, RemoteException {
		LOG.info("Unlocking Execution Profile... ");
		
		getProperties();
		final ITransferEngineRMI termi = connect();
		
		System.out.println("Unlocking Execution Profile");
		termi.unLockExecutionprofile();
		System.out.println("Profile unlocked successfully");

	}

	public void refreshTransformations() throws MalformedURLException, NotBoundException, RemoteException {
		LOG.info("Refreshing transformations");
		
		getProperties();
		final ITransferEngineRMI termi = connect();
		
		System.out.println("Refresing transformations...");
		termi.reloadTransformations();
		System.out.println("Transformations refreshed succesfully");

	}

	public void reloadLogging() throws MalformedURLException, NotBoundException, RemoteException {
		LOG.info("Reloading logging levels");
		
		getProperties();
		final ITransferEngineRMI termi = connect();
		
		System.out.println("Reloading logging levels...");
		termi.reloadLogging();
		System.out.println("Logging levels reloaded succesfully");

	}

	public void loggingStatus() throws MalformedURLException, NotBoundException, RemoteException {
		LOG.info("Print logging status");
		
		getProperties();
		final ITransferEngineRMI termi = connect();
		
		System.out.println("Querying logging status...");
		for (String line : termi.loggingStatus()) {
			System.out.println(line);
		}
		System.out.println("Logging status printed succesfully");

	}

	public void updateTransformation(final String tpName) throws MalformedURLException, NotBoundException,
			RemoteException {
		LOG.info("Update transformation " + tpName);
		
		getProperties();
		final ITransferEngineRMI termi = connect();
		
		System.out.println("Updating transformation...");
		termi.updateTransformation(tpName);
		System.out.println("Transformation " + tpName + " updated succesfully");

	}

	public void refreshDBLookups(final String tableName) throws MalformedURLException, NotBoundException, RemoteException {
		LOG.info("Refresh database lookups");
		
		getProperties();
		final ITransferEngineRMI termi = connect();
		
		System.out.println("Refreshing database lookups...");
		termi.reloadDBLookups(tableName);
		System.out.println("Lookups refreshed succesfully");

	}

	public void lockExecutionprofile() throws MalformedURLException, NotBoundException, RemoteException {
		LOG.info("Lock execution profile");
		
		getProperties();
		final ITransferEngineRMI termi = connect();
		
		System.out.println("Locking Execution Profile...");
		termi.lockExecutionprofile();
		System.out.println("Profile locked successfully");

	}

	public Set<String> getAllActiveSetTypesInExecutionProfiles() throws MalformedURLException, NotBoundException,
			RemoteException {

		getProperties();
		final ITransferEngineRMI termi = connect();
		return termi.getAllActiveSetTypesInExecutionProfiles();

	}

	public void reloadProfiles() throws MalformedURLException, NotBoundException, RemoteException {
		LOG.info("Reload execution profiles");

		getProperties();
		final ITransferEngineRMI termi = connect();
		
		System.out.println("Reloading profiles...");
		termi.reloadExecutionProfiles();
		System.out.println("Reload profiles requested successfully");
	}

	public void holdPriorityQueue() throws MalformedURLException, NotBoundException, RemoteException {
		LOG.info("Hold priority queue");

		getProperties();
		final ITransferEngineRMI termi = connect();
		
		System.out.println("Holding priority queue...");
		termi.holdPriorityQueue();
		System.out.println("Priority hold successfully");
	}

	public void restartPriorityQueue() throws MalformedURLException, NotBoundException, RemoteException {
		LOG.info("Restart priority queue");

		getProperties();
		final ITransferEngineRMI termi = connect();
		
		System.out.println("Restarting priority queue...");
		termi.restartPriorityQueue();
		System.out.println("Restart priority queue requested successfully");
	}

	public void startSet(final String ip, final String username,
			final String passwd, final String driver, final String coll,
			final String set) throws MalformedURLException, NotBoundException,
			RemoteException {
		LOG.info(timeStampFormat.format(new Date()) + "	Start set " + set);
		System.out.println(timeStampFormat.format(new Date()) + "	Start set " + set);

		getProperties();
		final ITransferEngineRMI termi = connect(coll);
		if (set.contains("DWHM_StorageTimeUpdate_") || set.contains("DWHM_Install_")) {
			startAndWaitSet(coll, set, "");
		} else {
			termi.execute(ip, username, passwd, driver, coll, set);
		}
		LOG.info(timeStampFormat.format(new Date()) + "	Start set requested successfully");
		System.out.println(timeStampFormat.format(new Date()) + "	Start set requested successfully");
	}

	public void startSet(final String coll, final String set, final String scheduleInfo) throws MalformedURLException,
			NotBoundException, RemoteException {
		LOG.info(timeStampFormat.format(new Date()) + "	Starting set " + set);
		System.out.println(timeStampFormat.format(new Date()) + "	Starting set " + set);

		getProperties();
		final ITransferEngineRMI termi = connect(coll);
		if (set.contains("DWHM_StorageTimeUpdate_") || set.contains("DWHM_Install_")) {
			startAndWaitSet(coll, set, scheduleInfo);
		} else {
			termi.execute(coll, set, scheduleInfo);
		}

		LOG.info(timeStampFormat.format(new Date()) + "	Start set requested successfully");
		System.out.println(timeStampFormat.format(new Date()) + "	Start set requested successfully");
	}

	public void startAndWaitSet(final String coll, final String set, final String scheduleInfo) 
			throws MalformedURLException, NotBoundException, RemoteException {
		LOG.info(timeStampFormat.format(new Date()) + "	StartAndWaitSet called for " + set);
		System.out.println(timeStampFormat.format(new Date()) + "	StartAndWaitSet called for " + set);

		getProperties();
		final ITransferEngineRMI termi = connect(coll);
		List<String> dependentList = new ArrayList<String>();
		dependentList.add(coll);

		if (set.contains("DWHM_StorageTimeUpdate_") || set.contains("DWHM_Install_")) {
			System.out.println("Disable techpack for " + dependentList);
			LOG.info("Disable techpack for " + dependentList);
			termi.disableDependentTP(dependentList);
		}

		System.out.println("Executing set : " + set);
		LOG.info("Executing set : " + set);
		final String status = termi.executeAndWait(coll, set, scheduleInfo);

		if (set.contains("DWHM_StorageTimeUpdate_") || set.contains("DWHM_Install_")) {
			System.out.println("Enabling techpack for " + dependentList);
			LOG.info("Enabling techpack for " + dependentList);
			termi.enableDependentTP(dependentList);
		}
		
		LOG.info(timeStampFormat.format(new Date()) + "	Set execution of " + set + " finished with status: " + status);
		System.out.println(timeStampFormat.format(new Date()) + "	Set execution of " + set + " finished with status: " + status);

		if (status.equals(SetListener.SUCCEEDED)) {
			System.exit(0);
		} else if (status.equals(SetListener.NOSET)) {
			System.exit(1);
		} else if (status.equals(SetListener.DROPPED)) {
			System.exit(2);
		} else {
			System.exit(69);
		}
	}
	
	public ITransferEngineRMI connect(String collectionName) throws MalformedURLException, RemoteException, NotBoundException {
		int reTryCount = 120;
        ITransferEngineRMI termi = null;
        String collectionType = getCollectionType(collectionName);
        if (collectionType.isEmpty()) {
        	LOG.log(Level.INFO,"Could not get collectionType for CollectionName : "
        			+ collectionName+" , falling back to round robin");
        	return connect();
        }
        while(true){
              termi = (ITransferEngineRMI) Naming.lookup(RmiUrlFactory.getInstance().getEngineRmiUrl(collectionType));
              if (termi == null && reTryCount != 0){
                    termi = (ITransferEngineRMI) Naming.lookup(RmiUrlFactory.getInstance().getEngineRmiUrl(collectionType));
                    try {
                          Thread.sleep(5 * 1000);
                          System.out.println(timeStampFormat.format(new Date()) + "   Retrying RMI Connect in 5 sec.." );
                    } catch (InterruptedException e) {
                    	System.err.println(timeStampFormat.format(new Date()) + "  Couldn't connect to RMI Registry. Error while retrying: " + e);
                    }
                    reTryCount--;
              }
              if (termi == null){
                    System.err.println(timeStampFormat.format(new Date()) + "   Couldn't connect to RMI Registry even after retry. ");
                    System.exit(99);
              }
              break;
        }
        return termi; 
	}
			
	
	private String getCollectionType(String collectionName) {
		String result = "";
		final String selectSql = "SELECT SETTYPE FROM META_COLLECTIONS WHERE COLLECTION_NAME = ?";
		try(Connection conn = DatabaseConnections.getETLRepConnection().getConnection();
			PreparedStatement stmt = conn.prepareStatement(selectSql);
			) {
			stmt.setString(0, collectionName);
			try (ResultSet rs = stmt.executeQuery()) {
				if (rs.next()) {
					result = rs.getString(0);
				}
			}
		}catch (Exception e) {
			LOG.log(Level.WARNING, "Not able to query the META_COLLECTIONS table",e);
		}
		LOG.log(Level.INFO, "Collection Type : "+result+" returned for collection name : "+ collectionName);
		return result;
	}

	public void startSets(final String techpacks, final String sets, final int times) throws MalformedURLException,
			NotBoundException, RemoteException, InterruptedException {

		for (int i = 0; i < times; i++) {
			final StringTokenizer collTokens = new StringTokenizer(techpacks, ",");
			final StringTokenizer setTokens = new StringTokenizer(sets, ",");

			if (collTokens.countTokens() == setTokens.countTokens()) {

				while (collTokens.hasMoreTokens()) {
					final String tp = collTokens.nextToken();
					final String set = setTokens.nextToken();

					try {

						LOG.info("Starting: " + tp + "/" + set);
						
						getProperties();
						final ITransferEngineRMI termi = connect(tp);
						
						System.out.println("Starting (" + i + "): " + tp + "/" + set);
						termi.execute(tp, set, "");
						System.out.println("Start set requested successfully");

					} catch (final Exception e) {
						System.out.println("Error in: " + tp + "/" + set + "\n" + e);
						Thread.sleep(1000);
					}

				}
			}
		}

	}

	public void reloadProperties() throws MalformedURLException, NotBoundException, RemoteException {
		getProperties();
		final ITransferEngineRMI termi = connect();
		termi.reloadProperties();
	}

	public void reloadAggregationCache() throws MalformedURLException, NotBoundException, RemoteException {
		LOG.info("Reload aggregation cache");
		
		getProperties();
		final ITransferEngineRMI termi = connect();
		
		System.out.println("Reloading aggregation cache...");
		termi.reloadAggregationCache();
		System.out.println("Reload aggregation cache requested successfully");
	}

	public void reloadPropertiesFromConfigTool() throws MalformedURLException, NotBoundException, RemoteException {
		// Do not run getProperties() here.
		final ITransferEngineRMI termi = connect();
		termi.reloadProperties();
	}

	public void reloadAlarmConfigCache() throws MalformedURLException, NotBoundException, RemoteException {
		LOG.info("Reload alarm cache");
		getProperties();
		System.out.println("Reload alarm cache");

		final ITransferEngineRMI termi = connect();
		termi.reloadAlarmConfigCache();
		System.out.println("Reload alarm cache requested successfully");
	}
	
	public void reloadBackupConfigCache() throws MalformedURLException, NotBoundException, RemoteException {
		LOG.info("Reload backup cache");
		getProperties();
		System.out.println("Reload backup cache");
		
		final ITransferEngineRMI termi = connect();
		termi.reloadBackupConfigCache();		      
		System.out.println("Reload Backup Configuration cache requested successfully");
		
	}
	
	public void triggerRestoreOfData() throws RemoteException, MalformedURLException, NotBoundException {
		LOG.info("Triggering the restore for all backup data");
		getProperties();
		
		final ITransferEngineRMI termi = connect();
		termi.triggerRestoreOfData();
		System.out.println("Triggering of restore data requested successfully");
	}

	public boolean testConnection() throws MalformedURLException, NotBoundException, RemoteException {
		boolean success = true;

		try {
			final ITransferEngineRMI termi = connect(); // NOPMD
		} catch (final Exception e) {
			success = false;
		}

		return success;

	}

	public void giveEngineCommand(final String com) throws MalformedURLException, NotBoundException, RemoteException {
		LOG.info("Engine command " + com);
		
		final ITransferEngineRMI termi = connect();
		
		System.out.println("Giving engine command...");
		termi.giveEngineCommand(com);
		System.out.println("Engine command given successfully");

	}

	public void reloadPropertiesWText() throws MalformedURLException, NotBoundException, RemoteException {
		LOG.info("Reload properties");
		
		System.out.println("Reloading properties...");
		reloadProperties();
		System.out.println("Reload properties requested successfully");
	}

	public void removeSetFromPriorityQueue(final long ID) throws MalformedURLException, NotBoundException,
			RemoteException {
		LOG.info("Remove set " + ID + " from queue");

		getProperties();
		final ITransferEngineRMI termi = connect();
		
		System.out.println("Removing set from priority queue...");
		if (termi.removeSetFromPriorityQueue(Long.valueOf(ID))) {
			System.out.println("Set (" + ID + ") Removed from priority queue successfully");
		} else {
			System.out.println("Set (" + ID + ") not removed from priority queue");
		}
	}

	public void changeSetPriorityInPriorityQueue(final long ID, final long priority) throws MalformedURLException,
			NotBoundException, RemoteException {
		LOG.info("Change priority of set " + ID + " to " + priority + " in queue");

		getProperties();
		final ITransferEngineRMI termi = connect();
		
		System.out.print("Changing set priority...");
		if (termi.changeSetPriorityInPriorityQueue(Long.valueOf(ID), priority)) {
			System.out.println("Set (" + ID + ")  priority changed to " + priority + " successfully");
		} else {
			System.out.println("Set (" + ID + ") priority not changed to " + priority);
		}
	}

	public void activateSetInPriorityQueue(final long ID) throws MalformedURLException, NotBoundException,
			RemoteException {
		LOG.info("Activate set " + ID + " in queue");
		
		getProperties();
		final ITransferEngineRMI termi = connect();
		
		System.out.print("Activating set...");
		termi.activateSetInPriorityQueue(Long.valueOf(ID));
		System.out.println("Set (" + ID + ") is activated ");

	}

	public void holdSetInPriorityQueue(final long ID) throws MalformedURLException, NotBoundException, RemoteException {
		LOG.info("Hold set " + ID + " in queue");
		
		getProperties();
		final ITransferEngineRMI termi = connect();
		
		System.out.print("Holding set...");
		termi.holdSetInPriorityQueue(Long.valueOf(ID));
		System.out.println("Set (" + ID + ") is set on hold ");
	}

	public boolean isSetRunning(final Long techpackID, final Long setID) throws MalformedURLException, NotBoundException,
			RemoteException {
		getProperties();
		final ITransferEngineRMI termi = connect();
		return termi.isSetRunning(techpackID, setID);
	}

	public boolean isSetInQueue(final String setname) throws MalformedURLException, NotBoundException, RemoteException {
		getProperties();
		final ITransferEngineRMI termi = connect();
		final List<Map<String, String>> l = termi.getQueuedSets();

		boolean ret = false;
		
		for(Map<String, String> setMap : l) {
			if (setMap.get("techpackName").equals(setname)) {
				ret = true;
			}
		}
		
		return ret;
	}

	public void showSetsInQueue() throws MalformedURLException, NotBoundException, RemoteException {
		LOG.info("Show sets in queue");
		
		getProperties();
		final ITransferEngineRMI termi = connect();
		
		System.out.println("Querying sets in queue...");
		final List<Map<String, String>> setList = termi.getQueuedSets();

		final Map<String, String> header = new LinkedHashMap<String, String>();
		header.put("ID", "ID");
		header.put("techpackName", "TechPack");
		header.put("version", "Version");
		header.put("setName", "SetName");
		header.put("setType", "SetType");
		header.put("priority", "Prio");
		header.put("creationDate", "Created");
		header.put("active", "Active");
		header.put("schedulingInfo", "Scheduling");
		header.put("earliestExec", "EarliestExecution");
		header.put("lastExec", "LastExecution");

		System.out.println(ColumnFormat.format(header, setList));

		System.out.println("Finished successfully");
	}

  public void showSetsInExecutionSlots(final List<String> techPackNames) throws MalformedURLException, NotBoundException, RemoteException {
		LOG.info("Show sets in execution slots");
		
		getProperties();
		final ITransferEngineRMI termi = connect();
		
		if( isEngineInitializedWithFullCacheRefresh(termi) ){	
			System.out.println(timeStampFormat.format(new Date()) + "   Querying sets in execution...");
			final List<Map<String, String>> setList = termi.getRunningSets(techPackNames);
	
			final Map<String, String> header = new LinkedHashMap<String, String>();
			header.put("techpackName", "TechPack");
			header.put("version", "Version");
			header.put("setName", "SetName");
			header.put("setType", "SetType");
			header.put("startTime", "StartTime");
			header.put("priority", "Prio");
			header.put("runningSlot", "Slot");
			header.put("runningAction", "Action");
			header.put("schedulingInfo", "Scheduling");
	
			System.out.println(ColumnFormat.format(header, setList));
	
			System.out.println(timeStampFormat.format(new Date()) + "   Finished successfully");
		}
	}

	public void changeAggregationStatus(final String status, final String aggregation, final long datadate)
			throws MalformedURLException, NotBoundException, RemoteException {

		final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault());
		LOG.info("Change aggregation status " + aggregation + " @ " + sdf.format(new Date(datadate)) + " to " + status);

		getProperties();
		final ITransferEngineRMI termi = connect();
		
		System.out.println("Changing aggregation " + aggregation + " at " + sdf.format(new Date(datadate)) + " status to "
				+ status);
		termi.changeAggregationStatus(status, aggregation, datadate);
		System.out.println("Finished successfully");
	}

	public void acticateScheduler() throws MalformedURLException, NotBoundException, RemoteException {
		LOG.info("Activate scheduler");
		
		getProperties();
		final ITransferEngineRMI termi = connect();
		termi.activateScheduler();

	}

  public List<String> getTableNamesForRawEvents(final String viewName, final Timestamp startTime,
			final Timestamp endTime) throws MalformedURLException, NotBoundException, RemoteException {
    LOG.info("Get Table Names For Raw Events");
    getProperties();
    final ITransferEngineRMI termi = connect();
    final List<String> tableNames = termi.getTableNamesForRawEvents(viewName, startTime, endTime);

    for (String tableName : tableNames) {
      System.out.println(tableName);
    }
    return tableNames;
  }

	public List<String> getLatestTableNamesForRawEvents(final String viewName) throws MalformedURLException,
			NotBoundException, RemoteException {
    LOG.info("Get Latest Table Names For Raw Events");
    getProperties();
    final ITransferEngineRMI termi = connect();
    final List<String> tableNames = termi.getLatestTableNamesForRawEvents(viewName);

    for (String tableName : tableNames) {
      System.out.println(tableName);
    }
    return tableNames;
  }

  public void manualCountReAgg(final String techPackName, final Timestamp minTimestamp, final Timestamp maxTimestamp,
			final String intervalName, final boolean isScheduled) throws MalformedURLException, NotBoundException,
			RemoteException {
    LOG.info("Running Manual Count ReAggregation");

    getProperties();
    final ITransferEngineRMI termi = connect();
    if (termi.isTechPackEnabled(techPackName, "ENIQ_EVENT")) {
      if (termi.isIntervalNameSupported(intervalName)) {
        final long oldestReAggTimeInMs = termi.getOldestReAggTimeInMs(techPackName);
        if (oldestReAggTimeInMs != -1) {
          if (minTimestamp.getTime() >= oldestReAggTimeInMs) {
            if (maxTimestamp.getTime() >= oldestReAggTimeInMs) {
              termi.manualCountReAgg(techPackName, minTimestamp, maxTimestamp, intervalName, isScheduled);
            } else {
              System.out.println("maxTimeStamp = '" + minTimestamp + "'  is too old. \n"
                  + "Oldest manual re-aggregation allowed is '" + new Timestamp(oldestReAggTimeInMs) + "'\n");
            }
          } else {
            System.out.println("minTimeStamp = '" + minTimestamp + "'  is too old. \n"
                + "Oldest manual re-aggregation allowed is '" + new Timestamp(oldestReAggTimeInMs) + "'\n");
          }
        } else {
          System.out.println("No data exists or data is not old enough for manual re-aggregation.\n");
        }
      } else {
        System.out.println("intervalName  = '" + intervalName + "' is not supported.\n");
      }
    } else {
      System.out.format("Techpack(%s) is disabled.\n", techPackName);
    }
  }

	public void setRMI(final String serverHostName, final int serverPort) {
		this.serverHostName = serverHostName;
		this.serverPort = serverPort;
	}

	/**
	 * Reads configuration
	 */
	private void getProperties() {
				
		try {

			final Properties appProps = new ETLCServerProperties ();
			
			if (this.serverHostName == null || this.serverHostName.equalsIgnoreCase("")) {
        //Get the service name for engine (or at least host/ip from service_names)
        final String engineServiceName = ServicenamesHelper.getServiceHost("engine", "localhost");
        //Does ETLCServer.properties override anything
				this.serverHostName = appProps.getProperty("ENGINE_HOSTNAME", engineServiceName);
				if (this.serverHostName == null) { // trying to determine hostname
					this.serverHostName = "localhost";

					try {
						this.serverHostName = InetAddress.getLocalHost().getHostName();
					} catch (java.net.UnknownHostException ex) {
						LOG.info("Unable to resolve hostname");
					}
				}
			}

			this.serverPort = 1200;
			final String sporttmp = appProps.getProperty("ENGINE_PORT", "1200");
			try {
				this.serverPort = Integer.parseInt(sporttmp);
			} catch (NumberFormatException nfe) {
				LOG.info("ENGINE_PORT mallformed");
			}

			this.serverRefName = appProps.getProperty("ENGINE_REFNAME", "TransferEngine");

		} catch (Exception e) {
			System.err.println("Cannot read configuration: " + e.getMessage());
		} 
	}
	
	private ITransferEngineRMI connectUsingUrl(String url) throws NotBoundException, MalformedURLException, RemoteException {
		// Retry upto 10 min if RMI Registry is not connected/binded. 
		int reTryCount = 120;
        ITransferEngineRMI termi = null;
        while(true){
              termi = (ITransferEngineRMI) Naming.lookup(url);
              if (termi == null && reTryCount != 0){
                    termi = (ITransferEngineRMI) Naming.lookup(url);
                    try {
                          Thread.sleep(5 * 1000);
                          System.out.println(timeStampFormat.format(new Date()) + "   Retrying RMI Connect in 5 sec.." );
                    } catch (InterruptedException e) {
                    	System.err.println(timeStampFormat.format(new Date()) + "  Couldn't connect to RMI Registry. Error while retrying: " + e);
                    }
                    reTryCount--;
              }
              if (termi == null){
                    System.err.println(timeStampFormat.format(new Date()) + "   Couldn't connect to RMI Registry even after retry. ");
                    System.exit(99);
              }
              break;
        }
        return termi;
	}

	/**
	 * Looks up the transfer engine
	 */
	private ITransferEngineRMI connect() throws NotBoundException, MalformedURLException, RemoteException {
		// Retry upto 10 min if RMI Registry is not connected/binded. 
		int reTryCount = 120;
        ITransferEngineRMI termi = null;
        while(true){
              termi = (ITransferEngineRMI) Naming.lookup(RmiUrlFactory.getInstance().getEngineRmiUrl());
              if (termi == null && reTryCount != 0){
                    termi = (ITransferEngineRMI) Naming.lookup(RmiUrlFactory.getInstance().getEngineRmiUrl());
                    try {
                          Thread.sleep(5 * 1000);
                          System.out.println(timeStampFormat.format(new Date()) + "   Retrying RMI Connect in 5 sec.." );
                    } catch (InterruptedException e) {
                    	System.err.println(timeStampFormat.format(new Date()) + "  Couldn't connect to RMI Registry. Error while retrying: " + e);
                    }
                    reTryCount--;
              }
              if (termi == null){
                    System.err.println(timeStampFormat.format(new Date()) + "   Couldn't connect to RMI Registry even after retry. ");
                    System.exit(99);
              }
              break;
        }
        return termi;
	}

	static {
		CMD_TO_CLASS.put("activateSetInPriorityQueue", ActivateSetInPriorityQueueCommand.class);
		CMD_TO_CLASS.put("changeAggregationStatus", ChangeAggregationStatusCommand.class);
		CMD_TO_CLASS.put("changeProfile", ChangeProfileCommand.class);
		CMD_TO_CLASS.put("changeProfileAndWait", ChangeProfileAndWaitCommand.class);
		CMD_TO_CLASS.put("changeSetPriorityInPriorityQueue", ChangeSetPriorityInPriorityQueueCommand.class);
		CMD_TO_CLASS.put("disableSet", DisableSetCommand.class);
		CMD_TO_CLASS.put("enableSet", EnableSetCommand.class);
		CMD_TO_CLASS.put("giveEngineCommand", GiveEngineCommand.class);
		CMD_TO_CLASS.put("holdPriorityQueue", HoldPriorityQueueCommand.class);
		CMD_TO_CLASS.put("holdSetInPriorityQueue", HoldSetInPriorityQueueCommand.class);
		CMD_TO_CLASS.put("lockExecutionprofile", LockExecutionProfileCommand.class);
		CMD_TO_CLASS.put("loggingStatus", LoggingStatusCommand.class);
		CMD_TO_CLASS.put("printSlotInfo", PrintSlotInfoCommand.class);
		CMD_TO_CLASS.put("printServiceInfo", PrintServiceConnInfoCommand.class);
		CMD_TO_CLASS.put("queue", ShowSetsInQueueCommand.class);
		CMD_TO_CLASS.put("restore", RestoreCommand.class);
		CMD_TO_CLASS.put("refreshDBLookups", RefreshDBLookupsCommand.class);
		CMD_TO_CLASS.put("refreshTransformations", RefreshTransformationsCommand.class);
		CMD_TO_CLASS.put("reloadAggregationCache", ReloadAggregationCacheCommand.class);
		CMD_TO_CLASS.put("reloadConfig", ReloadConfigCommand.class);
		CMD_TO_CLASS.put("reloadLogging", ReloadLoggingCommand.class);
		CMD_TO_CLASS.put("reloadProfiles", ReloadProfilesCommand.class);
		CMD_TO_CLASS.put("reloadAlarmCache", ReloadAlarmCacheCommand.class);
		CMD_TO_CLASS.put("reloadBackupCache", ReloadBackupCacheCommand.class);
		CMD_TO_CLASS.put("triggerRestoreOfData", RestoreOfData.class);
		CMD_TO_CLASS.put("removeSetFromPriorityQueue", RemoveSetFromPriorityQueueCommand.class);
		CMD_TO_CLASS.put("restartPriorityQueue", RestartPriorityQueueCommand.class);
		CMD_TO_CLASS.put("showDisabledSets", ShowDisabledSetsCommand.class);
		CMD_TO_CLASS.put("showSetsInExecutionSlots", ShowSetsInExecutionSlotsCommand.class);
		CMD_TO_CLASS.put("showSetsInQueue", ShowSetsInQueueCommand.class);
		CMD_TO_CLASS.put("shutdown_slow", ShutdownSlowCommand.class);
		CMD_TO_CLASS.put("slots", ShowSetsInExecutionSlotsCommand.class);
		CMD_TO_CLASS.put("startAndWaitSet", StartAndWaitSetCommand.class);
		CMD_TO_CLASS.put("startSetInEngine", StartSetInEngineCommand.class);
		CMD_TO_CLASS.put("startSet", StartSetCommand.class);
		CMD_TO_CLASS.put("startSets", StartSetsCommand.class);
		CMD_TO_CLASS.put("status", StatusCommand.class);
		CMD_TO_CLASS.put("shutdown_fast", StopOrShutdownFastCommand.class);
		CMD_TO_CLASS.put("shutdown_forceful", ShutdownForcefulCommand.class);
		CMD_TO_CLASS.put("stop", StopOrShutdownFastCommand.class);
		CMD_TO_CLASS.put("unLockExecutionprofile", UnlockExecutionProfileCommand.class);
		CMD_TO_CLASS.put("updateTransformation", UpdateTransformationCommand.class);
		CMD_TO_CLASS.put("showActiveInterfaces", ShowActiveInterfaces.class);
		CMD_TO_CLASS.put("getProfile", GetProfileCommand.class);
    CMD_TO_CLASS.put("getTableNamesForRawEvents", GetTableNamesForRawEventsCommand.class);
    CMD_TO_CLASS.put("manualCountReAgg", ManualCountReAggCommand.class);
    CMD_TO_CLASS.put("getLatestTableNamesForRawEvents", GetLatestTableNamesForRawEventsCommand.class);
		CMD_TO_CLASS.put("currentProfile", GetProfileCommand.class);
		CMD_TO_CLASS.put("getCurrentProfile", GetProfileCommand.class);//Added for TR HR87687
    CMD_TO_CLASS.put("updatethresholdLimit", UpdateThresholdLimit.class);
    CMD_TO_CLASS.put("removeTechPacksInPriorityQueue", RemoveTechPacksInPriorityQueueCommand.class);
    CMD_TO_CLASS.put("killRunningSets", KillRunningSetsCommand.class);
    CMD_TO_CLASS.put("lockEventsUIusers", LockEventsUIusersCommand.class);
	}

/**
   * This method updates the threshold property in staticProperties
   * @param numberOfMinutes - The time the user specifies.
   * @throws Exception
   */
  public void updateThresholdProperty(final int numberOfMinutes) throws Exception {

    final String name = EngineConstants.THRESHOLD_NAME;
    StaticProperties.reload();
    final boolean savedOK = StaticProperties.setProperty(name, Integer.toString(numberOfMinutes));
    if(savedOK) {
      LOG.info("StaticProperties saved.");
    }
  }

  /**
   * Hold sets for tech packs in the priority queue.
   * @param techPackNames
   */
  public void removeTechPacksInPriorityQueue(List<String> techPackNames) throws MalformedURLException, NotBoundException, RemoteException {
    LOG.info("Removing tech pack sets from queue");    
    getProperties();
    final ITransferEngineRMI termi = connect();    
    if( isEngineInitializedWithFullCacheRefresh(termi) ){
	    termi.removeTechPacksInPriorityQueue(techPackNames);
	    System.out.println(timeStampFormat.format(new Date()) + "   Removed tech pack sets from queue");
    }
  }

  /**
   * Kills currently running sets for a list of tech packs.
   * @param techPackNames
   * @throws MalformedURLException
   * @throws NotBoundException
   * @throws RemoteException
   */
  public void killRunningSets(List<String> techPackNames) throws MalformedURLException, NotBoundException, RemoteException {
    LOG.info("Killing running sets");    
    getProperties();
    final ITransferEngineRMI termi = connect();   
    if( isEngineInitializedWithFullCacheRefresh(termi) ){
	    termi.killRunningSets(techPackNames);
	    System.out.println(timeStampFormat.format(new Date()) + "   Killed running sets");   
    }    
  }
  
  /**
   * Lock or unlock Events UI users.
   * @param lock
   * @throws MalformedURLException
   * @throws NotBoundException
   * @throws RemoteException
   */
  public void lockEventsUIusers(final boolean lock) throws MalformedURLException, NotBoundException, RemoteException {
    LOG.info("Locking Events UI users");    
    getProperties();
    final ITransferEngineRMI termi = connect();   
    termi.lockEventsUIusers(lock);
    if (lock) {
      System.out.println("Locked Events UI users");      
    } else {
      System.out.println("Unlocked Events UI users"); 
    }
  }
  
  public boolean isEngineInitializedWithFullCacheRefresh( final ITransferEngineRMI termi )
		  throws MalformedURLException, NotBoundException, RemoteException{
	  
	  boolean isEngineInitializedWithFullCacheRefresh = false;
	  while (true){
		  if (termi.isInitialized()){
			  if (termi.isCacheRefreshed()){
				  isEngineInitializedWithFullCacheRefresh = true;
				  break;
			  } else{
				  System.out.println(timeStampFormat.format(new Date()) + "   Waiting for engine to refresh cache..!" );
			  }
		  }
		  try {
			Thread.sleep(3 *1000);
		} catch (InterruptedException e) {
			System.err.println(timeStampFormat.format(new Date()) + "  Error while Waiting for engine to refresh cache: " + e);
		}
	  }
	  return isEngineInitializedWithFullCacheRefresh;
  }

}
