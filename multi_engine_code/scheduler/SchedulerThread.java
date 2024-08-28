package com.distocraft.dc5000.etl.scheduler;

import java.net.MalformedURLException;
import java.rmi.*;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.RmiUrlFactory;
import com.distocraft.dc5000.etl.engine.main.ITransferEngineRMI;
import com.distocraft.dc5000.etl.rock.*;
import com.distocraft.dc5000.etl.scheduler.trigger.*;
import com.ericsson.eniq.common.RemoteExecutor;
import com.ericsson.eniq.scheduler.exception.SchedulerException;

public class SchedulerThread extends Thread {

	private static final String APPLICATION_USER = "dcuser";

	private Logger log = Logger.getLogger("scheduler.thread");

	private static HashMap<Meta_schedulings, String> tpcache = new HashMap<Meta_schedulings, String>();

	// Time to connect again to the database if it fails
	private final long reConnectTime;

	// Trigger list
	private Vector<IScheduleTrigger> triggerThreadList = null;

	// The database connect object
	private RockFactory rockFact;

	// The database connect information:
	private final String url;

	private final String userName;

	private final String password;

	private final String dbDriverName;

	// Boolean value for stopping the execution
	private boolean stopped = false;

	private long pollInterval = 5000;

	private static Map<String, Integer> penaltyBox = new HashMap<String, Integer>();

	private int penaltyWait = 30;
	

	private boolean flag = false;

	private boolean retryExecution;

	private int retrycount = 0;

	private int retryTriggerCount = 0;
	
	public static boolean schedulerState = false;
	
	/**
	 * Constructor for starting the transfer
	 */
	public SchedulerThread(final long pollInterval, final int penaltyWait,
			final long reConnectTime, final String url,
			final String dbDriverName, final String userName,
			final String password) throws RemoteException {

		this.url = url;
		this.dbDriverName = dbDriverName;
		this.userName = userName;
		this.password = password;
		this.stopped = false;
		this.reConnectTime = reConnectTime;
		this.pollInterval = pollInterval;
		this.penaltyWait = penaltyWait;
		this.retryExecution = true;
	}

	/**
	 * Stops the thread execution.
	 * 
	 * @exception RemoteException
	 */
	public void cancel() {
		try {
			// Stop execution
			this.stopped = true;
			
			// Wait to actually stop the thread
			sleep((this.pollInterval * 2));

			// Clear data
			this.clearThreads();
			
			// Make sleep for 10 sec
			Thread.sleep(10 * 1000);
		} catch (Exception e) {
			log.log(Level.SEVERE, "Cancel failed exceptionally, exiting...: ", e);
		} finally {
			try {
				if (rockFact != null) {
					if (rockFact.getConnection() != null) {
						rockFact.getConnection().close();
					}
				}
			} catch (Exception ex) {
				log.log(Level.SEVERE, "Exception thrown while closing repdb connection...: ", ex);
			}
		}
	}

	/**
	 * Creates threads
	 * 
	 * @exception RemoteException
	 */
	@Override
	public void run() {
		log.info("Starting Scheduler Thread... ");
		try {
			retryExecution = true;
			schedulerState = false;
			
			// Creates threads from metadata
			makeThreads();

			// loop until we drop...
			while (!this.stopped) {
				try {
					sleep(this.pollInterval);
					startThreads();
				} catch (Exception e) {
					Logger.getLogger("scheduler").log(Level.SEVERE, "General Error in Scheduler", e);
				}
			}
			
			// Make sleep for 10 sec
			Thread.sleep(10 * 1000);
		} catch (Exception e) {
			log.log(Level.SEVERE, "Unrecoverable error occured", e);
		} finally {
			if (rockFact != null) {
				try {
					rockFact.getConnection().close();
				} catch (Exception e) {
					log.warning("Couldn't able to close Rockfactory connection... " + e);
				}
			}
		}
	}

	/**
	 * Destroys the threads in the threadlist and removes all elements from the
	 * vector.
	 */
	private void clearThreads() throws SQLException {
		if (this.triggerThreadList != null) {
			for (int i = 0; i < this.triggerThreadList.size(); i++) {
				final IScheduleTrigger sTrigger = (IScheduleTrigger) this.triggerThreadList.elementAt(i);
				sTrigger.cancel();
			}
			this.triggerThreadList.removeAllElements();
		}

		// clear techpack name cache.
		tpcache.clear();
	}

	/**
	 * Initialize rock engine
	 */
	private void initRock() throws SchedulerException {
		this.rockFact = null;

		while (this.rockFact == null) {
			try {
				log.info("Initaiting REPDB Connection for scheduler. ");
				this.rockFact = new RockFactory(this.url, this.userName,
						this.password, this.dbDriverName, "ETLSch", true);
				log.info("REPDB Connection for scheduler is successful. ");
			} catch (Exception e) {
				log.log(Level.SEVERE, "Database connection failed.. repdb may be offline..", e);
				try {
					Thread.sleep(this.reConnectTime);
				} catch (InterruptedException f) {
				}
			}
		}
	}

	private String getTechpackName(final Meta_schedulings schedule)
			throws SchedulerException {
		
		if (stopped){
			// Since stopped is called, RockFact connection will be close. Hence, don't trigger anything. 
			log.log(Level.FINEST,"Stopped is called off. Not able to get TechpackName from DB. ");
			return "";
		}
		
		if (tpcache.containsKey(schedule)) {
			return (String) tpcache.get(schedule);
		} else {
			try {
				// setup collection set
				final Meta_collection_sets whereCollection_sets = new Meta_collection_sets(this.rockFact);
				whereCollection_sets.setCollection_set_id(schedule.getCollection_set_id());
				final Meta_collection_setsFactory mcsF = new Meta_collection_setsFactory(
						this.rockFact, whereCollection_sets);
				final Meta_collection_sets mcs = mcsF.getElementAt(0);

				// create collection set
				final Meta_collection_sets collection_set = new Meta_collection_sets(schedule.getRockFactory(),
						schedule.getCollection_set_id(), mcs.getVersion_number());

				final String tp = collection_set.getCollection_set_name();
				tpcache.put(schedule, tp);
				return tp;
			} catch (SQLException s) {
				throw new SchedulerException(" SQLExceptions : ", s);
			} catch (RockException r) {
				throw new SchedulerException(" Rock Exception: ", r);
			}
		}
	}

	/**
	 * Creates triggers from etlrep.Meta_schedulings
	 * 
	 * @param currentTime
	 * @exception RemoteException
	 */
	private void makeThreads() throws SchedulerException {
		try {
			log.info("Creating triggers");

			this.initRock();

			this.triggerThreadList = new Vector<IScheduleTrigger>();

			tpcache.clear();

			ITransferEngineRMI engine = null;
			
			engine = (ITransferEngineRMI) Naming.lookup(RmiUrlFactory.getInstance().getEngineRmiUrl());

			// Wait until engine has been initialized.
			while (!engine.isInitialized() || (!engine.isCacheRefreshed())) {
				engine = (ITransferEngineRMI) Naming.lookup(RmiUrlFactory.getInstance().getEngineRmiUrl());
				this.log.fine("Waiting for the engine to initialize before starting scheduling.");
				Thread.sleep(1000);
			}
			
			log.info("Connected to ETLC engine");

			// get list of collection sets
			final Meta_collection_sets whereCollection_sets = new Meta_collection_sets(this.rockFact);
			final Meta_collection_setsFactory activeCollectionSets = new Meta_collection_setsFactory(
					this.rockFact, whereCollection_sets);

			// list IDs of the active TechPacks
			final List<Long> activeCollectionIDs = new ArrayList<Long>();

			for (int i = 0; i < activeCollectionSets.size(); i++) {
				final Meta_collection_sets cSet = activeCollectionSets.getElementAt(i);

				// if collection set is active add it to the list
				if (cSet.getEnabled_flag().equalsIgnoreCase("Y")) {
					activeCollectionIDs.add(cSet.getCollection_set_id());
				}
			}

			log.info(activeCollectionIDs.size() + " active techpacks found");

			// Retrieves only active rows from schedule
			final Meta_schedulings whereSchedule = new Meta_schedulings(rockFact);

			log.info("Enabled blocking on");
			rockFact.executeSql("set temporary option blocking='on';set temporary option blocking_timeout=60000;");
			String whereClause = "COLLECTION_SET_ID in (select COLLECTION_SET_ID from META_COLLECTION_SETS where ENABLED_FLAG= 'Y')";

			final Meta_schedulingsFactory schedules = new Meta_schedulingsFactory(
					rockFact, whereSchedule, whereClause, null);

			log.info(schedules.size() + " active schedules found");

			for (int i = 0; i < schedules.size(); i++) {
				final Meta_schedulings schedule = schedules.getElementAt(i);

				log.finer("Schedule \"" + schedule.getName() + "\"");

				// Create only scheduling that reference active techpack
				if (activeCollectionIDs.contains(schedule.getCollection_set_id())) {
					log.fine("Creating trigger for schedule " + schedule.getName());

					// Create scheduler trigger type
					final IScheduleTrigger sTrigger = createScheduleTrigger(schedule, engine);

					if (sTrigger != null) {
						triggerThreadList.addElement(sTrigger);
					}
				} else {
					log.fine("Schedule " + schedule.getName() + " references to not active TP");
				}
			}

			log.info("Trigger creation complete. " + triggerThreadList.size() + " triggers created.");
			// This is set to indicate that the makeThreads operation is
			// complete
			retryExecution = false;
		} catch (NotBoundException nbe) {
			throw new SchedulerException("ETLC Engine is not running", nbe);
		} catch (RemoteException re) {
			throw new SchedulerException("Registry cannot be contacted", re);
		} catch (Exception e) {
			retrycount++;
			log.info("Unexpected error while creating triggers " + e + "Retrying once.. ");
			if ((retryExecution) && (retrycount <= 1)) {
				makeThreads();
			} else {
				throw new SchedulerException("Unexpected error while creating triggers", e);
			}
		} finally {
			schedulerState = true;
		}
	}

	/**
	 * Triggers all the triggers that have correct trigger term
	 */
	private void startThreads() {

		if (triggerThreadList != null) {
			log.finer("Checking triggers. List size " + triggerThreadList.size());

			for (int i = 0; i < triggerThreadList.size(); i++) {
				final IScheduleTrigger sTrigger = (IScheduleTrigger) this.triggerThreadList.elementAt(i);

				log.finest("Trigger " + sTrigger.getName());

				final Meta_schedulings schedule = sTrigger.getSchedule();
				//final ITransferEngineRMI engine = sTrigger.getEngine();
				

				try {
					
					final String tpname = getTechpackName(schedule);

					// Is scheduled trigger on hold
					if (schedule.getHold_flag().equalsIgnoreCase("n")) {

						// is schedule in penaltyBox
						if (!penaltyBox.containsKey(sTrigger.getName())) {
							try {
								// Release the trigger, if execution terms are correct
								if (sTrigger.execute()) {
									log.finest("Trigger " + sTrigger.getName() + " released. Trying to execute set...");
									//final ITransferEngineRMI engine = (ITransferEngineRMI)Naming.lookup(RmiUrlFactory.getInstance().getEngineUrlByFreeSlots());
									//if (engine != null) {
										try {
											flag = true;
											triggerSet(sTrigger, schedule, null, "", tpname);
											
											if (stopped){
												return;
											}
										} catch (Exception e) {
											Logger.getLogger("tp." + tpname).warning("Schedule " + sTrigger.getName() + " set trigger failed");
											log.log(Level.WARNING, "Schedule " + sTrigger.getName() + " set execution failed", e);
											executionFailed(sTrigger, schedule);
										}
									//} else {
										//log.warning("Could not trigger " + sTrigger.getName() + " no engine found");
									//}
								}
							} catch (SchedulerException e) {
								log.log(Level.WARNING, "Trigger " + sTrigger.getName() + " error while evaluating", e);
								penaltyBox.put(sTrigger.getName(), new Integer(penaltyWait));
								log.fine("Trigger " + sTrigger.getName() + " sent to penalty box");
							}
						} else {
							int pValue = ((Integer) penaltyBox.get(sTrigger.getName())).intValue();
							if (pValue > 0) {
								// Scheduling penalty reduced
								penaltyBox.put(sTrigger.getName(), new Integer(--pValue));
								log.finest("Trigger " + sTrigger.getName()
										+ " in penalty, " + pValue + " cycles left");
							} else {
								// Scheduling penalty removed
								penaltyBox.remove(sTrigger.getName());
								log.finest("Trigger " + sTrigger.getName() + " released from penalty");
							}
						}
					} else {
						log.finest("Trigger " + sTrigger.getName() + " is on HOLD");
					}
				} catch (Exception e) {
					executionFailed(sTrigger, schedule);
					log.log(Level.WARNING, "Error while starting a thread", e);
				}
			}
		} 
	}

	/**
	 * Triggers a thread with given name.
	 * 
	 * @param triggerName
	 * @return boolean true if set was triggered
	 */
	boolean startThread(final String triggerName, final String command) {

		if (stopped) {
			// Since stopped is called, RockFact connection will be close. Hence, don't trigger anything. 
			log.log(Level.FINEST,"Stopped is called off. Set " + triggerName + " not triggered. ");
			return false;
		} else {
			log.fine("Triggering: " + triggerName);
		}
		

		if (this.triggerThreadList != null) {
			boolean found = false;

			for (int i = 0; i < this.triggerThreadList.size(); i++) {
				final IScheduleTrigger sTrigger = (IScheduleTrigger) this.triggerThreadList .elementAt(i);

				if (!sTrigger.getName().equalsIgnoreCase(triggerName)) {
					continue;
				}

				found = true;

				log.finest("Trigger " + triggerName + " found: \"" + sTrigger.getName() + "\"");

				final Meta_schedulings schedule = sTrigger.getSchedule();
				final ITransferEngineRMI engine = sTrigger.getEngine();

				String tpname = "N/A";

				try {
					tpname = getTechpackName(schedule);

					// Is scheduled trigger on hold
					if (schedule.getHold_flag().equalsIgnoreCase("n")) {
						if (engine != null) {
							try {
								flag = true;

								triggerSet(sTrigger, schedule, engine, command, tpname);
								return true;
							} catch (Exception e) {
								Logger.getLogger("tp." + tpname).log(
										Level.FINE, "Schedule " + triggerName + " set trigger failed");
								log.log(Level.WARNING, "Trigger " + sTrigger.getName() + " set execution failed", e);
								executionFailed(sTrigger, schedule);
							}
						} else {
							log.warning("Could not trigger set " + sTrigger.getName() + " no engine found.");
						}
					} else {
						log.finest("Trigger " + sTrigger.getName() + " is on HOLD");
						Logger.getLogger("tp." + tpname).info( "Schedule " + sTrigger.getName() + " is on HOLD");
					}
				} catch (Exception e) {
					executionFailed(sTrigger, schedule);
					log.log(Level.WARNING, "Error while starting a thread", e);
				}
			} // foreach trigger

			if (!found) {
				log.info("Trigger " + triggerName + " not found");
			}
		}

		return false;
	}
	
	private ITransferEngineRMI getEngine(String setType) throws SchedulerException {
		try {
			return (ITransferEngineRMI)Naming.lookup(RmiUrlFactory.getInstance().getEngineUrlByFreeSlots(setType));
		} catch (MalformedURLException | RemoteException | NotBoundException e) {
			log.log(Level.WARNING, "Not able to get the engine instance, Exception: ",e);
			throw new SchedulerException(e.getMessage());
		}
		
	}

	/**
	 * Triggers set
	 * 
	 * 
	 * @param sTrigger
	 * @param schedule
	 * @param engine
	 * @throws RMIConnectionException 
	 * @throws Exception
	 */
	private synchronized void triggerSet(final IScheduleTrigger sTrigger,
			final Meta_schedulings schedule, final ITransferEngineRMI engine,
			final String command, final String tpname)
			throws SchedulerException{

		// Some sanity checking...
		if (log == null) {
			log = Logger.getLogger("tp." + tpname);
			log.severe("SchedulerThread.triggerSet log was null. Created a new one.");
		}

		if (sTrigger == null) {
			this.log.severe("SchedulerThread.triggerSet variable sTrigger was null.");
		}

		if (schedule == null) {
			this.log.severe("SchedulerThread.triggerSet variable schedule was null.");
		}

		if (engine == null) {
			this.log.severe("SchedulerThread.triggerSet variable engine was null.");
		}

		if (command == null) {
			this.log.severe("SchedulerThread.triggerSet variable command was null.");
		}

		if (tpname == null || tpname == "" ) {
			this.log.severe("SchedulerThread.triggerSet variable tpname was null.");
		}
		
		if (stopped){
			// Since stopped is called, RockFact connection will be close. Hence, don't trigger anything. 
			log.log(Level.FINEST,"Stopped is called off. Set " + sTrigger.getName() + " will not be triggered. ");
			return;
		}
		
		try {
			// setup collection set
			final Meta_collection_sets whereCollection_sets = new Meta_collection_sets(this.rockFact);
			whereCollection_sets.setCollection_set_id(schedule.getCollection_set_id());
			final Meta_collection_setsFactory mcsF = new Meta_collection_setsFactory(this.rockFact, whereCollection_sets);

			if (mcsF.size() <= 0) {
				Logger.getLogger("tp." + tpname).warning("Schedule " + sTrigger.getName() + ": No such techpack");
				log.warning("Techpack not found");
				return;
			}

			final Meta_collection_sets mcs = mcsF.getElementAt(0);

			if (mcs == null) {
				this.log.severe("SchedulerThread.triggerSet variable mcs was null.");
				return;
			}

			// setup collection
			final Meta_collections whereCollection = new Meta_collections(this.rockFact);
			whereCollection.setCollection_id(schedule.getCollection_id());
			final Meta_collectionsFactory mcF = new Meta_collectionsFactory(this.rockFact, whereCollection);

			if (mcF.size() <= 0) {
				Logger.getLogger("tp." + tpname).warning("Schedule " + sTrigger.getName() + ": No such set");
				log.warning("Set not found");
				return;
			}

			final Meta_collections mc = mcF.getElementAt(0);
			
			// create collection set
			final Meta_collection_sets collection_set = new Meta_collection_sets(
					schedule.getRockFactory(), schedule.getCollection_set_id(),	mcs.getVersion_number());

			// create collection
			final Meta_collections collection = new Meta_collections(
					schedule.getRockFactory(), schedule.getCollection_id(),
					mc.getVersion_number(), schedule.getCollection_set_id());
			
			ITransferEngineRMI engineBasedonSet = getEngine(collection.getSettype());
			
			if (retryTriggerCount > 0) {
				try {
					boolean isInit = engineBasedonSet.isInitialized();
					boolean isRef = engineBasedonSet.isCacheRefreshed();
					if (!isInit || !isRef) {
						log.warning("Check to engine connect failed. ");
					}
				} catch (RemoteException ex) {
					log.warning("Still not working. rebind didn't worked. " +ex);
				}
			}
			
			// execute set in engine
			engineBasedonSet.execute(schedule.getRockFactory().getDbURL(), schedule.getRockFactory().getUserName(), 
					schedule.getRockFactory().getPassword(), schedule.getRockFactory().getDriverName(),
					collection_set.getCollection_set_name(), collection.getCollection_name(), command);
			
			Logger.getLogger("tp." + tpname).info("Schedule " + sTrigger.getName() + " triggered set "
							+ collection.getCollection_name());
			log.info("Trigger " + sTrigger.getName() + " triggered set " + collection.getCollection_name());
			
			// Set execution time
			schedule.setLast_execution_time(new Timestamp(sTrigger.getLastExecutionTime()));
			schedule.setLast_exec_time_ms(new Long(sTrigger.getLastExecutionTime()));

			// Set Status
			schedule.setStatus(Scheduler.STATUS_EXECUTED);

			// Save to DB
			schedule.updateDB();
		} catch (SQLException s) {
			throw new SchedulerException(" SQLExceptions : ", s);
		} catch (RockException r) {
			throw new SchedulerException(" Rock Exception: ", r);
		} catch (RemoteException e) {
			if (flag) {
				try {
					Scheduler sch = new Scheduler();
					if (!sch.init()){
						log.severe("RMI rebind failed... ");
					}
					
					retryTriggerCount++;
					if (retryTriggerCount <= 3) {
						log.info("Retrying to trigger set again for set : "
								+ sTrigger.getName() + " and retryTriggerCount : " + retryTriggerCount);
						Thread.sleep(60000);
						triggerSet(sTrigger, schedule, null, "", tpname);
					} else {
						log.warning("Max retried for 3 times. Still failed for set : " + sTrigger.getName());
						log.info("Restarting scheduler service,");
						String restartScript = "bash /eniq/sw/installer/ServiceRestart.bsh";
						RemoteExecutor.executeComandSshKey(APPLICATION_USER, "scheduler", restartScript);
					}
				} catch (Exception ex) {
					throw new SchedulerException("Failed to connect in RemoteException ", ex);
				}
			} else {
				throw new SchedulerException(" Failed to connect ", e);
			}
		} finally {
			flag = false;
		}
	}

	/**
	 * Creates objects from the trigger classes
	 * 
	 * @param schedule
	 * @param engine
	 * @return
	 * @throws Exception
	 */
	private IScheduleTrigger createScheduleTrigger(
			final Meta_schedulings schedule, final ITransferEngineRMI engine)
			throws SchedulerException {

		IScheduleTrigger ist = null;

		if (schedule.getExecution_type().equalsIgnoreCase("fileExists")) {
			ist = new FileExists();
		} else if (schedule.getExecution_type().equalsIgnoreCase("interval")
				|| schedule.getExecution_type().equalsIgnoreCase("intervall")) {
			ist = new Interval();
		} else if (schedule.getExecution_type().equalsIgnoreCase("timeDirCheck")) {
			ist = new IntervalDirCheck();
		} else if (schedule.getExecution_type().equalsIgnoreCase("once")) {
			ist = new Once();
		} else if (schedule.getExecution_type().equalsIgnoreCase("weekly")) {
			ist = new Weekly();
		} else if (schedule.getExecution_type().equalsIgnoreCase("weeklyinterval")) {
			ist = new WeeklyInterval();
		} else if (schedule.getExecution_type().equalsIgnoreCase("monthly")) {
			ist = new Monthly();
		} else if (schedule.getExecution_type().equalsIgnoreCase("wait")) {
			ist = new WaitToBeTriggered();
		} else if (schedule.getExecution_type().equalsIgnoreCase("onStartup")) {
			log.fine("Trigger type \"" + schedule.getName()	+ " is of type onStartup and handled by ETLC engine");
			return null;
		} else {
			log.warning("Unknown Trigger type: " + schedule.getExecution_type());
			return null;
		}

		ist.init(schedule, engine);
		log.fine("Trigger " + schedule.getName() + " created: "	+ getDesc(schedule));

		return ist;
	}

	/**
	 * Reports a failed execution into th db.
	 */
	private void executionFailed(final IScheduleTrigger sTrigger,
			final Meta_schedulings schedule) {
		try {
			schedule.setStatus(Scheduler.STATUS_FAILED);
			schedule.setLast_execution_time(new Timestamp(sTrigger.getLastExecutionTime()));
			schedule.setLast_exec_time_ms(new Long(sTrigger.getLastExecutionTime()));
			schedule.updateDB();
		} catch (Exception e) {
			log.log(Level.SEVERE, "Updating execution status failed", e);
		}
	}

	/**
	 * Returns description of specified Schedule
	 */
	private String getDesc(final Meta_schedulings sch) {

		final String typ = sch.getExecution_type();
		if (typ.equals("interval") || typ.equals("intervall")) {
			return "Occurs once every " + sch.getInterval_hour() + " hours " + sch.getInterval_min() + " minutes";
		} else if (typ.equals("timeDirCheck")) {
			final String checkIfEmpty = (sch.getTrigger_command() == null) ? "true"
					: sch.getTrigger_command().split(";")[0].trim();
			final String dirs = (sch.getTrigger_command() == null) ? "" : sch.getTrigger_command().split(";")[1].trim();
			return "Occurs once every " + sch.getInterval_hour() + " hours " + sch.getInterval_min() + " minutes"
					+ ", check for empty directories = " + checkIfEmpty + ", directories to check: " + dirs;
		} else if (typ.equals("wait")) {
		} else if (typ.equals("wait")) {
			return "Waiting trigger";
		} else if (typ.equals("fileExists")) {
			return "Waiting file " + sch.getTrigger_command();
		} else if (typ.equals("weekly")) {
			final StringBuffer sb = new StringBuffer("");
			if ("Y".equals(sch.getMon_flag())) {
				sb.append("Mon");
			}
			if ("Y".equals(sch.getTue_flag())) {
				if (sb.length() > 0) {
					sb.append(",");
				}
				sb.append("Tue");
			}
			if ("Y".equals(sch.getWed_flag())) {
				if (sb.length() > 0) {
					sb.append(",");
				}
				sb.append("Wed");
			}
			if ("Y".equals(sch.getThu_flag())) {
				if (sb.length() > 0) {
					sb.append(",");
				}
				sb.append("Thu");
			}
			if ("Y".equals(sch.getFri_flag())) {
				if (sb.length() > 0) {
					sb.append(",");
				}
				sb.append("Fri");
			}
			if ("Y".equals(sch.getSat_flag())) {
				if (sb.length() > 0) {
					sb.append(",");
				}
				sb.append("Sat");
			}
			if ("Y".equals(sch.getSun_flag())) {
				if (sb.length() > 0) {
					sb.append(",");
				}
				sb.append("Sun");
			}
			sb.insert(0, "Every ");
			sb.append(" at ");
			sb.append(sch.getScheduling_hour()).append(":").append(sch.getScheduling_min());

			return sb.toString();
		} else if (typ.equals("monthly")) {
			final int day = sch.getScheduling_day().intValue();
			if (day <= 0) {
				return "Occures last day of month at " + sch.getScheduling_hour() + ":"
						+ sch.getScheduling_min();
			} else {
				return "Occures " + sch.getScheduling_day()
						+ ". day of month at " + sch.getScheduling_hour() + ":" + sch.getScheduling_min();
			}
		}

		return "";
	}
	
	
}
