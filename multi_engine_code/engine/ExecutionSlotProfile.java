package com.distocraft.dc5000.etl.engine.executionslots;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.distocraft.dc5000.etl.engine.common.Share;
import com.distocraft.dc5000.etl.engine.main.EngineThread;

/**
 * @author savinen
 */
public class ExecutionSlotProfile {

	private String name;
	private String profileId;

	private final List<ExecutionSlot> executionSlotList = new ArrayList<ExecutionSlot>();

	private boolean active = false;

	private final Logger log;

	private final Map<String,Integer> maxConcurrentWorkers;
	private final Map<String,String> regexpsForWorkerLimitations;
	private int maxMemoryUsageMB = 0;

	public ExecutionSlotProfile(final String name, final String profileId) {
		this.name = name;
		this.profileId = profileId;

		this.log = Logger.getLogger("etlengine.SlotProfile." + name);

		final Share share = Share.instance();
		maxConcurrentWorkers = (Map<String,Integer>) share.get("max_concurrent_workers");
		regexpsForWorkerLimitations = (Map<String,String>) share.get("regexps_for_worker_limitations");
		if (share.contains("execution_profile_max_memory_usage_mb")
				&& share.get("execution_profile_max_memory_usage_mb") != null) {
			maxMemoryUsageMB = (Integer) share.get("execution_profile_max_memory_usage_mb");
		} else {
			maxMemoryUsageMB = 0;
		}
	}

	/**
	 * Adds an execution slot to this profile.
	 */
	public void addExecutionSlot(final ExecutionSlot exSlot) {
		synchronized (executionSlotList) {
			if(this.name.indexOf("DefaultProfile")<0 || exSlot.toString().indexOf("Default0")>0){
				log.fine("Profile Name :: " + this.name + " Profile ID :: " + this.profileId + " Adding slot :: " + exSlot + " Slot Type :: " + exSlot.getSlotType());
				executionSlotList.add(exSlot);
			}
		}
	}

	/**
	 * Remove specified execution slot from this profile
	 */
	public void removeExecutionSlot(final ExecutionSlot exSlot) {
		log.fine("Removing slot " + exSlot);
		synchronized (executionSlotList) {
			executionSlotList.remove(exSlot);
		}
	}

	/**
	 * Searches a running set with given set name (collection_id) and set id
	 * (collection_id) from active profiles slots. if found: set (EngineThread) is
	 * returned else null is returned
	 * 
	 * @param setName
	 * @param setID
	 * @return EngineThread, or null
	 */
	public EngineThread getRunningSet(final String setName, final long setID) {

		EngineThread ret = null;

		synchronized (executionSlotList) {

			for (ExecutionSlot exSlot : executionSlotList) {
				final EngineThread rSet = exSlot.getRunningSet();

				if (rSet != null && rSet.getSetID().longValue() == setID && rSet.getSetName().equals(setName)) {
					ret = rSet;
					break;
				}
			}

		}

		return ret;
	}

	/**
	 * Returns execution slot from this profile specified by order number
	 */
	public ExecutionSlot getExecutionSlot(final int nro) {
		return executionSlotList.get(nro);
	}

	/**
	 * Returns all execution slots from this profile
	 */
	public Iterator<ExecutionSlot> getAllExecutionSlots() {
		final List<ExecutionSlot> ret = new ArrayList<ExecutionSlot>();
		synchronized (executionSlotList) {
			for (ExecutionSlot slot : executionSlotList) {
				ret.add(slot);
			}
		}

		return ret.iterator();
	}

	/**
	 * Returns all free (not running) execution slots from this profile
	 */
	public Iterator<ExecutionSlot> getAllFreeExecutionSlots() {
		final List<ExecutionSlot> tmp = new ArrayList<ExecutionSlot>();
		synchronized (executionSlotList) {
			for (ExecutionSlot slot : executionSlotList) {
				if (slot.isFree()) {
					tmp.add(slot);
				}
			}
		}

		return tmp.iterator();
	}
	
	/**
	 * Returns the number of free slots that can run given set.
	 * 
	 * @param setType - type of the set
	 * @return
	 */
	public synchronized int getNumberOfFreeSlotsforSetType(String setType) {
		log.log(Level.FINEST, "Execution slot list " + executionSlotList);
		return executionSlotList.stream()
		.filter(exSlot -> exSlot.isFree() && exSlot.getApprovedSettypes().contains(setType.toLowerCase()))
		.collect(Collectors.toList()).size();
	}

	/**
	 * Returns first free (not running) execution slots from this profile. Returns
	 * null if no free slots available.
	 */
	public ExecutionSlot getFirstFreeExecutionSlots() {
		ExecutionSlot ret = null;

		synchronized (executionSlotList) {
			for (ExecutionSlot slot : executionSlotList) {
				if (slot.isFree()) {
					ret = slot;
					break;
				}
			}
		}

		return ret;
	}

	/**
	 * Returns number of execution slots in this profile
	 */
	public int getNumberOfExecutionSlots() {
		return executionSlotList.size();
	}

	/**
	 * Returns number of free (not running) execution slots in this profile
	 */
	public int getNumberOfFreeExecutionSlots() {
		int count = 0;

		synchronized (executionSlotList) {
			for (ExecutionSlot slot : executionSlotList) {
				if (slot.isFree()) {
					count++;
				}
			}
		}

		return count;

	}

	/**
	 * Returns all running execution slots from this profile
	 */
	public Iterator<ExecutionSlot> getAllRunningExecutionSlots() {

		final List<ExecutionSlot> tmp = new ArrayList<ExecutionSlot>();

		synchronized (executionSlotList) {
			for (ExecutionSlot slot : executionSlotList) {
				if (!slot.isFree()) {
					tmp.add(slot);
				}
			}
		}

		return tmp.iterator();

	}

	/**
	 * Returns all set types of all running execution slots.
	 */
	public Set<String> getAllRunningExecutionSlotSetTypes() {

		final Set<String> tmp = new HashSet<String>();

		synchronized (executionSlotList) {
			for (ExecutionSlot slot : executionSlotList) {
				if (!slot.isFree()) {
					tmp.add(slot.getRunningSet().getSetType());
				}
			}
		}

		return tmp;
	}

	/**
	 * Returns all worker objects which are currently running
	 */
	public Set<Object> getAllRunningExecutionSlotWorkers() {
		final Set<Object> tmp = new HashSet<Object>();

		synchronized (executionSlotList) {
			for (ExecutionSlot slot : executionSlotList) {
				if (!slot.isFree() && slot.getRunningSet().getWorkerObject() != null) {
					tmp.add(slot.getRunningSet().getWorkerObject());
				}
			}
		}

		return tmp;
	}

	/**
	 * Return true if specified set is not currently executed by any of execution
	 * slots.
	 */
	public boolean notInExecution(final EngineThread set) {
		boolean ret = true;

		synchronized (executionSlotList) {
			for (ExecutionSlot slot : executionSlotList) {
				final EngineThread runningThread = slot.getRunningSet();
				if (runningThread != null && runningThread.getSetName().equals(set.getSetName())
						&& (runningThread.getSetID().equals(set.getSetID()))) {
					ret = false;
				}
			}
		}

		return ret;

	}

	/**
	 * Returns true if one of the given sets tables are already in execution. if
	 * given sets table list is empty, false is always returned.
	 */
	public boolean checkTable(final EngineThread set) {

		boolean ret = false;

		if (set.getSetTables().size() > 0) {

			synchronized (executionSlotList) {
				for (ExecutionSlot slot : executionSlotList) {
					final EngineThread runningThread = slot.getRunningSet();

					if (runningThread != null) {
						for (String iTables : runningThread.getSetTables()) {
							if (set.getSetTables().contains(iTables)) {
								ret = true;
							}
						}

					}
				}
			}

		}

		return ret;

	}

	/**
	 * Returns true if all slots are locked or free
	 */
	public boolean areAllSlotsLockedOrFree() {
		boolean ret = true;

		synchronized (executionSlotList) {
			for (ExecutionSlot slot : executionSlotList) {
				if (!slot.islocked() && !slot.isFree()) {
					ret = false;
					break;
				}
			}
		}

		return ret;
	}

	/**
	 * Cleans profile from slots marked for removal (isRemovedAfterExecution)
	 * after execution has ended (isFree).
	 */
	public void cleanProfile() {
		synchronized (executionSlotList) {
			final Iterator<ExecutionSlot> iter = executionSlotList.iterator();
			while (iter.hasNext()) {
				final ExecutionSlot slot = iter.next();
				if (slot.isRemovedAfterExecution() && slot.isFree()) {
					log.fine("Removing execution slot from active profile");
					iter.remove();
				}
			}
		}
	}

	/**
	 * Checks that the profile does not contain any slots marked for removal
	 * (isRemovedAfterExecution).
	 */
	public boolean isProfileClean() {
		boolean ret = true;

		synchronized (executionSlotList) {
			for (ExecutionSlot slot : executionSlotList) {
				if (slot.isRemovedAfterExecution()) {
					ret = false;
				}
			}
		}
		return ret;
	}

	/**
	 * 
	 * Checks that does the maximum amount of concurrent workers exceed. Returns
	 * true if exceeds.
	 */
	public boolean hasMaxConcurrentWorkersExceeded(final EngineThread set) {
		boolean hasExceeded = false;
		boolean isConfigured = true;
		final WorkerLimits workerLimits = getWorkerLimitsBySetName(set);
		int amountOfConcurrentWorkersToBeLimited = 0;

		if (maxConcurrentWorkers.keySet().isEmpty()) {
			isConfigured = false;
			log.fine("No configuration for maxConcurrentWorkers.");
		}

		if (regexpsForWorkerLimitations.keySet().isEmpty()) {
			isConfigured = false;
			log.fine("No configuration for regexpsForWorkerLimitations.");
		}

		if (!executionSlotList.isEmpty() && null != workerLimits && isConfigured) {
			log.fine("Starting to limit workers of set: " + set.getSetName());

			synchronized (executionSlotList) {

				for (ExecutionSlot slot : executionSlotList) {
					final EngineThread runningSet = slot.getRunningSet();

					if (runningSet != null) {
						// does the set name match with regexp
						if (setNameMatchesWithRegexp(runningSet.getSetName(), workerLimits.regexp)) {
							amountOfConcurrentWorkersToBeLimited++;
						}
					}
				}

			} // synchronized
				
			hasExceeded = workerLimits.maximumAmount <= amountOfConcurrentWorkersToBeLimited;
			log.fine("Set: " + set.getSetName() + " hasExceeded=" + hasExceeded);

		} else {
			log.fine("No configuration to limit number of concurrent workers.");
		}

		return hasExceeded;
	}

	/**
	 * 
	 * Gets worker limitations by set name (if the set name matches configured
	 * regexp). With found regexp's key the maximum amount of workers are got
	 * also. Returns null if there's no regexp or amount configuration to limit
	 * the set.
	 */
	private WorkerLimits getWorkerLimitsBySetName(final EngineThread set) {
		String regexp = null;
		String key = null;
		Integer maximumAmountOfWorkers = null;
		boolean regexpFound = false;
		
		final Iterator<String> iter = regexpsForWorkerLimitations.keySet().iterator();

		while (iter.hasNext() && !regexpFound) {
			key = iter.next();
			regexp = regexpsForWorkerLimitations.get(key);
			regexpFound = setNameMatchesWithRegexp(set.getSetName(), regexp);
		}

		WorkerLimits workerLimits = null;

		if (regexpFound) {
			try {
				maximumAmountOfWorkers = maxConcurrentWorkers.get(key);
				workerLimits = new WorkerLimits(maximumAmountOfWorkers, regexp);
				log.fine("Using regexp: " + regexp + " to limit max amount of concurrent workers: " + maximumAmountOfWorkers
						+ ". Possible limitation for set:" + set.getSetName());
			} catch (Exception e) {
				log.log(Level.WARNING, "No maximum amount of workers calculated for regexp configuration. Key: " + key
						+ " regexp: " + regexp, e);
			}
		} else {
			log.fine("No limitation configuration found for set: " + set.getSetName());
		}

		return workerLimits;
	}

	/**
	 * 
	 * Checks that does the set name match with given regular expression.
	 * 
	 */
	private boolean setNameMatchesWithRegexp(final String setName, final String regexp) {
		boolean returnValue = false;
		try {
			final Pattern pat = Pattern.compile(regexp);
			final Matcher mat = pat.matcher(setName);
			returnValue = mat.find();
		} catch (Exception e) {
			log.log(Level.WARNING, "Set limitation regexp matching failed.", e);
		}
		return returnValue;
	}

	private class WorkerLimits {
		public Integer maximumAmount = null;
		public String regexp = null;

		public WorkerLimits(final Integer maximumAmount, final String regexp) {
			this.maximumAmount = maximumAmount;
			this.regexp = regexp;
		}
	}

	/**
	 * Checks that has the maximum memory usage exceeded. Returns true if exceeds.
	 */
	public boolean hasMaxMemoryUsageExceeded(final EngineThread set) {

		boolean hasExceeded;

		final int setsMemoryNeed = set.getMemoryConsumptionMB();

		if (!executionSlotList.isEmpty() && 0 < setsMemoryNeed) {

			log.finest("Starting to calculate total memory usage in execution slots for set: " + set.getSetName());

			final ExecutionMemoryConsumption emc = ExecutionMemoryConsumption.instance();

			final int totalMemUsageMB = emc.calculate();

			hasExceeded = (totalMemUsageMB + setsMemoryNeed) >= maxMemoryUsageMB;
			
			if ( setsMemoryNeed > (Runtime.getRuntime().freeMemory() * .4 / 1024 / 1024 ))
			{
				hasExceeded = true ;
				
			}

			log.finest("To run set: " + set.getSetName() + " maxMemoryUsageHasExceeded=" + hasExceeded);

		} else {
			hasExceeded = false;
		}

		return hasExceeded;
	}

	// Setters and getters

	public String name() {
		return this.name;
	}

	public String ID() {
		return this.profileId;
	}

	public void setID(final String profileId) {
		this.profileId = profileId;
	}

	public void setName(final String name) {
		this.name = name;
	}

	public void activate() {
		this.active = true;
	}

	public void deactivate() {
		this.active = false;
	}

	public boolean IsActivate() {
		return this.active;
	}

}
