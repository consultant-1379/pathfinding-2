package com.distocraft.dc5000.etl.engine.executionslots;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.logging.Logger;

import com.distocraft.dc5000.etl.engine.main.EngineThread;

/**
 * @author savinen
 * @author etuolem
 */
public final class ExecutionSlot {

  private static final String DEFAULT_SLOT = "dwh";

  private final Logger log; // NOPMD
	
  private transient Set<String> approvedSetTypes = new HashSet<String> ();
	
  private String name;
  private int slotId;
  private String slotType;

  private boolean locked = false;
  private boolean hold = false;
  private boolean removeAfterExecution = false;
  public boolean worker = false;
  
  private EngineThread runningSet = null;  
  
  // Variables for running statistics printing
  private long idleTime = 0L;
  private long busyTime = 0L;
  private long periodStart = System.currentTimeMillis();

  public ExecutionSlot(final int slotId, final String slotName) {
    this.name = slotName;
    this.slotId = slotId;
    this.slotType = DEFAULT_SLOT; 
    
    log = Logger.getLogger("etlengine.Slot." + name);
  }

  /**
   * Constructor defining comma separated list of approved set types.
   */
  public ExecutionSlot(final int slotId, final String slotName, final String settypes, final String serviceNode) {
    this(slotId, slotName);
    
    setApprovedSettypes(settypes);
    
    if(serviceNode != null && !serviceNode.isEmpty()){
    	this.slotType = serviceNode.toLowerCase();
    }else{
    	this.slotType = DEFAULT_SLOT;
    }
  }

  public ExecutionSlot(final int slotId, final String name, final List<String> settypes, final String serviceNode) {
    this(slotId,name);

    setApprovedSettypes(settypes);
    
    if(serviceNode != null && !serviceNode.isEmpty()){
    	this.slotType = serviceNode.toLowerCase();
    }else{
    	this.slotType = DEFAULT_SLOT;
    }
  }

  /**
   * 
   * returns true if set is accepted in this slot.
   * 
   * checks if the thread exsists checks the type of the set checks for
   * doublicates in executions slots
   * 
   * @param exo The thread to check
   * @return <code>TRUE</code> is the set can be accepted into the slot, <code>FALSE</code> otherwise
   */
  public boolean isAccepted(final EngineThread exo) {
  	
  	boolean ret = false;
  	
  	if (exo != null) {
  		
  		final String settype = (exo.getSetType() == null) ? "" : exo.getSetType().trim().toLowerCase();
  		
  		if (exo.isShutdownSet()) {
  			if(settype.equalsIgnoreCase(this.name)) {
  				ret = true;
  			}
  		} else {
  			if (approvedSetTypes.contains("all")) {
  				ret = true;
  			} else if (approvedSetTypes.contains(settype)) {
  				ret = true;
  			}
  		}
  	}

  	return ret;
  	
  }

  /**
   * Method checks if slot is free or if slots set has ended then it is set
   * free. return true if this slot is free. False is returned is set is on
   * hold.
   */
  public boolean isFree() {
    return !this.hold && (this.runningSet == null || !this.runningSet.isAlive());
  }

  /**
   * Executes a set in this execution slot if this slot is free.
   */
  public void execute(final EngineThread set) {

    if (this.isFree() && !this.locked) {

      if (set.isShutdownSet()) {
        this.locked = true;
        log.fine("Locking execution slot");
      } else {         
	        if (set.isWorker()){
		        if (Runtime.getRuntime().freeMemory() > (Runtime.getRuntime().totalMemory() * .2)) {
	        	this.worker = true;
	        }
    	 }
        printRunningStats();
        
        log.finer("Executing set \"" + set.getSetName() + "\"");
        set.start();
        
        this.runningSet = set;

      }
    } else {
      if (this.locked) {
        log.finer("Slot is locked");
      } else {
        log.finer("Slot is busy");
      }
    }

  }

  private void printRunningStats() {
  	final long now = System.currentTimeMillis();
  	
  	if (this.runningSet == null) {
  		this.idleTime += (now - this.periodStart);
  	} else {
    	this.busyTime += (this.runningSet.getExecEndTime() - this.runningSet.getExecStartTime());
    	this.idleTime += (now - this.runningSet.getExecEndTime());
    }
  	
  	final long period = now - this.periodStart;
  	if(period > 3600000L) {
  		log.fine(toString() + " busy " + busyTime + "/" + period);
  		log.fine(toString() + " idle " + idleTime + "/" + period);
  		
  		this.busyTime = 0L;
  		this.idleTime = 0L;
  		
  		this.periodStart = now;
  	}
  	
  }
  
  public void removeAfterExecution(final boolean flag) {
    this.removeAfterExecution = flag;
  }

  public boolean isRemovedAfterExecution() {
    return this.removeAfterExecution;
  }

  /**
   * Returns reference to EngineThread this Execution slot is executing.
   * If this executionSlot is idle null is returned.
   */
  public EngineThread getRunningSet() {
  	EngineThread ret = null;
  	
  	if(runningSet != null && runningSet.isAlive()) {
  		ret = runningSet;
  	}
  		
    return ret;
  }
    
  /**
   * For unit tests.
   * @param set 
   */
  public void setRunningSet(final EngineThread set) {
    this.runningSet = set;
  }  

  public void hold() {
    log.info("Slot holded");
    this.hold = true;
  }

  public void restart() {
    log.info("Slot restarted");
    this.hold = false;
  }

  public boolean isOnHold() {
    return this.hold;
  }

  public boolean islocked() {
    return this.locked;
  }

  public String getName() {
    return this.name;
  }

  public void setSlotType(final String slotType) {
    this.slotType = slotType;
  }

  public void setName(final String name) {
    this.name = name;
  }

  public void setSlotId(final int id){
    this.slotId = id;
  }

  public List<String> getApprovedSettypes() {
  	final List<String> setTypes = new ArrayList<String> ();
  	
  	for(String type : approvedSetTypes) {
  		setTypes.add(type);
  	}
  		
    return setTypes;
  }

  public int getSlotId(){
    return this.slotId;
  }
   
  public String getSlotType() {
    return slotType;
  }
	  
  /**
   * Set approved set types for this ExecutionSlot.
   */
  public void setApprovedSettypes(final List<String> vec) {
  	final Set<String> setTypes = new HashSet<String> ();
  	
  	for(String type : vec) {
  		setTypes.add(type.toLowerCase(Locale.getDefault()));
  	}
  	
  	this.approvedSetTypes = setTypes;

  }

  /**
   * Set approved set types for this ExecutionSlot.
   */
  public void setApprovedSettypes(final String settypes) {

  	final Set<String> setTypes = new HashSet<String> ();
  	
  	if(settypes != null && settypes.length() > 0) {
  		final String[] arr = settypes.split(",");
  		
  		for(String type : arr) {
  			if(arr.length > 0) {
  				setTypes.add(type.toLowerCase(Locale.getDefault()));
  			}
  		}
  		
  	}
  	
  	this.approvedSetTypes = setTypes;
  	
  }

  @Override
  public String toString() {
  	final StringBuilder builder = new StringBuilder();
  	builder.append("ExecutionSlot ").append(name).append(" (");
  	for(final Iterator<String> i = approvedSetTypes.iterator() ; i.hasNext() ; ) {
  		builder.append(i.next());
  		if(i.hasNext()) {
  			builder.append(',');
  		}
  	}
  	builder.append(')');

  	return builder.toString();
  }

  /**
   * Returns database name of this slot. Mainly future reservation for multiple
   * writer implementation.
   */
  public String getDBName() {
    return slotType;
  }

}
