package com.distocraft.dc5000.etl.engine.main;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.distocraft.dc5000.etl.engine.system.SetStatusTO;

public interface ITransferEngineRMI extends Remote {

  /**
   * Executes the initialized collection set / coleection
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
  void execute(String url, String userName, String password, String dbDriverName, String collectionSetName,
      String collectionName) throws RemoteException;

  /**
   * Executes the initialized collection set / coleection
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
   * @param SchedulerInfo
   *          Information from the scheduler
   * @exception RemoteException
   */
  void execute(String url, String userName, String password, String dbDriverName, String collectionSetName,
      String collectionName, String ScheduleInfo) throws RemoteException;

  /**
   * Executes the initialized collection set / collection
   * 
   * @param collectionSetName
   *          the name of the transfer collection set
   * @param collectionName
   *          the name of the transfer collection
   * @param SchedulerInfo
   *          Information from the scheduler
   * @exception RemoteException
   */
  void execute(String collectionSetName, String collectionName, String ScheduleInfo) throws RemoteException;

  /**
   * 
   * @param collectionSetName
   * @param collectionName
   * @param ScheduleInfo
   * @return
   * @throws RemoteException
   */
  String executeAndWait(String collectionSetName, String collectionName, String ScheduleInfo)
      throws RemoteException;
  
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
  String executeWithSetListener(String collectionSetName, String collectionName, String ScheduleInfo) 
    throws RemoteException;
  
  
  /**
   * Executes the initialized collection set / collection, and creates a 
   * listener object to observe the execution. 
   * 
   * @param collectionSetName
   * @param collectionName
   * @param ScheduleInfo
   * @param properties
   * @return SetStatusTO
   * @throws RemoteException
   */
  SetStatusTO executeSetViaSetManager(String collectionSetName,
      String collectionName, String ScheduleInfo, java.util.Properties props)
      throws RemoteException;
  
  SetStatusTO getSetStatusViaSetManager(String collectionSetName, String collectionName, int beginIndex, int count)
    throws RemoteException;
  
  
  /**
   * Retrieves the status information and events from a set listener object. The
   * listener is identified by the statusListenerId parameter. 
   * 
   * @param statusListenerId Status listener's id, returned by the 
   * 						 executeWithSetListener method.
   * @param beginIndex		 The index of the first retrieved status event
   * @param count			 The number of status events to be retrieved
   * @return				 A set status transfer object, containing the 
   * 						 observed state and the status events. 
   * @throws RemoteException
   */
  SetStatusTO getStatusEventsWithId(String statusListenerId, int beginIndex, int count) 
  throws RemoteException;
  
  /**
   * Writes the SQL Loader ctl file contents
   * 
   * @param String
   *          fileContents The ctl -file description.
   * @param String
   *          fileName The ctl file name.
   * @exception RemoteException
   */
  void writeSQLLoadFile(String fileContents, String fileName) throws RemoteException;

  /**
   * Returns all available plugin names
   * 
   * @return String[] plugin names
   * @throws RemoteException
   */
  String[] getPluginNames() throws RemoteException;

  /**
   * Returns the specified plugins methods
   * 
   * @param String
   *          pluginName the plugin that the methods are fetched from
   * @param boolean
   *          isGetSetMethods if true, only set method names are returned
   * @param boolean
   *          isGetGetMethods if true, only get method names are returned
   * @return String[] method names
   * @throws RemoteException
   */
  String[] getPluginMethods(String pluginName, boolean isGetSetMethods, boolean isGetGetMethods)
      throws RemoteException;

  /**
   * Returns the constructor parameters separated with ,
   * 
   * @param String
   *          pluginName The plugin to load
   * @return String
   */
  String getPluginConstructorParameters(String pluginName) throws RemoteException;

  // SS
  /**
   * Returns the constructor parameter info
   * 
   * @param String
   *          pluginName The plugin to load
   * @return String
   */
  String getPluginConstructorParameterInfo(String pluginName) throws RemoteException;

  /**
   * Returns the method parameters separated with ,
   * 
   * @param String
   *          pluginName The plugin to load
   * @param String
   *          methodName The method that hold the parameters
   * @return String
   */
  String getPluginMethodParameters(String pluginName, String methodName) throws RemoteException;
  
  
  /**
   * Method to query Cache status
   */

 boolean isCacheRefreshed() throws RemoteException;
  
  /**
   * Method to query engine status
   */
  List<String> status() throws RemoteException;

  /**
   * Method to query currentProfile
   */
  String currentProfile() throws RemoteException;
  
  /**
   * Method to shutdown engine
   */
  void fastGracefulShutdown() throws RemoteException;

  /**
   * Method to shutdown engine
   */
  void slowGracefulShutdown() throws RemoteException;    
  
  /**   * Method to Pause engine   */ 
  void slowGracefulPauseEngine() throws RemoteException;
  
  /**   * Method to Pause engine   */
  boolean init() throws RemoteException;

  /**
   * Method to shutdown engine
   */
  void forceShutdown() throws RemoteException;

  /**
   * Method to clear the Counting Management Cache, for a given storageId
   */
  void clearCountingManagementCache(String storageId) throws RemoteException;

  /**
   * Method to change active profile
   */
  boolean setActiveExecutionProfile(String profileName, final boolean resetConMon) throws RemoteException;
  boolean setActiveExecutionProfile(String profileName) throws RemoteException;
  
  /**
   * Method to change active profile with message text
   */
  boolean setActiveExecutionProfile(String profileName, String messageText) throws RemoteException;

  /**
   * Method to change active profile
   */
  boolean setAndWaitActiveExecutionProfile(String profileName) throws RemoteException;

  /**
   * @throws RemoteException
   */
  void reloadExecutionProfiles() throws RemoteException;

  /**
   * Hold a Execution slot.
   */

  void holdExecutionSlot(int ExecutionSlotNumber) throws RemoteException;

  /**
   * Restart a Execution slot.
   */
  void restartExecutionSlot(int ExecutionSlotNumber) throws RemoteException;

  boolean removeSetFromPriorityQueue(Long ID) throws RemoteException;

  boolean changeSetPriorityInPriorityQueue(Long ID, long priority) throws RemoteException;

  void holdPriorityQueue() throws RemoteException;

  void restartPriorityQueue() throws RemoteException;

  void refreshCache() throws RemoteException;
  
  void reloadProperties() throws RemoteException;
  
  void reloadAggregationCache() throws RemoteException;
  
  void reloadAlarmConfigCache() throws RemoteException;
  
  void reloadBackupConfigCache() throws RemoteException;
  
  void triggerRestoreOfData() throws RemoteException;
  
  List<String> loggingStatus() throws RemoteException;

  boolean isSetRunning(Long techpackID, Long setID) throws RemoteException;

  void activateSetInPriorityQueue(Long ID) throws RemoteException;

  void holdSetInPriorityQueue(Long ID) throws RemoteException;
  
  boolean changeSetTimeLimitInPriorityQueue(final Long queueId, final long queueTimeLimit) throws RemoteException;
  
  /**
   * Returns the failed sets in the ETL engine.
   * 
   * @return
   */
  List<Map<String, String>> getFailedSets() throws java.rmi.RemoteException;

  /**
   * Returns the queued sets in the ETL engine.
   * 
   * @return
   */
  List<Map<String, String>> getQueuedSets() throws java.rmi.RemoteException;

  /**
   * Returns the executed sets in the ETL engine.
   * 
   * @return
   */
  List<Map<String, String>> getExecutedSets() throws java.rmi.RemoteException;

  /**
   * Returns the running sets in the ETL engine.
   * 
   * @return
   */
  List<Map<String, String>> getRunningSets() throws java.rmi.RemoteException;

  List<Map<String, String>> getRunningSets(List<String> techPackNames) throws java.rmi.RemoteException;

  void reaggregate(String aggregation, long datadate) throws RemoteException;

  void changeAggregationStatus(String status, String aggregation, long datadate) throws RemoteException;

  Set<String> getAllActiveSetTypesInExecutionProfiles() throws RemoteException;

  void lockExecutionprofile() throws RemoteException;

  void unLockExecutionprofile() throws RemoteException;

  void activateScheduler() throws RemoteException;

  Set<Object> getAllRunningExecutionSlotWorkers() throws RemoteException;

  void addWorkerToQueue(String name, String type, Object wobj) throws RemoteException;

  void reloadDBLookups(String tableName) throws RemoteException;
  
  boolean isTechPackEnabled(final String techPackName, String techPackType) throws RemoteException;
  
  List <String> getMeasurementTypesForRestore(final String techPackName,final String regex) throws RemoteException;
  
  void restore(final String techPackName,final List<String> measureMentTypes,final String fromRestoreDate,final String toRestoreDate) throws RemoteException;
  
  void reloadTransformations() throws RemoteException;
  
  void updateTransformation(String tpName) throws RemoteException;
  
  boolean isInitialized()throws RemoteException;
  
  void reloadLogging() throws RemoteException;

  void giveEngineCommand(String com)throws RemoteException;

  List<Map<String, String>> slotInfo() throws RemoteException;
  
  void disableTechpack (String techpackName)throws RemoteException;
  
  void disableSet(String techpackName, String setName) throws RemoteException;
  
  void disableAction (String techpackName, String setName, Integer actionOrder) throws RemoteException;
  
  void enableTechpack (String techpackName) throws RemoteException;
  
  void enableSet (String techpackName, String setName) throws RemoteException;
 
  void enableAction(String techpackName, String setName, Integer actionNumber) throws RemoteException;
  
  List<String> showDisabledSets() throws RemoteException;
  
  List<String> showActiveInterfaces() throws RemoteException;
  
  /**
   * Release defined pending set
   */
  void releaseSet(final long queueId) throws RemoteException;
  
  /**
   * Release all pending sets of defined techpack for defined set type
   */
  void releaseSets(final String techpackName, final String setType) throws RemoteException;

  /**
   * Returns a list of RAW Event tables that are ACTIVE for a specified viewName in the specified time range. This will
   * not return RAW Event tables if the startTime and endTime are equal to zero.
   */
  List<String> getTableNamesForRawEvents(final String viewName, final Timestamp startTime,
      final Timestamp endTime) throws RemoteException;

  /**
   * Returns the latest RAW Event tables that are ACTIVE for a specified
   * viewName
   */
  List<String> getLatestTableNamesForRawEvents(final String viewName) throws java.rmi.RemoteException;

  /**
   * Executes the Manual Counting ReAggregation
   */
  void manualCountReAgg(final String techPackName, final Timestamp minTimestamp, final Timestamp maxTimestamp,
      final String intervalName, final boolean isScheduled) throws RemoteException;

  /**
   * Checks if intervalName is supported for the Manual Counting ReAggregation
   */
  boolean isIntervalNameSupported(final String intervalName) throws RemoteException;
  
  /**
   * Gets the oldest time that a Manual Counting ReAggregation can be be
   * executed for
   */
  long getOldestReAggTimeInMs(final String techPackName) throws RemoteException;

  /**
   * Get the status of DbConnectionMonitor
   */
  Map<String, String> serviceNodeConnectionInfo() throws RemoteException;

  /**
   * Hold sets for tech packs in the priority queue.
   * @param techPackNames
   */
  void removeTechPacksInPriorityQueue(List<String> techPackNames) throws RemoteException;
  
  /**
   * Kill sets that are running for a list of tech packs.
   * @param techPackNames
   * @throws RemoteException
   */
  void killRunningSets(List<String> techPackNames) throws RemoteException;

  /**
   * Lock Events UI users in table ENIQ_EVENTS_ADMIN_PROPERTIES.
   * @param lock  True if the Events UI users should be locked, 
   *              False to unlock UI users.
   */
  void lockEventsUIusers(boolean lock) throws RemoteException;
  
  /**
   * Method to get list of dependent techpack and interface along with mount info. 
   * @param techpackName
   * @return
   * @throws RemoteException
   */
  List<String> getDependentList(String techpackName) throws RemoteException;
  
  /**
   * Method to disable dependent techpack or interface and reload scheduler cache. 
   * @param dependentList
   * @throws RemoteException
   */
  void disableDependentTP(final List<String> dependentList) throws RemoteException;
  
  /**
   * Method to enable dependent techpack or interface and reload scheduler cache.
   * @param dependentList
   * @throws RemoteException
   */
  void enableDependentTP(final List<String> dependentList) throws RemoteException;
  
  /**
   * Method to return the free execution slots in the engine
   * 
   * @return free execution slots
   * @throws RemoteException
   */
  int getFreeExecutionSlots() throws RemoteException;
  
  /**
   * Returns the number of free slots that can run given set
   * 
   * @param setType
   * @return
   * @throws RemoteException
   */
  int getFreeExecutionSlots(String setType) throws RemoteException;
}

