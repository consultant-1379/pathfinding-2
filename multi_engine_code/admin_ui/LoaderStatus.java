package com.distocraft.dc5000.etl.gui.systemmonitor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.stream.Stream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.velocity.Template;
import org.apache.velocity.context.Context;

import com.distocraft.dc5000.common.ServertypesHelper;
import com.distocraft.dc5000.etl.engine.main.ITransferEngineRMI;
import com.distocraft.dc5000.etl.gui.common.ENIQServiceStatusInfo;
import com.distocraft.dc5000.etl.gui.common.Environment;
import com.distocraft.dc5000.etl.gui.common.EtlguiServlet;
import com.distocraft.dc5000.etl.gui.common.RmiUrlFactory;
import com.distocraft.dc5000.etl.gui.config.Configuration;
import com.distocraft.dc5000.etl.gui.config.ConfigurationFactory;
import com.distocraft.dc5000.etl.gui.config.ServiceNames;
import com.distocraft.dc5000.etl.gui.enminterworking.EnmInterUtils;
import com.distocraft.dc5000.etl.gui.util.DateFormatter;
import com.distocraft.dc5000.etl.gui.util.Helper;
import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.scheduler.ISchedulerRMI;
import com.ericsson.eniq.common.DatabaseConnections;
import com.ericsson.eniq.common.RemoteExecutor;
import com.ericsson.eniq.common.lwp.ILWPHelperRMI;
import com.ericsson.eniq.common.lwp.LwpServer;
import com.ericsson.eniq.enminterworking.IEnmInterworkingRMI;
import com.ericsson.eniq.licensing.cache.LicensingCache;
import com.ericsson.eniq.repository.DBUsersGet;
import com.ericsson.eniq.repository.dbusers.AddServiceUser;
import com.jcraft.jsch.JSchException;

import ssc.rockfactory.RockFactory;


//Related to FRH
/*import com.distocraft.dc5000.etl.gui.common.FRHLicenseCheck;
import com.ericsson.eniq.exception.LicensingException;*/


public class LoaderStatus extends EtlguiServlet {

	// NOPMD by eheijun on
	// 03/06/11 08:20

	private static final long serialVersionUID = 1L;

	private static final String ADMINUI_ERROR_PAGE_TEMPLATE = "adminuiErrorPage.vm";

	private static final String LOADER_STATUS_TEMPLATE = "loader_status.vm";

	private static final String UNKNOWN_SERVICE_STATE = "Unknown";

	public static final String MZ_INFORMATION = "mzInformation";

	public static final String GLASSFISH_INFO = "glassFishInfo";

	private static final Log log = LogFactory.getLog(LoaderStatus.class);

	private ServiceNames servicenames;

	private static Set<String> hosts;

	private Set<String> ecs;

	private Set<String> services;

	private static RockFactory rockfactoryObject;

	private final String rmiPort = "1200";

	protected Map<String, List<String>> hostToServiceMap;

	private long lastModifiedTime;

	protected List serviceStatusList;

	protected Map<String, Map<String, String>> serviceToReaderOrWriterMap;

	private static AddServiceUser addMissingServiceUsers = new AddServiceUser();

	private boolean DNS_STATUS = true;
	
	private static final String max_counter_volume_filename = "/eniq/sw/conf/max_counter_volume";

	private static final File counter_volume_file = new File(max_counter_volume_filename);

	private boolean counter_volume_file_exists = true;
	
	private static final String APPLICATION_USER = "dcuser";

	protected static final String HOST_ADD = "webserver";
	
	protected static final String  PASS_AGING = "passAgingColor";
	//global constants related to FRH
	
/*	public final static String FRHUSER = "dcuser";

	public final static String SERVICE_NAME = "frh";

	public static final String FRH_OPERATIONAL = "FRH state is Operational..";

	public static final String FRH_NONOPERATIONAL = "FRH state is Non-Operational..";

	public static final String FRH_DEGRADED = "FRH state is degraged..";

	public static final String CONTROLLERSTARTTIME_FILEPATH = "/eniq/log/controller/.controller_startTime.txt";

	private static final String FRH_NAME = "ERIC_FRH";
*/


	/**
	 * Function to parse the service_names file and populate a map with host
	 * name and its corresponding services
	 */
	protected void populateHostNameToTypeMapping() {
		if (hostToServiceMap == null) {
			hostToServiceMap = new HashMap<String, List<String>>();
		}
		final File file = new File(System.getProperty("CONF_DIR", "/eniq/sw/conf"),
				System.getProperty("service_names", "service_names"));
		try {
			if (file.exists()) {
				if (lastModifiedTime != file.lastModified()) {
					lastModifiedTime = file.lastModified();
					hostToServiceMap.clear();
				} else {
					log.debug("Already populated hostToServiceMap: " + hostToServiceMap.toString());
					return;
				}
				final FileReader fr = new FileReader(file);
				final BufferedReader br = new BufferedReader(fr);
				try {
					String line = null;
					StringTokenizer tokenizer = null;
					while ((line = br.readLine()) != null) {
						if (!(line.contains(ServiceNames.SPECIAL_CHARECTER1)
								|| line.contains(ServiceNames.SPECIAL_CHARECTER2))) {
							tokenizer = new StringTokenizer(line, ServiceNames.DELIMITER);
							if (tokenizer.hasMoreTokens()) {
								tokenizer.nextToken(); // IP, skip it
								if (tokenizer.hasMoreTokens()) {
									final String hostName = tokenizer.nextToken();
									List<String> serviceList = hostToServiceMap.get(hostName);
									if (serviceList != null) {
										if (tokenizer.hasMoreTokens()) {
											final String serviceName = tokenizer.nextToken();
											serviceList.add(serviceName);
											hostToServiceMap.put(hostName, serviceList);
										}

									} else {
										if (tokenizer.hasMoreTokens()) {
											final String serviceName = tokenizer.nextToken();
											serviceList = new ArrayList<String>();
											serviceList.add(serviceName);
											hostToServiceMap.put(hostName, serviceList);
										}
									}

								}
							}
						}
					}
				} finally {
					br.close();
				}
				log.debug("Host to service Map populated: " + hostToServiceMap);
			} else {
				log.error("File: " + file.getAbsolutePath() + " does not exist.");
			}
		} catch (final Exception e) {
			log.error("Exception while parsing file: " + file.getAbsolutePath(), e);
		}
	}

	@Override
	public Template doHandleRequest(final HttpServletRequest request, final HttpServletResponse response,
			final Context context) throws Exception {
		// NOPMD by eheijun on
		
		// 03/06/11 08:20
		
		try {
			rockfactoryObject = (RockFactory) context.get("rockEtlRep");
			final Configuration configuration = ConfigurationFactory.getConfiguration();
			servicenames = configuration.getServiceNames();
			hosts = servicenames.getHosts();
			log.debug("found " + hosts.size() + " hosts");
			services = servicenames.getServices();
			log.debug("found " + services.size() + " services");
			ecs = servicenames.getECs();
			log.debug("found " + ecs.size() + " EC hosts");

			final Environment environment = (Environment) context.get(ENVIRONMENT);
			log.debug("environment is " + environment);

			final MonitorInformation licSrv = getStatusMonitor("LicSrv", request.getParameter("licsrv"), environment);
			log.debug("licSrv information gathered");
			final MonitorInformation licMgr = getStatusMonitor("LicMgr", request.getParameter("licmgr"), environment);
			log.debug("licMgr information gathered");
			final MonitorInformation transferEngine = getStatusMonitorForEngine("TransferEngine", request.getParameter("te"),
					environment);
			log.debug("engine information gathered");
			final MonitorInformation lwpHelper = getStatusMonitor("lwphelper", request.getParameter("lwphelper"),
					environment);
			log.debug("lwphelper information gathered");
			final MonitorInformation fls = getStatusMonitor("MultiESController", request.getParameter("fls"),
					environment);
			log.debug("fls information gathered");
			final MonitorInformation scheduler = getStatusMonitor("Scheduler", request.getParameter("sc"), environment);
			log.debug("scheduler information gathered");

			final MonitorInformation dwhdb = getDwhdbInformation();
			log.debug("dwhdb information gathered");

			final MonitorInformation rollSnapshot = getStatusMonitor("RollingSnapshot",
					request.getParameter("rollsnap"), environment);
			log.debug("Rolling snapshot status information gathered");

			final MonitorInformation ombsBackup = getStatusMonitor("OMBSBackup", request.getParameter("ombsbackup"),
					environment);
			log.debug("OMBS backup status information gathered");

			//Related to frh
			/*
			final MonitorInformation frh = getStatusMonitor("frh", "frh", environment);
			final boolean frhStatus = getFrhStatus(); */
				
			final List<DatabaseInfo> dbInfos = getDataSourceStatusDetails(context);

			context.put("databases", dbInfos);
			log.debug("database information gathered");

			// To find out which service is reader and which is writer
			populateMapServiceToReaderOrWriter(context);

			populateHostNameToTypeMapping();
			if (services.contains(MonitorInformation.GLASSFISH_HOST_ID)) {
				context.put(GLASSFISH_INFO, getGlassfishStatus());
				log.debug("glassfish information gathered");
			} else {
				context.put("CounterVolumeDisplay", "YES");
				context.put("counterVolume", getCounterVolume(context));
			}
			if (services.contains(MonitorInformation.MZ_HOST_ID)) {
				context.put(MZ_INFORMATION, getMediationGatewayStatus());
				log.debug("mediazone information gathered");
			}
			context.put("hardwareInformation", getHardwareInformation(context));
			log.debug("hardware information gathered");
			context.put("transferEngine", transferEngine);
			context.put("isPassAgingApplied", osPasswordDetails(context));
			context.put("lwphelper", lwpHelper);
			context.put("fls", fls);
			context.put("scheduler", scheduler);
			context.put("licSrv", licSrv);
			context.put("licMgr", licMgr);
			context.put("dwhdb", dwhdb);
			context.put("RollingSnapshot", rollSnapshot);
			context.put("OMBSBackup", ombsBackup);
			context.put("userNames",passwordDetails(context, log));
			context.put("colour", certificateAlarm(log));

			//Related to FRH
			/*context.put("frh", frh);
			context.put("frhStatus", frhStatus);*/
			
			if (isFlsEnabled()) {
				Connection connDwh = ((RockFactory) context.get("rockDwhRep")).getConnection();				
				try {
					if (EnmInterUtils.getSelfRole(connDwh).equals("MASTER")) {
						int unassignedNodes = getUnassignedNodes(connDwh);
						context.put("unassignedNodes", unassignedNodes);
					}
				} finally {
					connDwh.close();
				}
			}
			
			return getTemplate(LOADER_STATUS_TEMPLATE);
		} catch (final Exception e) {
			log.error("LoaderStatus.doHandleRequest failed", e);
			context.put("errorSet", true);
			context.put("errorText", "LoaderStatus.doHandleRequest failed due to exception: " + e.getMessage());
			return getTemplate(EtlguiServlet.ADMINUI_NEW_ERROR_PAGE_TEMPLATE_WITHMENU);
			// return getTemplate(ADMINUI_ERROR_PAGE_TEMPLATE);
		}	
	}
	
	 /**
	 * @return true if FLS is enabled in integrated ENM server false if FLS is
	 *         not enabled
	 */
	private Boolean isFlsEnabled() {
		  try {
			IEnmInterworkingRMI multiEs = (IEnmInterworkingRMI) Naming.lookup(com.distocraft.dc5000.common.RmiUrlFactory
					.getInstance().getMultiESRmiUrl(EnmInterUtils.getEngineIP()));

			  return multiEs.IsflsServiceEnabled();
		} catch (Exception e) {
			  return false;
		  }
	  }
	
	private Boolean isFlsProfileOnHold()
	{
		try {
			IEnmInterworkingRMI multiEs = (IEnmInterworkingRMI) Naming.lookup(com.distocraft.dc5000.common.RmiUrlFactory
					.getInstance().getMultiESRmiUrl(EnmInterUtils.getEngineIP()));
			if(multiEs.status().get(1).get(1).contains("OnHold"))
			{
				return true;	
			}
			return false;
		}
		catch (Exception e) {
			  return true;
		  }
		
	}
	
	public boolean flsConfExistsAndHasEntry() throws Exception {
		    	final File flsConf = new File("/eniq/installation/config/fls_conf");
				if(flsConf.exists() && !(flsConf.length() == 0)){
					log.info("FLS is configured");
					return true;
				}else{
					return false;
				}
	  }

	private int getUnassignedNodes(Connection connDwh) {
		int unassignedNodes = 0;
		String sql = "Select count(*) from ENIQS_Node_Assignment where ENIQ_IDENTIFIER = ''";
		if (isFlsEnabled()) {			
			try(Statement stmt = connDwh.createStatement();
				ResultSet rSet = stmt.executeQuery(sql);) {

				while (rSet.next()) {
				unassignedNodes = rSet.getInt(1);
					log.debug("The unassigned nodes are " + unassignedNodes);
				}
			} catch (Exception e) {
				log.error("Exception: ", e);
			}
		} else {
			log.debug("fls is not enabled");
		}
		return unassignedNodes;
	}

	private void populateMapServiceToReaderOrWriter(Context context) {
		try {
			if (serviceToReaderOrWriterMap == null) {
				serviceToReaderOrWriterMap = new HashMap<String, Map<String, String>>();
			} else {
				serviceToReaderOrWriterMap.clear();
			}

			Statement s = ((RockFactory) context.get(MonitorInformation.IQ_NAME_STATUS_DWH)).getConnection()
					.createStatement();
			final String query = "sp_iqmpxinfo";
			ResultSet rs = s.executeQuery(query);
			while (rs.next()) {
				String service = rs.getString("server_name");
				String type = rs.getString("role");
				String status = rs.getString("inc_state");
				HashMap<String, String> map = new HashMap<String, String>();
				map.put(type, status);
				serviceToReaderOrWriterMap.put(service, map);
			}
			log.debug("populateMapServiceToReaderOrWriter:: Map content: " + serviceToReaderOrWriterMap.toString());
			rs.close();
			s.close();
		} catch (SQLException e) {
			log.error("Exception while creating database connection with: " + MonitorInformation.IQ_NAME_STATUS_REP, e);
		} catch (final Exception e) {
			log.error("Exception while creating database connection with: " + MonitorInformation.IQ_NAME_STATUS_REP, e);
		}

	}

	/**
	 * Gets hardware information of this loader server. Works only on windows
	 * xp, hpux and Solaris
	 * 
	 * @return MonitorInformation that has the hardware status information of
	 *         this host.
	 */
	private List<MonitorInformation> getHardwareInformation(final Context context) {

		final List<MonitorInformation> monitorInformations = new ArrayList<MonitorInformation>();
		MonitorInformation monitorInformation = new MonitorInformation();

		monitorInformation = null;
		
		final int servicesListinPropFile = servicenames.getServices().size();
		List<Meta_databases> servicesListinDB = DBUsersGet.getMetaDatabases(MonitorInformation.DC_USER_NAME, "ALL");
		if ((servicesListinPropFile > servicesListinDB.size())) {
			addServiceUser();
		}

		try {
			int count = 0;
			final Environment environment = (Environment) context.get(ENVIRONMENT);

			for (String host : hosts) {

				monitorInformation = new MonitorInformation(); // NOPMD by
																// eheijun on
																// 03/06/11
																// 08:19

				monitorInformation.setStatus(MonitorInformation.BULB_GREEN);
				monitorInformation.setFieldName(MonitorInformation.TITLE_HOST);
				monitorInformation.setHostname(host);

				log.debug("THE OUTPUT FOR REMOTEHOST" + host);
				String systemCommandString = "";
				final String user = MonitorInformation.DC_USER_NAME;

				final String service_name = servicenames.getFirstServiceName(host);
				List<Meta_databases> mdList = DBUsersGet.getMetaDatabases(MonitorInformation.DC_USER_NAME,
						service_name);
				if (mdList.isEmpty()) {
					addServiceUser();
					mdList = DBUsersGet.getMetaDatabases(MonitorInformation.DC_USER_NAME, service_name);
					if (mdList.isEmpty()) {
						throw new Exception("Could not find an entry for " + MonitorInformation.DC_USER_NAME + ":"
								+ service_name + " in repdb! (was is added?)");
					}
				}
				final String password = mdList.get(0).getPassword();

				if (MonitorInformation.OS_VERSION.toLowerCase().indexOf("windows") == -1) {
					// Run hpux & Solaris specific commands.
					if (MonitorInformation.OS_VERSION.toLowerCase().indexOf("hp") == -1) {

						systemCommandString = MonitorInformation.CMD_SO_HW_VERSION;
					} else {
						systemCommandString = MonitorInformation.CMD_HP_HW_VERSION;
					}
					monitorInformation.setHwversion(
							RemoteExecutor.executeComand(user, password, host, systemCommandString) + "<br />");

					monitorInformation.setOsversion(RemoteExecutor
							.executeComand(user, password, host, MonitorInformation.CMD_OS_VERSION_S).trim() + "&nbsp;"
							+ RemoteExecutor.executeComand(user, password, host, MonitorInformation.CMD_OS_VERSION_R)
									.trim()
							+ "&nbsp;" + "<br />");

					monitorInformation.setHostname(
							RemoteExecutor.executeComand(user, password, host, MonitorInformation.CMD_HOSTNAME)
									+ "<br />");
					monitorInformation
							.setUptime(RemoteExecutor.executeComand(user, password, host, MonitorInformation.CMD_UPTIME)
									+ "<br />");
				} else {
					// Run windows specific commands.
					systemCommandString = MonitorInformation.CMD_WIN_HW_VERSION;
					String hardwareString = RemoteExecutor.executeComand(user, password, host, systemCommandString);

					hardwareString = hardwareString.substring(hardwareString.indexOf(":") + 1, hardwareString.length())
							+ "<br />";

					String osNameString = RemoteExecutor
							.executeComand(user, password, host, MonitorInformation.CMD_OS_NAME).trim();
					osNameString = osNameString.substring(osNameString.indexOf(":") + 1, osNameString.length());

					String osVersionString = RemoteExecutor
							.executeComand(user, password, host, MonitorInformation.CMD_OS_VERSION_WIN).trim();
					osVersionString = osVersionString.substring(osVersionString.indexOf(":") + 1,
							osVersionString.length()) + "<br />";
					monitorInformation.setOsversion(osNameString + "&nbsp;" + osVersionString);

					final String hostNameString = RemoteExecutor.executeComand(user, password, host,
							MonitorInformation.CMD_HOSTNAME_WIN);
					monitorInformation.setHostname(hostNameString);

					String uptimeString = RemoteExecutor.executeComand(user, password, host,
							MonitorInformation.CMD_WIN_UPTIME);
					uptimeString = uptimeString.substring(uptimeString.indexOf(":") + 1, uptimeString.length())
							+ "<br />";
					monitorInformation.setUptime(uptimeString);
				}

				List<String> servicesForThisHost = hostToServiceMap.get(host);
				populateServiceStatusList(servicesForThisHost, monitorInformation, host, context);

				if (environment != null && Environment.Type.EVENTS.equals(environment.getType())) {
					if (hosts.size() > 1) {

						applicationStatusForMultipleHost(servicesForThisHost, monitorInformation, context);
					} else {

						applicationStatusForSingleHost(servicesForThisHost, monitorInformation, context, host);

					}
				}

				log.debug("Host Name: " + monitorInformation.getHostname());

				log.debug("List of Service Stats: " + serviceStatusList.toString());
				monitorInformation.setServiceStatusList(serviceStatusList);
				log.debug("Retreiving List: " + monitorInformation.getServiceStatusList().toString());

				monitorInformations.add(count, monitorInformation);
				count++;
				monitorInformation = null;
			}
			log.debug("The Ouput of the hosts" + hosts);
		} catch (final JSchException e) {
			statusCommandError(monitorInformation, e);
			log.error("JSchException ", e);
			monitorInformation.setStatus(MonitorInformation.BULB_GRAY);
			monitorInformation.setFieldName(MonitorInformation.TITLE_HOST);
			monitorInformations.add(monitorInformation);

		} catch (final Exception e) {
			statusCommandError(monitorInformation, e);
			log.error("Exception ", e);
			monitorInformation.setStatus(MonitorInformation.BULB_GRAY);
			monitorInformation.setFieldName(MonitorInformation.TITLE_HOST);
			monitorInformations.add(monitorInformation);

		}
		return monitorInformations;
	}

	private MonitorInformation getDwhdbInformation() {
		MonitorInformation monitorInformation = new MonitorInformation();
		try {
			int count = 0;
			String systemCommandString = "";
			final String user = MonitorInformation.DC_USER_NAME;

			final String service_name = "dwhdb";
			List<Meta_databases> mdList = DBUsersGet.getMetaDatabases(MonitorInformation.DC_USER_NAME, service_name);
			if (mdList.isEmpty()) {
				addServiceUser();
				mdList = DBUsersGet.getMetaDatabases(MonitorInformation.DC_USER_NAME, service_name);
				if (mdList.isEmpty()) {
					throw new Exception("Could not find an entry for " + MonitorInformation.DC_USER_NAME + ":"
							+ service_name + " in repdb! (was is added?)");
				}
			}
			final String password = mdList.get(0).getPassword();
			systemCommandString = MonitorInformation.DWHDB_STATUS_COMMAND;
			String dwhDbStatus = RemoteExecutor.executeComand(user, password, service_name, systemCommandString);
			if (dwhDbStatus != null && !dwhDbStatus.equals(" ")) {
				if (dwhDbStatus.contains(MonitorInformation.DWHDB_STATUS_RUNNING)) {
					ENIQServiceStatusInfo.setdwhDBHealth(ENIQServiceStatusInfo.ServiceHealth.Online);
				} else {
					ENIQServiceStatusInfo.setdwhDBHealth(ENIQServiceStatusInfo.ServiceHealth.Offline);
				}
			}

		} catch (final JSchException e) {
			statusCommandError(monitorInformation, e);
			log.error("JSchException ", e);
		} catch (final Exception e) {
			statusCommandError(monitorInformation, e);
			log.error("Exception ", e);
		}

		return monitorInformation;
	}

	/**
	 * Call AddServiceUsers to add all missing connection details to
	 * meta_databases
	 */
	private void addServiceUser() {
		String[] args = { "-all" };
		addMissingServiceUsers.addServiceUsers(args);
	}

	private void populateServiceStatusList(final List<String> servicesForThisHost, final MonitorInformation mi,
			final String host, final Context context) throws Exception {

		//Frh related code to add server information of FRH(Remove comments when required)
		/*String serverType;
		boolean frhService = false;*/

		if (serviceStatusList == null) {
			serviceStatusList = new ArrayList();
		} else {
			serviceStatusList.clear();
		}
		for (String service : servicesForThisHost) {

			log.info("Service: " + service);
			
			Map<String, String> serviceStatusMap = new HashMap<String, String>();
			final String serviceName = service.trim();
			if (serviceName.equalsIgnoreCase("engine")) {
				if (ENIQServiceStatusInfo.isEngineOnline()) {
					serviceStatusMap.put(serviceName, ServicesStatusStore.ONLINE);
				} else {
					serviceStatusMap.put(serviceName, ServicesStatusStore.OFFLINE);
				}
			} else if (serviceName.equalsIgnoreCase("lwphelper")) {
				if (ENIQServiceStatusInfo.isLwpHelperOnline()) {
					serviceStatusMap.put(serviceName, ServicesStatusStore.ONLINE);
				} else {
					serviceStatusMap.put(serviceName, ServicesStatusStore.OFFLINE);
				}
			} else if (serviceName.equalsIgnoreCase("fls")) {
				if (flsConfExistsAndHasEntry()){
					if (ENIQServiceStatusInfo.isFlsOnline()) {
						serviceStatusMap.put(serviceName, ServicesStatusStore.ONLINE);
					}else if (ENIQServiceStatusInfo.isFlsOnHold()){
						serviceStatusMap.put(serviceName, ServicesStatusStore.YELLOW);
					}else {
						serviceStatusMap.put(serviceName, ServicesStatusStore.OFFLINE);
					}
				} 
			} else if (serviceName.equalsIgnoreCase("scheduler")) {
				if (ENIQServiceStatusInfo.isSchedulerOnline()) {
					serviceStatusMap.put(serviceName, ServicesStatusStore.ONLINE);
				} else {
					serviceStatusMap.put(serviceName, ServicesStatusStore.OFFLINE);
				}

			} else if (serviceName.equalsIgnoreCase("licenceservice")) {
				if (ENIQServiceStatusInfo.isLicManagerOnline()) {
					serviceStatusMap.put(serviceName, ServicesStatusStore.ONLINE);
				} else {
					serviceStatusMap.put(serviceName, ServicesStatusStore.OFFLINE);
				}
			} else if (serviceName.equalsIgnoreCase("ldapserver")) {
				if (ENIQServiceStatusInfo.isLdapOnline()) {
					serviceStatusMap.put(serviceName, ServicesStatusStore.ONLINE);
				} else {
					serviceStatusMap.put(serviceName, ServicesStatusStore.OFFLINE);
				}
			} else if (serviceName.equalsIgnoreCase("repdb")) {
				if (ENIQServiceStatusInfo.isRepDBOnline()) {
					serviceStatusMap.put(serviceName, ServicesStatusStore.ONLINE);
				} else {
					serviceStatusMap.put(serviceName, ServicesStatusStore.OFFLINE);
				}
			} else if (serviceName.equalsIgnoreCase("dwhdb")) {
				if (ENIQServiceStatusInfo.isDwhDBOnline()) {
					serviceStatusMap.put(serviceName, ServicesStatusStore.ONLINE);
				} else {
					serviceStatusMap.put(serviceName, ServicesStatusStore.OFFLINE);
				}
			} else if (serviceName.equalsIgnoreCase("webserver")) {
				serviceStatusMap.put(serviceName, ServicesStatusStore.ONLINE);
				
				//Related to FRH server information fetching
			/*} else if (serviceName.contains("frh")) {
				serviceStatusMap.put(serviceName, ServicesStatusStore.ONLINE);
				serverType = FRH_NAME;
				frhService = true;*/
				
			} else if (serviceName.contains("dwh_reader")) {
				try {
					// serviceToReaderOrWriterMap would be empty for single
					// blade environment
					// check size first and only get status of node for
					// multiblade
					if (serviceToReaderOrWriterMap.size() > 0) {
						log.debug("serviceToReaderOrWriterMap:: " + serviceToReaderOrWriterMap.toString());
						Map<String, String> typeStatusMap = serviceToReaderOrWriterMap.get(serviceName);
						log.debug("typeStatusMap:: " + typeStatusMap.toString());
						for (String type : typeStatusMap.keySet()) {
							final String status = typeStatusMap.get(type);
							if (status.equalsIgnoreCase("active")) {
								serviceStatusMap.put(serviceName, ServicesStatusStore.ONLINE);
							} else {
								serviceStatusMap.put(serviceName, ServicesStatusStore.OFFLINE);
							}
						}
					}
				} catch (Exception e) {
					log.error("problem getting reader status for " + serviceName + "\n" + e);
				}
			} else if (serviceName.equalsIgnoreCase(MonitorInformation.GLASSFISH_HOST_ID)) {

				MonitorInformation glassfishInfo = (MonitorInformation) context.get(GLASSFISH_INFO);
				// glassfish is online?
				if (glassfishInfo.isGreen()) {
					serviceStatusMap.put(serviceName, ServicesStatusStore.ONLINE);
				} else if (glassfishInfo.isRed()) {
					serviceStatusMap.put(serviceName, ServicesStatusStore.OFFLINE);
				} else {
					serviceStatusMap.put(serviceName, ServicesStatusStore.GRAY);
				}
			} else if (ECStatus.isLoggedService(serviceName)) {
				if (ECStatus.ecIsRunning(serviceName)) {
					serviceStatusMap.put(serviceName, ServicesStatusStore.ONLINE);
				} else {
					serviceStatusMap.put(serviceName, ServicesStatusStore.OFFLINE);
				}
			} else {
				// Unknown service
				log.error("Unknown service comes: " + serviceName);
				serviceStatusMap.put(serviceName, ServicesStatusStore.GRAY);
			}

			// for single blader, dwh_reader_1, there would be nothing to add
			// so check there is something to add before trying to add it...
			if (serviceStatusMap.size() > 0) {
				serviceStatusList.add(serviceStatusMap);
			}
		}
		
/*FRH server information related	
 	try {
			if (frhService) {
				serverType = FRH_NAME;
			} else {
				serverType = ServertypesHelper.getDisplayServertype(host);
			}
*/
		String serverType;
		try {
			serverType = ServertypesHelper.getDisplayServertype(host);
		} catch (IOException e) {
			log.error("Can't get server type for " + host);
			serverType = "Unknown";
		}
		final String hostName = host + " (<b>" + serverType + "</b>)";
		mi.setHostname(hostName);
	}

	/**
	 * @param string
	 * @param string2
	 * @return
	 */
	protected List<Meta_databases> getMetaDatabasesObjects(final String USERNAME, final String CONNECTION_NAME) {
		return DBUsersGet.getMetaDatabases("ALL", "ALL");
	}

	/**
	 * @return
	 */
	protected Meta_databases getMeta_DatabasesObject() {
		return new Meta_databases(rockfactoryObject);
	}

	/**
	 * Get hostname from IpAddress using host command.
	 * 
	 * @return hostname with domain . If errors/exceptions are encountered while
	 *         trying to get the domain name, then Ipaddress is returned .
	 */
	private String getHostnameWithDomain(final java.net.InetAddress ipAddress) {

		String hostNameWithDomain = ipAddress.getHostAddress();
		final SystemCommand systemCommand = new SystemCommand();
		try {
			final java.net.InetAddress hostName = java.net.InetAddress.getByName(ipAddress.getHostAddress());
			final String hostCommand = Helper.getEnvEntryString(MonitorInformation.CMD_HOST);
			if (DNS_STATUS == true) {
				final String tempResult = systemCommand.runCmdPlain(hostCommand + " " + ipAddress.getHostAddress());
				if (tempResult.indexOf(hostName.getHostName()) == -1) {
					log.debug("getHostnameWithDomain():hostCommand is" + hostCommand);
					log.debug("getHostnameWithDomain():host of ipaddress is" + tempResult);
					log.error("getHostnameWithDomain():Problem with result of host command execution");
					DNS_STATUS = false;
				} else {
					hostNameWithDomain = tempResult.substring(tempResult.indexOf(hostName.getHostName()),
							tempResult.length() - 1);
				}
			}
		} catch (java.net.UnknownHostException e) {
			log.error("getHostnameWithDomain():UnknownHostException", e);
		} catch (final IOException ioe) {
			log.error("getHostnameWithDomain():IOException", ioe);
		} catch (final Exception e) {
			log.error("getHostnameWithDomain():Exception ", e);
		}
		return hostNameWithDomain;
	}

	/**
	 * Get status of Mediation Gateway execution contexts.
	 * 
	 * @return capture output of mediation zone status command host.
	 */
	private MonitorInformation getMediationGatewayStatus() {

		final MonitorInformation monitorInformation = new MonitorInformation();
		monitorInformation.setFieldName(MonitorInformation.MZ_STATUS_FIELD_NAME);

		try {
			final java.net.InetAddress mzIPAddress = java.net.InetAddress.getByName(MonitorInformation.MZ_HOST_ID);
			monitorInformation.setHostname(mzIPAddress.getHostAddress());
			if (ENIQServiceStatusInfo.isRepDBOffline()) {
				monitorInformation.setStatus(MonitorInformation.BULB_GRAY);
				monitorInformation.appendToStatusMessage(
						"<font color='red' face='verdana' size='1px'>ENIQ REP database is down. Could not execute command to get the status.</font><br/>");
				return monitorInformation;
			}
			monitorInformation.setStatus(MonitorInformation.BULB_YELLOW);
			final String user = MonitorInformation.DC_USER_NAME;
			final String mzcommand = Helper.getEnvEntryString(MonitorInformation.MZ_MEDIATION_STATUS_COMMAND);
			final String password = DBUsersGet
					.getMetaDatabases(MonitorInformation.DC_USER_NAME, MonitorInformation.MZ_HOST_ID).get(0)
					.getPassword();

			String statusOutput = RemoteExecutor.executeComand(user, password, MonitorInformation.MZ_HOST_ID, mzcommand)
					.trim();

			final java.net.InetAddress hostName = java.net.InetAddress.getByName(mzIPAddress.getHostAddress());
			final String mzHostNameWithDomain = getHostnameWithDomain(mzIPAddress);
			int ecport = 0;
			for (String ec : ecs) {
				final java.net.InetAddress ecIPAddress = java.net.InetAddress.getByName(ec);
				final String ecHostNameWithDomain = getHostnameWithDomain(ecIPAddress);
				final int index = ec.indexOf('_');
				if (ec.equals("ec_2")) {
					ecport = Helper.getEnvEntryInt(MonitorInformation.MZ_EC_PORT) + 1;
				} else {
					ecport = Helper.getEnvEntryInt(MonitorInformation.MZ_EC_PORT);
				}
				final StringBuilder EC = new StringBuilder(); // NOPMD by
																// eheijun on
																// 03/06/11
																// 08:19
				EC.append(ec.substring(index + 1));
				final StringBuilder ecHref = new StringBuilder(); // NOPMD by
																	// eheijun
																	// on
																	// 03/06/11
																	// 08:20
				ecHref.append("<a  class='a_bold'target='_blank' href='http://" + ecHostNameWithDomain + ":" + ecport
						+ "' ><b><font size=2>Execution Context Monitor</font></b> " + "<b> : </b>" + "<b><font size=2>"
						+ ec + "</font></b></a>");

				if (statusOutput.contains(MonitorInformation.EXECUTION_CONTEXT_HREF + EC.toString() + " "
						+ MonitorInformation.MZ_STATUS_RUNNING_KEY)) {
					monitorInformation.appendToStatusMessage(ecHref.toString());
				}
			}
			DNS_STATUS = true;
			final StringBuilder czHref = new StringBuilder();
			czHref.append("<a class='a_bold' target='_blank' href='http://" + mzHostNameWithDomain + ":"
					+ Helper.getEnvEntryInt(MonitorInformation.MZ_PLATFORM_PORT)
					+ "/mz/main' ><b><font size=2>Platform Monitor</font></b></a>");
			if (statusOutput.contains(MonitorInformation.MZ_PLATFORM_RUNNING)) {
				monitorInformation.appendToStatusMessage(czHref.toString());
			}
			log.debug("The Inet address is " + hostName.getHostName());
			monitorInformation.setHostname(mzIPAddress.getHostAddress());

			if (statusOutput.contains(MonitorInformation.MZ_STATUS_RUNNING_KEY)) {

				monitorInformation.setStatus(MonitorInformation.BULB_GREEN);
				monitorInformation.appendToStatusMessage(MonitorInformation.STATUS_RUNNING);
				statusOutput = statusOutput.replace("Platform", ECStatus.CONTROL_ZONE);

				final String[] arrRunnings = statusOutput.split(MonitorInformation.MZ_STATUS_RUNNING);

				for (final String item : arrRunnings) {
					ECStatus.add(item);
					monitorInformation
							.appendToMediationGatewayStatusMessage(item + MonitorInformation.MZ_STATUS_RUNNING);
				}

			} else {
				monitorInformation.setStatus(MonitorInformation.BULB_RED);
				monitorInformation.appendToStatusMessage(MonitorInformation.STATUS_NOT_RUNNING);
			}

			log.debug("MG status output : " + statusOutput + " len = " + statusOutput.length());
			monitorInformation.appendToStatusMessage("<br>");
			log.debug("THE STATUS MZ OUTPUT IS " + statusOutput);
			final List<String> mgStatus = monitorInformation.getMediationGatewayStatusMessage();
			log.debug("MG status message: " + mgStatus + " size = " + mgStatus.size());

		} catch (final JSchException e) {
			statusCommandError(monitorInformation, e);
			log.error("JSchException ", e);
		} catch (final IOException e) {
			statusCommandError(monitorInformation, e);
			log.error("IOException ", e);
		} catch (final Exception e) {
			statusCommandError(monitorInformation, e);
			log.error("Exception ", e);
		}

		return monitorInformation;
	}

	/**
	 * Helper method for handling Glassfish status command execution .
	 * 
	 * @param monitorInformation
	 * 
	 **/

	public MonitorInformation getGlassfishStatus() {
		final MonitorInformation monitorInformation = new MonitorInformation();
		monitorInformation.setFieldName(MonitorInformation.GLASSFISH_STATUS_FIELD_NAME);

		try {
			final java.net.InetAddress glassfishIPAdd = java.net.InetAddress
					.getByName(MonitorInformation.GLASSFISH_HOST_ID);
			final java.net.InetAddress hostName = java.net.InetAddress.getByName(glassfishIPAdd.getHostAddress());
			monitorInformation.setHostname(hostName.getHostName());
			if (ENIQServiceStatusInfo.isRepDBOffline()) {
				monitorInformation.setStatus(MonitorInformation.BULB_GRAY);
				monitorInformation.appendToStatusMessage(
						"<font color='red' face='verdana' size='1px'>ENIQ REP database is down. Could not execute command to get the status.</font><br/>");
				return monitorInformation;
			}

			// Collect the output returned by systemCommand
			final String user = MonitorInformation.DC_USER_NAME;
			final String glassfsihcommand = Helper.getEnvEntryString(MonitorInformation.GLASSFISH_STATUS_COMMAND);
			final String password = DBUsersGet
					.getMetaDatabases(MonitorInformation.DC_USER_NAME, MonitorInformation.GLASSFISH_HOST_ID).get(0)
					.getPassword();

			final String statusOutput = RemoteExecutor
					.executeComand(user, password, MonitorInformation.GLASSFISH_HOST_ID, glassfsihcommand).trim();

			if (statusOutput.contains(MonitorInformation.GLASSFISH_STATUS_CHECK)) {
				monitorInformation.setStatus(MonitorInformation.BULB_GREEN);
				monitorInformation.appendToStatusMessage(MonitorInformation.STATUS_RUNNING);

				final String[] arrRunnings = statusOutput.split(MonitorInformation.GF_COMMAND_LIST);
				for (final String item : arrRunnings) {
					monitorInformation.appendToglassfishStatusMessage(item);
				}
				monitorInformation.appendToglassfishStatusMessage(MonitorInformation.GF_COMMAND_LIST);

				/*
				 * final String deployedApps = getGlassfishDeploymentStatus();
				 * monitorInformation
				 * .appendToglassfishStatusMessage(deployedApps);
				 */

				// formattedStatusOut = statusOutput.replaceFirst("- ", "");
			} else {
				log.debug("GLASSFSIH OUPUT STATUS = " + statusOutput);
				monitorInformation.setStatus(MonitorInformation.BULB_RED);
				monitorInformation.appendToStatusMessage(MonitorInformation.STATUS_NOT_RUNNING);

			}

		} catch (final JSchException e) {
			statusCommandError(monitorInformation, e);
			log.error("JSchException ", e);
		} catch (final IOException e) {
			statusCommandError(monitorInformation, e);
			log.error("IOException ", e);
		} catch (final Exception e) {
			statusCommandError(monitorInformation, e);
			log.error("Exception ", e);
		}
		return monitorInformation;
	}

	/*
	 * private static String getGlassfishDeploymentStatus() throws IOException,
	 * InstanceNotFoundException, ReflectionException,
	 * MalformedObjectNameException, MBeanException { final JMXServiceURL url =
	 * new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" +
	 * MonitorInformation.GLASSFISH_HOST_ID + ":8686/jmxrmi");
	 * System.out.println("Connecting to " + url.toString()); final Map<String,
	 * String[]> env = new HashMap<String, String[]>(1);
	 * 
	 * final String[] credentials = new String[]{"admin", "admin"};
	 * env.put(JMXConnector.CREDENTIALS, credentials);
	 * 
	 * final JMXConnector jmxc = JMXConnectorFactory.connect(url, env); final
	 * MBeanServerConnection server = jmxc.getMBeanServerConnection();
	 * 
	 * 
	 * final ObjectName applications = new
	 * ObjectName("amx:pp=/domain,type=applications"); final List<ObjectName>
	 * results = (List<ObjectName>) server.invoke(applications,
	 * "getApplications", null, null); final String[] appProperties =
	 * {"Enabled"}; final StringBuilder sb = new StringBuilder(); sb.append(
	 * "\n\n\nDeployed Applications\n"); try{ for (ObjectName oName : results) {
	 * final String appName = oName.getKeyProperty("name"); final String appRef
	 * = "amx:pp=/domain/servers/server[server],type=application-ref,name=" +
	 * appName; final ObjectName appRefName = new ObjectName(appRef); final
	 * AttributeList values = server.getAttributes(appRefName, appProperties);
	 * if (!values.isEmpty()) { final Attribute att = (Attribute) values.get(0);
	 * final Object value = att.getValue(); final boolean enabled = value ==
	 * null ? false : Boolean.valueOf(value.toString());
	 * sb.append(appName).append(" : "); if(enabled){ sb.append("Enabled"); }
	 * else { sb.append("Disabled"); } sb.append("\n"); } } } finally {
	 * jmxc.close(); } return sb.toString(); }
	 */

	/**
	 * Helper method for handling status command execution errors.
	 * 
	 * @param monitorInformation
	 * @param e
	 *            exception thrown while attempting to execute status command
	 */
	static void statusCommandError(final MonitorInformation monitorInformation, final Exception e) {
		monitorInformation.setStatus(MonitorInformation.BULB_RED);
		monitorInformation.appendToStatusMessage(MonitorInformation.STATUS_COMMAND_ERROR + " : " + e.getMessage());
		// monitorInformation.appendToStatusMessage("<font color='red'
		// face='verdana' size='2px'>ERROR:</font> "
		// + e.getMessage());
	}

	/**
	 * Check context for specific data base names and return those found in a
	 * list.
	 * 
	 * @param context
	 *            - velocity context
	 * @return list of databases
	 */
	private List<String> getDataSourceList() {
		final List<String> databases = new ArrayList<String>();

		// Always add DWH and REP as we want the status of these
		// whether initialised or not.
		databases.add(MonitorInformation.IQ_NAME_STATUS_DWH);
		databases.add(MonitorInformation.IQ_NAME_STATUS_REP);

		return databases;
	}

	/**
	 * Gets database status details for specified databases
	 * 
	 * @param context
	 *            is the Velocity context used.
	 * @return list of DatabaseInfo objects
	 */
	private List<DatabaseInfo> getDataSourceStatusDetails(final Context context) {
		final List<DatabaseInfo> databaseInfos = new ArrayList<DatabaseInfo>();
		final List<String> dataSources = getDataSourceList();

		log.debug("Data sources = " + dataSources);
		for (final String dataSourceName : dataSources) {
			databaseInfos
					.add(getDatabaseConnectionInfo((RockFactory) context.get(dataSourceName), dataSourceName, context));
		}

		return databaseInfos;
	}

	/**
	 * Get the database display name given the data source name
	 * 
	 * @param dataSourceName
	 * @return display name or null
	 */
	private static String getDataSourceDisplayName(final String dataSourceName) {
		String dbName = null;
		if (dataSourceName.equals(MonitorInformation.IQ_NAME_STATUS_DWH)) {
			dbName = ENIQServiceStatusInfo.getDwhDBName();
		}
		if (dataSourceName.equals(MonitorInformation.IQ_NAME_STATUS_REP)) {
			dbName = ENIQServiceStatusInfo.getRepDBName();
		}

		return dbName;
	}

	/**
	 * Fill Databaseinfo with limited status information. To recieve full status
	 * details, use
	 * <code>@link #getDetailedDatabaseConnectionInfo(Context, DatabaseInfo, String)</code>
	 * 
	 * @param rockFactory
	 * @param dataSourceName
	 *            uses MonitorInformation.IQ_NAME_STATUS_DWH,
	 *            MonitorInformation.IQ_NAME_STATUS_REP or
	 *            MonitorInformation.IQ_NAME_STATUS_BO
	 * @return holds info if receiving status info was a huge success or not.
	 */
	private DatabaseInfo getDatabaseConnectionInfo(final RockFactory rockFactory, final String dataSourceName,
			final Context context) { // NOPMD
		// by
		// eheijun
		// on
		// 03/06/11
		// 08:22

		final DatabaseInfo databaseInfo = new DatabaseInfo(getDataSourceDisplayName(dataSourceName));
		databaseInfo.setIsDetails(false);
		databaseInfo.setDetailUrl(dataSourceName);
		context.put("isRepDBOk", true);
		if (rockFactory == null) {
			databaseInfo.setStatus(MonitorInformation.BULB_RED);
			String dName = null;
			if (dataSourceName.equals(MonitorInformation.IQ_NAME_STATUS_REP)) {
				context.put("isRepDBOk", false);
				dName = ENIQServiceStatusInfo.getRepDBName();
				if (getRealTimeDBServiceStatus(MonitorInformation.repDBServiceName)) {
					// repDB service is UP and running
					databaseInfo.setStatus(MonitorInformation.BULB_GRAY);
					if (ENIQServiceStatusInfo.isRepDBConnLimitExceeded()) {
						final String message = "<br/>  Reason: Connection limit exceeds.";
						databaseInfo
								.setAllInfo("<font color='red' face='verdana' size='1'>Unable to connect to database '"
										+ dName + "'</font>" + message, true);
					} else {
						databaseInfo
								.setAllInfo("<font color='red' face='verdana' size='1'>Unable to connect to database '"
										+ dName + "'</font>", true);
					}
				} else {
					// repDB service is down or not pingable
					databaseInfo.setAllInfo(
							"<font color='red' face='verdana' size='2'><b>Failed to initialise database connection to '"
									+ dName + "'</b></font>",
							true);
				}
			} else if (dataSourceName.equals(MonitorInformation.IQ_NAME_STATUS_DWH)) {
				dName = ENIQServiceStatusInfo.getDwhDBName();
				if (getRealTimeDBServiceStatus(MonitorInformation.dwhDBServiceName)) {
					// dwhdb service is UP and running
					databaseInfo.setStatus(MonitorInformation.BULB_GRAY);
					if (ENIQServiceStatusInfo.isDwhDBConnLimitExceeded()) {
						final String message = "<br/>  Reason: Connection limit exceeds.";
						databaseInfo
								.setAllInfo("<font color='red' face='verdana' size='1'>Unable to connect to database '"
										+ dName + "'</font>" + message, true);
					} else {
						databaseInfo
								.setAllInfo("<font color='red' face='verdana' size='1'>Unable to connect to database '"
										+ dName + "'</font>", true);
					}
				} else {
					// dwhdb service is down or not pingable
					databaseInfo.setAllInfo(
							"<font color='red' face='verdana' size='2'><b>Failed to initialise database connection to '"
									+ dName + "'</b></font>",
							true);
				}
			}
			return databaseInfo;
		}

		String mainIq = null;
		String totalSpace = "";
		String freeSpace = "";
		String spaceUsed = "";
		String page = "";
		String version = "";
		String otherVersionSize = "";
		String backupTime = "";

		Statement statement = null;
		boolean errorOccured = false;

		try {

			if (dataSourceName.equals(MonitorInformation.IQ_NAME_STATUS_REP)) {
				// Get all the details from the SQL Anywhere Database...
				statement = rockFactory.getConnection().createStatement();
				// command to determine the: free pages, page size, total space
				// and version of DB...

				final ResultSet resultsetproductversion = statement.executeQuery(
						"SELECT PropName, Value FROM sa_eng_properties() where PropName='ProductVersion';");
				try {
					if (resultsetproductversion.next()) {
						if (resultsetproductversion.getString("PropName").equalsIgnoreCase("ProductVersion")) {
							version = resultsetproductversion.getString("Value");
						}
					}
				} finally {
					resultsetproductversion.close();
				}

				SystemCommand systemCommand = new SystemCommand();

				String repdb_info = "//eniq//database//rep_main//";
				String[] cmd = { "/bin/sh", "-c",
						"df -h " + repdb_info + " | awk " + "'" + "{print $4}" + "'" + "| tail -n +2" };
				log.info("Going to run command for freeSpace: " + cmd[2]);
				String cmdOutput = systemCommand.runCmdMultiple(cmd);
				log.info("command Output: " + cmdOutput);
				freeSpace = cmdOutput;

				repdb_info = "//eniq//database//rep_main//repdb.db";
				String[] cmd_args = { "/bin/sh", "-c", "ls -hl " + repdb_info + " | awk " + "'" + "{print $5}" + "'" };
				log.info("Going to run command for spaceUsed: " + cmd_args[2]);
				cmdOutput = systemCommand.runCmdMultiple(cmd_args);
				log.info("command Output: " + cmdOutput);
				spaceUsed = cmdOutput;

				// Code changes for TR HP61285, taking the max date from the
				// syshistory table.
				final ResultSet resultsetbackuptime = statement.executeQuery(
						"SELECT max(last_time) as last_time FROM sys.syshistory WHERE operation = 'LAST_BACKUP'");
				try {
					if (resultsetbackuptime.next()) {
						backupTime = resultsetbackuptime.getString("last_time");
					}
					log.info("getDatabaseConnectionInfo: BACKUP TIME          = " + backupTime);
				} finally {
					resultsetbackuptime.close();
				}
			} else {
				statement = rockFactory.getConnection().createStatement();
				final ResultSet rs = statement.executeQuery("sp_iqstatus");
				try {
					while (rs.next()) {
						if (rs.getString(1).indexOf("Version") != -1 && rs.getString(1).indexOf("Versions") == -1) {
							final String ver = rs.getString(2);
							log.debug("SP_IQSTATUS VERSION = " + ver);
							version = StatusDetails.parseDbversion(ver);
						} else if (rs.getString(1).indexOf("Main IQ Blocks") != -1
								&& rs.getString(1).indexOf("Versions") == -1) {
							mainIq = rs.getString(1) + rs.getString(2);
						} else if (rs.getString(1).indexOf("Page Size") != -1) {
							page = rs.getString(1) + rs.getString(2);
						} else if (rs.getString(1).indexOf("Other Versions") != -1) {
							otherVersionSize = rs.getString(2);
						} else if (rs.getString(1).indexOf("Last Backup Time") != -1) {
							backupTime = rs.getString(2);
						}
					}
				} finally {
					rs.close();
				}
			}
		} catch (final Exception e) {
			log.error("Exception", e);
			databaseInfo.setStatus(MonitorInformation.BULB_RED);
			String dName = null;
			if (dataSourceName.equals(MonitorInformation.IQ_NAME_STATUS_REP)) {
				context.put("isRepDBOk", false);
				dName = ENIQServiceStatusInfo.getRepDBName();
				if (getRealTimeDBServiceStatus(MonitorInformation.repDBServiceName)) {
					// repDB service is UP and running
					databaseInfo.setStatus(MonitorInformation.BULB_GRAY);
					if (ENIQServiceStatusInfo.isRepDBConnLimitExceeded()) {
						final String message = "<br/>  Reason: Connection limit exceeds.";
						databaseInfo
								.setAllInfo("<font color='red' face='verdana' size='1'>Unable to connect to database '"
										+ dName + "'</font>" + message, true);
					} else {
						databaseInfo
								.setAllInfo("<font color='red' face='verdana' size='1'>Unable to connect to database '"
										+ dName + "'</font>", true);
					}
				} else {
					// repDB service is down or not pingable
					databaseInfo.setAllInfo(
							"<font color='red' face='verdana' size='2'><b>Failed to initialise database connection to '"
									+ dName + "'</b></font>",
							true);
				}
			} else if (dataSourceName.equals(MonitorInformation.IQ_NAME_STATUS_DWH)) {
				dName = ENIQServiceStatusInfo.getDwhDBName();
				if (getRealTimeDBServiceStatus(MonitorInformation.dwhDBServiceName)) {
					// dwhdb service is UP and running
					databaseInfo.setStatus(MonitorInformation.BULB_GRAY);
					if (ENIQServiceStatusInfo.isDwhDBConnLimitExceeded()) {
						final String message = "<br/>  Reason: Connection limit exceeds.";
						databaseInfo
								.setAllInfo("<font color='red' face='verdana' size='1'>Unable to connect to database '"
										+ dName + "'</font>" + message, true);
					} else {
						databaseInfo
								.setAllInfo("<font color='red' face='verdana' size='1'>Unable to connect to database '"
										+ dName + "'</font>", true);
					}
				} else {
					// dwhdb service is down or not pingable
					databaseInfo.setAllInfo(
							"<font color='red' face='verdana' size='2'><b>Failed to initialise database connection to '"
									+ dName + "'</b></font>",
							true);
				}
			}
			errorOccured = true;
		} finally {
			try {
				if (statement != null) {
					statement.close();
				}
			} catch (final SQLException e) {
				log.error("Exception", e);
			}

		}

		if (!errorOccured) {
			if (dataSourceName.equals(MonitorInformation.IQ_NAME_STATUS_REP)) {
				databaseInfo.new_parseSQLAnywhereSize(version, freeSpace, spaceUsed, backupTime);
			} else {
				databaseInfo.parseSize(mainIq, page, version, otherVersionSize, backupTime);
			}
			checkWarnings(databaseInfo);
		}

		if (!errorOccured && !databaseInfo.isWarning()) {
			databaseInfo.setStatus(MonitorInformation.BULB_GREEN);
		} else if (databaseInfo.isWarning()) {
			databaseInfo.setStatus(MonitorInformation.BULB_YELLOW);
		}

		if ((mainIq != null && mainIq.equals("")) || databaseInfo.isDBFull()) {
			databaseInfo.clearWarnings();
			if (databaseInfo.isDBFull()) {
				databaseInfo.setWarning(true, "- Database is 90% or more full -");
			}
			databaseInfo.setStatus(MonitorInformation.BULB_RED);
		}

		return databaseInfo;
	}

	/**
	 * For future use if we want to check whether the database server with given
	 * name is Pingable and running
	 * 
	 * @param name
	 * @return
	 */
	private boolean getRealTimeDBServiceStatus(final String name) {
		log.info("Starting to get real time Service Status for DB; " + name);
		boolean isRunning = true;
		final SystemCommand systemCommand = new SystemCommand();
		try {
			final String cmd = MonitorInformation.CMD_PING_STATUS + " " + name;
			log.info("Going to run command: " + cmd);
			final String pingOutput = systemCommand.runCmdPlain(cmd);
			log.info("Output after running command is: " + pingOutput);
			if (pingOutput.contains(MonitorInformation.CMD_PING_STATUS_RESULT_VALID)) {
				isRunning = true;
				log.info("Database: " + name + " is pingable.");
				// check the status
				final String statusCmd = name + " status";
				log.info("Going to run command: " + statusCmd);
				final String statusOutput = systemCommand.runCmdPlain(statusCmd);
				log.info("Output after running command is: " + statusOutput);
				if (statusOutput.contains(MonitorInformation.CMD_DB_STATUS_RESULT_VALID)) {
					log.info("Database: " + name + " is running Ok");
					isRunning = true;
				} else {
					log.info("Database: " + name + " is not running");
					isRunning = false;
				}
			} else {
				log.info("Database: " + name + " is not pingable.");
				isRunning = false;
			}
		} catch (final Exception e) {
			log.info("Exception comes while getting real time Service Status of database: " + name
					+ " Exception message: " + e.getMessage());
			isRunning = false;
		}
		log.info("Stoping to get real time Service Status for DB; " + name);
		return isRunning;
	}

	/**
	 * Checks warnings from <code>DatabaseInfo</code> class. Warning text and
	 * state are added if an warning exists.<br>
	 * Warnings are based on parametrisized info from Web DD.
	 * 
	 * @param databaseInfo
	 *            is an instance of DatabaseInfo containing database's info.
	 */
	private void checkWarnings(final DatabaseInfo databaseInfo) {
		// Other version is percentage of other version size
		final int versionSize = Helper.getEnvEntryInt("intOtherVersions");
		// backup delay is in days
		final int backupDelay = Helper.getEnvEntryInt("intBackupDelay");

		if (databaseInfo.getBackuptime() == null) {
			databaseInfo.setWarning(true, "- Backup has not been taken. -");
		} else if (databaseInfo.getBackuptime().trim().equals("")) {
			databaseInfo.setWarning(true, "- Backup has not been taken. -");
		} else {

			final Calendar reversed = new GregorianCalendar();
			reversed.add(Calendar.DATE, -backupDelay); // Note the minus sign,
			// it's
			// not a typo.

			final DateFormatter dateFormatter = new DateFormatter("yyyy-MM-dd HH:mm");
			dateFormatter.setCalendar(databaseInfo.getBackuptime());

			final Calendar backupTime = new GregorianCalendar(dateFormatter.getCurrentYear(),
					dateFormatter.getCurrentMonth(), dateFormatter.getCurrentDate(), dateFormatter.getCurrentHour(),
					dateFormatter.getCurrentMinute());

			log.debug("Database backup time: " + backupTime.getTime().toString());
			log.debug("Reversed time: " + reversed.getTime().toString());
			log.debug("Backupdelay in days: " + backupDelay);
			log.debug("Difference in days: "
					+ (reversed.getTimeInMillis() - backupTime.getTimeInMillis()) / (1000 * 60 * 60 * 24));

			if (backupTime.before(reversed)) {
				databaseInfo.setWarning(true, "- Backup has not been taken in " + backupDelay + " days. -");
			}
		}
		final float dbsize = ((float) Helper.getEnvEntryInt("intDbSize") / (float) 100);

		log.debug("Other version--" + ((float) versionSize / (float) 100) * databaseInfo.getOtherVersionSize());
		log.debug("totalsize--" + databaseInfo.getTotalSizeIntMB());
		if ((((float) versionSize / (float) 100) * databaseInfo.getOtherVersionSize()) >= (databaseInfo
				.getTotalSizeIntMB())) {
			databaseInfo.setWarning(true, "- Other versions limit exceeded -");
		}
		if ((dbsize * databaseInfo.getTotalSizeIntMB()) < databaseInfo.getUsedSpace()) {
			databaseInfo.setWarning(true, "- Database low on disk space -");
		}

	}
	
	
	
	private MonitorInformation getStatusMonitorForEngine(final String service, final String showDetails,
			final Environment environment) {
		ITransferEngineRMI transferEngineRmi;
		MonitorInformation monitorInformation = new MonitorInformation();
		try {
			List<String> engineUrls = com.distocraft.dc5000.common.RmiUrlFactory.getInstance().getAllEngineRmiUrls();
			log.info("Engine Urls :" + engineUrls);
			//String consolidatedMem = "";
			int prevStatus = MonitorInformation.BULB_GRAY;
			int currentStatus = MonitorInformation.BULB_GRAY;
			monitorInformation.setFieldName(MonitorInformation.TITLE_ETLENGINE);
			ENIQServiceStatusInfo.setEngineHealth(ENIQServiceStatusInfo.ServiceHealth.Online);
			for (String engineUrl : engineUrls) {
				try {
					log.info("Engine status result.");
					transferEngineRmi = (ITransferEngineRMI) Naming.lookup(engineUrl);
					final List<String> status = transferEngineRmi.status();
					for (String s : status) {
						log.info(s);
					}
					//monitorInformation.setCurrProfile((status.get(13)));
					if ("NoLoads".equalsIgnoreCase(EngineStatusDetails.getCurrentProfileName(status.get(13)))) {
						currentStatus = MonitorInformation.BULB_YELLOW;
					} else if ((status.get(7)).trim().equals(MonitorInformation.STATUS_ENGINE_STRING_OK)) {
						currentStatus = MonitorInformation.BULB_GREEN;
					} else {
						currentStatus = MonitorInformation.BULB_YELLOW;
					}
					prevStatus = EngineStatusDetails.getConsolidatedStatus(currentStatus, prevStatus);
					//monitorInformation.setTotMem(
							//"Total Memory: " + MonitorInformation.transformBytesToMegas(status.get(14)) + " MB.");
					//monitorInformation.setSize("Priority queue " + (status.get(5)).toLowerCase());
					final Iterator<String> iterator = status.iterator();

					while (iterator.hasNext()) {
						monitorInformation.setMessage((iterator.next()) + "<br />");
					}
				}

				catch (Exception e) {
					log.error(" Exception while getting status ", e);
					currentStatus = MonitorInformation.BULB_RED;
					prevStatus = EngineStatusDetails.getConsolidatedStatus(currentStatus, prevStatus);
				}
			}
			if (monitorInformation.getMessage() != null && !monitorInformation.getMessage().isEmpty() ) {
				monitorInformation.setStatus(prevStatus);
				if (showDetails != null && !showDetails.equals("")) {
					monitorInformation.setIsDetails(true);
				}
			} else {
				log.info("Engine is not running.");
				monitorInformation.setStatus(MonitorInformation.BULB_RED);
				monitorInformation.setFieldName(MonitorInformation.TITLE_ETLENGINE);
				monitorInformation.setEtlcServerStatus(MonitorInformation.ENGINE_NOT_RUNNING);
				ENIQServiceStatusInfo.setEngineHealth(ENIQServiceStatusInfo.ServiceHealth.Offline);
				checkEngineLicenseInfo(environment, monitorInformation);
			}
			
			
			

		} catch (final Exception e) {
			log.info("Engine is not running.");
			log.error("Exception ", e);
			monitorInformation.setStatus(MonitorInformation.BULB_RED);
			monitorInformation.setFieldName(MonitorInformation.TITLE_ETLENGINE);
			monitorInformation.setEtlcServerStatus(MonitorInformation.ENGINE_NOT_RUNNING);
			ENIQServiceStatusInfo.setEngineHealth(ENIQServiceStatusInfo.ServiceHealth.Offline);
			checkEngineLicenseInfo(environment, monitorInformation);
			// Engine offline
		}
		return monitorInformation;
	}

	/**
	 * Receives status monitor information of ETLC engine and scheduler. Both
	 * services are used as RMI calls. ResultSet resultsetbackuptime = null;
	 * resultsetbackuptime = statement.executeQuery(
	 * "select last_time from SYS.SYSHISTORY where operation = 'LAST_BACKUP'");
	 * while (resultsetbackuptime.next()){
	 * 
	 * backupTime = resultsetbackuptime.getString("last_time"); }
	 * 
	 * if(resultsetbackuptime != null){ resultsetbackuptime.close(); }
	 * 
	 * @param service
	 *            - service name either TransferEngine or Scheduler
	 * @param showDetails
	 *            - parameter to show if details are shown or not
	 * @return Returns an instance of MonitorInformation instance with info of
	 *         the service.
	 */
	MonitorInformation getStatusMonitor(final String service, final String showDetails, final Environment environment) { // NOPMD
												// by
												// eheijun
												// on
		// 03/06/11 08:22
		final MonitorInformation monitorInformation = new MonitorInformation();
		if (service.equals("TransferEngine")) {
			ITransferEngineRMI transferEngineRmi;
			try {
				transferEngineRmi = (ITransferEngineRMI) rmiConnect("engine", service);
				final List<String> status = transferEngineRmi.status();
				log.info("Engine status result.");
				for (String s : status) {
					log.info(s);
				}

				monitorInformation.setFieldName(MonitorInformation.TITLE_ETLENGINE);
				// Refer HT94444 : Engine Current Profile was not dispalying
				// properly
				monitorInformation.setCurrProfile((status.get(10)));
				ENIQServiceStatusInfo.setEngineHealth(ENIQServiceStatusInfo.ServiceHealth.Online);
				if ("NoLoads".equalsIgnoreCase(monitorInformation.getCurrProfileName())) {
					monitorInformation.setStatus(MonitorInformation.BULB_YELLOW);
				} else if ((status.get(4)).trim().equals(MonitorInformation.STATUS_ENGINE_STRING_OK)) {
					monitorInformation.setStatus(MonitorInformation.BULB_GREEN);
				} else {
					monitorInformation.setStatus(MonitorInformation.BULB_YELLOW);
				}
				monitorInformation.setUptime((status.get(1)));
				monitorInformation.setTotMem(
						"Total Memory: " + MonitorInformation.transformBytesToMegas(status.get(14)) + " MB.");
				monitorInformation.setSize("Priority queue " + (status.get(5)).toLowerCase());

				final Iterator<String> iterator = status.iterator();

				while (iterator.hasNext()) {
					monitorInformation.setMessage((iterator.next()) + "<br />");
				}
				if (showDetails != null && !showDetails.equals("")) {
					monitorInformation.setIsDetails(true);
				}
			} catch (RMIConnectException e) {
				log.info("Engine is not running.");
				log.error("Exception ", e);
				monitorInformation.setStatus(MonitorInformation.BULB_RED);
				monitorInformation.setFieldName(MonitorInformation.TITLE_ETLENGINE);
				monitorInformation.setEtlcServerStatus(MonitorInformation.ENGINE_NOT_RUNNING);
				checkEngineLicenseInfo(environment, monitorInformation);
				ENIQServiceStatusInfo.setEngineHealth(ENIQServiceStatusInfo.ServiceHealth.Offline);
			} catch (final Exception e) {
				log.info("Engine is not running.");
				log.error("Exception ", e);
				monitorInformation.setStatus(MonitorInformation.BULB_RED);
				monitorInformation.setFieldName(MonitorInformation.TITLE_ETLENGINE);
				monitorInformation.setEtlcServerStatus(MonitorInformation.ENGINE_NOT_RUNNING);
				ENIQServiceStatusInfo.setEngineHealth(ENIQServiceStatusInfo.ServiceHealth.Offline);
				checkEngineLicenseInfo(environment, monitorInformation);
				// Engine offline
			}
			return monitorInformation;
		} else if (service.equals("RollingSnapshot")) {
			monitorInformation.setFieldName(MonitorInformation.TITLE_ROLLING_SNAPSHOT);
			readRaiseAlarmStatus(monitorInformation, new File("//eniq//admin//etc//roll_snap_alarm"));
			return monitorInformation;
		} else if (service.equals("OMBSBackup")) {
			monitorInformation.setFieldName(MonitorInformation.TITLE_OMBS_BACKUP);
			readRaiseAlarmStatus(monitorInformation, new File("//eniq//admin//etc//ombs_backup_alarm"));
			return monitorInformation;
			
		/* FRH code to show FRH tab 
		 } else if (service.equals("frh")) {
			String frhServerIp = null;
			String frhPassword = null;
			String uptime = null;
			try {
				frhServerIp = getFrhServerIP();
				if (frhServerIp != null) {
					boolean isLicenseValid = isFrhLicenseValid();
					if (isLicenseValid) {
						final long currentTime = System.currentTimeMillis();
						log.debug("Current time is : " + currentTime);
						long controllerStartTime = 0;

						final File controllerStartTimeFile = new File(CONTROLLERSTARTTIME_FILEPATH);
						if (controllerStartTimeFile.exists() && !controllerStartTimeFile.isDirectory()
								&& controllerStartTimeFile.length() != 0) {
							log.debug("Controller_startTime file exists");

							BufferedReader br = null;
							try {
								br = new BufferedReader(new FileReader(controllerStartTimeFile));
								String getControllerStartTime = null;
								getControllerStartTime = br.readLine();
								controllerStartTime = Long.parseLong(getControllerStartTime);
								log.debug("Controller Start time is : " + controllerStartTime);
							} catch (final FileNotFoundException e1) {
								log.error("Controller start time file not found " + e1.getMessage());
							} catch (final IOException e) {
								log.error("IO Exception while reading controller_startTime file " + e.getMessage());
							} catch (final Exception e) {
								log.error("Exception while reading controller_startTime file " + e.getMessage());
							} finally {
								try {
									br.close();
								} catch (final IOException e) {
									log.error("Exception while closing the bufferedReader " + e.getMessage());
								}
							}
							// uptime in millisecond
							long calculatedUptime = currentTime - controllerStartTime;

							log.debug("The calculated uptime is : " + calculatedUptime);
							final long days = calculatedUptime / (1000 * 60 * 60 * 24);
							calculatedUptime -= (days * (1000 * 60 * 60 * 24));
							final long hours = calculatedUptime / (1000 * 60 * 60);
							if (days == 0 && hours == 0) {
								final long mins = calculatedUptime / (1000 * 60);
								uptime = "Uptime: " + days + " days " + hours + " hours " + mins + " mins";
							} else {
								uptime = "Uptime: " + days + " days " + hours + " hours";
							}
							log.debug("Uptime of controller is : " + uptime);
						 
						try {
							final String commandString = "bash /ericsson/frh/controller/bin/frh_service.sh status";
							List<Meta_databases> mdList = DBUsersGet.getMetaDatabases(FRHUSER, SERVICE_NAME);
							if (mdList.isEmpty()) {
								throw new Exception(
										"Could not find an entry for " + FRHUSER + ":" + SERVICE_NAME + " in FRH! ");
							}
							frhPassword = mdList.get(0).getPassword();
							if (frhPassword != null) {
								final String frhSystemStatus = RemoteExecutor.executeComand(FRHUSER, frhPassword,
										frhServerIp, commandString);
								log.info("Frh System Status is:" + frhSystemStatus);

								if (frhSystemStatus.trim().equalsIgnoreCase(FRH_OPERATIONAL)) {
									monitorInformation.setStatus(MonitorInformation.BULB_GREEN);
									monitorInformation.setUptime(uptime);

								} else if (frhSystemStatus.trim().equalsIgnoreCase(FRH_DEGRADED)) {
									monitorInformation.setStatus(MonitorInformation.BULB_YELLOW);
									monitorInformation.setUptime(uptime);
								} else if (frhSystemStatus.trim().equalsIgnoreCase(FRH_NONOPERATIONAL)) {
									monitorInformation.setStatus(MonitorInformation.BULB_RED);

								}
								if (showDetails != null && !showDetails.equals("")) {
									monitorInformation.setIsDetails(true);
								}
							}
						} catch (final JSchException e) {
							log.error("Jsch Exception in frh system status " + e.getMessage());

						} catch (final Exception e) {
							log.error("Exception in frh system status" + e.getMessage());
						}
						}
						else {
							monitorInformation.setEtlcServerStatus(MonitorInformation.CONTROLLER_NOT_RUNNING);
						}
						
					} else {
						monitorInformation.setStatus(MonitorInformation.BULB_GRAY);
						monitorInformation.setEtlcServerStatus(MonitorInformation.LICENSE_NOT_INSTALLED);
						if (showDetails != null && !showDetails.equals("")) {
							monitorInformation.setIsDetails(true);
						}
					}
				}
			} catch (final IOException e2) {
				log.error("IOException occurred in LoaderStatus : " + e2.getMessage());
			} catch (InterruptedException e) {
				log.error("InterruptedException while fetching frh server IP: " + e.getMessage());
			} catch (Exception e) {
				log.error(e.getMessage());
			}*/
			
		} else if (service.equals("Scheduler")) {
			ISchedulerRMI schedulerRmi;
			try {
				schedulerRmi = (ISchedulerRMI) rmiConnect("scheduler", service);
				final List<String> status = schedulerRmi.status();
				monitorInformation.setFieldName(MonitorInformation.TITLE_ETLSCHEDULER);
				ENIQServiceStatusInfo.setSchedulerHealth(ENIQServiceStatusInfo.ServiceHealth.Online);
				if ("NoLoads".equalsIgnoreCase(monitorInformation.getCurrProfileName())) {
					monitorInformation.setStatus(MonitorInformation.BULB_YELLOW);
				} else if ((status.get(1)).trim().equals(MonitorInformation.STATUS_SCHEDULER_STRING_OK)) {
					monitorInformation.setStatus(MonitorInformation.BULB_GREEN);
				} else {
					monitorInformation.setStatus(MonitorInformation.BULB_YELLOW);
				}
				monitorInformation.setPollInterval((status.get(2)));
				final Iterator<String> iterator = status.iterator();
				while (iterator.hasNext()) {
					monitorInformation.setMessage(iterator.next() + "<br />");
				}
				if (showDetails != null && !showDetails.equals("")) {
					monitorInformation.setIsDetails(true);
				}
			} catch (RMIConnectException e) {
				monitorInformation.setStatus(MonitorInformation.BULB_RED);
				monitorInformation.setFieldName(MonitorInformation.TITLE_ETLSCHEDULER);
				monitorInformation.setEtlcServerStatus(MonitorInformation.SCHEDULER_NOT_RUNNING);
				ENIQServiceStatusInfo.setSchedulerHealth(ENIQServiceStatusInfo.ServiceHealth.Offline);
				log.error("Exception ", e);
			} catch (final Exception e) {
				monitorInformation.setStatus(MonitorInformation.BULB_RED);
				monitorInformation.setFieldName(MonitorInformation.TITLE_ETLSCHEDULER);
				monitorInformation.setEtlcServerStatus(MonitorInformation.SCHEDULER_NOT_RUNNING);
				ENIQServiceStatusInfo.setSchedulerHealth(ENIQServiceStatusInfo.ServiceHealth.Offline);
				log.error("Exception ", e);
			}
		} else if (service.equals("lwphelper")) {
			try {
				final int registryPort = LwpServer.getRegistryPort();
				final String registryHost = LwpServer.getRegistryHost();
				final Registry registry = LocateRegistry.getRegistry(registryHost, registryPort);
				final ILWPHelperRMI helper = (ILWPHelperRMI) registry.lookup(service);
				helper.ping();
				monitorInformation.setFieldName(MonitorInformation.TITLE_LWPHELPER);
				monitorInformation.setStatus(MonitorInformation.BULB_GREEN);
				ENIQServiceStatusInfo.setLwpHelperHealth(ENIQServiceStatusInfo.ServiceHealth.Online);
				
			} catch (RemoteException e) {
				monitorInformation.setStatus(MonitorInformation.BULB_RED);
				monitorInformation.setFieldName(MonitorInformation.TITLE_LWPHELPER);
				ENIQServiceStatusInfo.setLwpHelperHealth(ENIQServiceStatusInfo.ServiceHealth.Offline);
				log.error("Exception ", e);
			} catch (final Exception e) {
				monitorInformation.setStatus(MonitorInformation.BULB_RED);
				monitorInformation.setFieldName(MonitorInformation.TITLE_LWPHELPER);
				ENIQServiceStatusInfo.setLwpHelperHealth(ENIQServiceStatusInfo.ServiceHealth.Offline);
				log.error("Exception ", e);
			}
		}  else if (service.equals("MultiESController")) {
			try {
				if (isFlsEnabled())
				{
					if(isFlsProfileOnHold())
					{
						monitorInformation.setStatus(MonitorInformation.BULB_YELLOW);
						ENIQServiceStatusInfo.setFlsHealth(ENIQServiceStatusInfo.ServiceHealth.StatusYellow);
					}
					else
					{
						monitorInformation.setStatus(MonitorInformation.BULB_GREEN);
						ENIQServiceStatusInfo.setFlsHealth(ENIQServiceStatusInfo.ServiceHealth.Online);
					}
				} else {
					monitorInformation.setStatus(MonitorInformation.BULB_RED);
					ENIQServiceStatusInfo.setFlsHealth(ENIQServiceStatusInfo.ServiceHealth.Offline);
					monitorInformation.setEtlcServerStatus(MonitorInformation.FLS_NOT_RUNNING);
				}

			}/*catch (RMIConnectException e) {
				monitorInformation.setStatus(MonitorInformation.BULB_RED);
				monitorInformation.setFieldName(MonitorInformation.TITLE_FLS);
				monitorInformation.setEtlcServerStatus(MonitorInformation.FLS_NOT_RUNNING);
				ENIQServiceStatusInfo.setFlsHealth(ENIQServiceStatusInfo.ServiceHealth.Offline);
				log.error("Exception ", e);
			}*/catch (final Exception e) {
				monitorInformation.setStatus(MonitorInformation.BULB_RED);
				monitorInformation.setFieldName(MonitorInformation.TITLE_FLS);
				ENIQServiceStatusInfo.setFlsHealth(ENIQServiceStatusInfo.ServiceHealth.Offline);
				log.error("Exception ", e);
			}
		} else if (service.equals("LicSrv")) {
			try {
				final LicensingCache licensingRmi = (LicensingCache) rmiConnect("licenceservice", service);
				final List<String> status = licensingRmi.sentinal_status();
				final StringBuilder sb = new StringBuilder();
				for (String line : status) {
					sb.append(line).append("\n");
				}
				final String statusOutput = sb.toString().trim();
				monitorInformation.setFieldName(MonitorInformation.TITLE_LICSERV);
				String formattedStatusOut = "";
				if (statusOutput.contains("is online")) {
					formattedStatusOut = statusOutput.replaceFirst("- ", "");
					formattedStatusOut = formattedStatusOut.replaceFirst(":", ":<br />");
					monitorInformation.setStatus(MonitorInformation.BULB_GREEN);
					ENIQServiceStatusInfo.setLicServerHealth(ENIQServiceStatusInfo.ServiceHealth.Online);
				} else {
					formattedStatusOut = "License server is not running.";
					monitorInformation.setStatus(MonitorInformation.BULB_RED);
					ENIQServiceStatusInfo.setLicServerHealth(ENIQServiceStatusInfo.ServiceHealth.Offline);
				}
				// Tidy up the output for HTML
				monitorInformation.setLicServerStatusMsg(formattedStatusOut);
			} catch (RMIConnectException e) {
				monitorInformation.setStatus(MonitorInformation.BULB_RED);
				monitorInformation.setFieldName(MonitorInformation.TITLE_LICSERV);
				ENIQServiceStatusInfo.setLicServerHealth(ENIQServiceStatusInfo.ServiceHealth.Offline);
				monitorInformation.setLicServerStatusMsg(
						"<font color='red' face='verdana' size='2'><b>Error while getting License server status.</b></font>");
				log.error("Exception ", e);
			} catch (final Exception e) {
				monitorInformation.setStatus(MonitorInformation.BULB_RED);
				monitorInformation.setFieldName(MonitorInformation.TITLE_LICSERV);
				ENIQServiceStatusInfo.setLicServerHealth(ENIQServiceStatusInfo.ServiceHealth.Offline);
				monitorInformation.setLicServerStatusMsg(
						"<font color='red' face='verdana' size='2'><b>Error while getting License server status.</b></font>");
				log.error("Exception ", e);
			}
		} else {
			try {
				final LicensingCache licensingRmi = (LicensingCache) rmiConnect("licenceservice", service);
				final List<String> status = licensingRmi.status();
				final StringBuilder sb = new StringBuilder();
				for (String line : status) {
					sb.append(line).append("\n");
				}
				final String statusOutput = sb.toString().trim();
				monitorInformation.setFieldName(MonitorInformation.TITLE_LICMGR);

				String formattedStatusOut = "";
				if (statusOutput.contains("is running")) {
					monitorInformation.setStatus(MonitorInformation.BULB_GREEN);
					ENIQServiceStatusInfo.setLicMangerHealth(ENIQServiceStatusInfo.ServiceHealth.Online);
					// Tidy up the output for HTML
					formattedStatusOut = "License manager is running OK";

				} else {
					monitorInformation.setStatus(MonitorInformation.BULB_RED);
					formattedStatusOut = "License manager is not running.";
					ENIQServiceStatusInfo.setLicMangerHealth(ENIQServiceStatusInfo.ServiceHealth.Offline);
				}
				monitorInformation.setLicManagerStatusMsg(formattedStatusOut);

			} catch (RMIConnectException e) {
				monitorInformation.setStatus(MonitorInformation.BULB_RED);
				monitorInformation.setFieldName(MonitorInformation.TITLE_LICMGR);
				ENIQServiceStatusInfo.setLicMangerHealth(ENIQServiceStatusInfo.ServiceHealth.Offline);
				monitorInformation.setLicServerStatusMsg(
						"<font color='red' face='verdana' size='2'><b>Error while getting License manager status.</b></font>");
				log.error("Exception ", e);
			} catch (final Exception e) {
				monitorInformation.setStatus(MonitorInformation.BULB_RED);
				monitorInformation.setFieldName(MonitorInformation.TITLE_LICMGR);
				ENIQServiceStatusInfo.setLicMangerHealth(ENIQServiceStatusInfo.ServiceHealth.Offline);
				monitorInformation.setLicServerStatusMsg(
						"<font color='red' face='verdana' size='2'><b>Error while getting License manager status.</b></font>");
				log.error("Exception ", e);
			}
		}

		return monitorInformation;
	}
	
	/* FRH helper methods
	 String getFrhServerIP() throws IOException, InterruptedException {
		String frhServerIp = null;
		// unix command to get frh server IP
		final String[] getFrhServerIp = { "/bin/sh", "-c", "cat /etc/hosts | grep -i frh | awk '{print $1}'" };
		// return process object
		final Process getFrhIPproc = Runtime.getRuntime().exec(getFrhServerIp);
		getFrhIPproc.waitFor();
		final BufferedReader reader = new BufferedReader(new InputStreamReader(getFrhIPproc.getInputStream()));
		String s = null;
		while ((s = reader.readLine()) != null) {
			frhServerIp = s;
		}
		log.debug("frhserver ip is " + frhServerIp);
		return frhServerIp;
	}

	boolean getFrhStatus() {
		boolean frhStatus = false;
		String frhServerIp = null;
		try {
			frhServerIp = getFrhServerIP();
			if (frhServerIp != null) {
				frhStatus = true;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			log.error(e.getMessage());
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			log.error(e.getMessage());
		}
		log.info(frhStatus);
		return frhStatus;

	}

	 
		public Boolean isFrhLicenseValid() {
				FRHLicenseCheck obj = FRHLicenseCheck.getInstance();
				try {
					if(!obj.checkFrhLicense())
					{
						return false;
					}
					else
					{
						return true;
					}
				} catch (final LicensingException licExp) {
					return false;
				} catch (final Exception e) {
					return false;
				}
			} */

	private void readRaiseAlarmStatus(MonitorInformation monitorInformation, File file) {
		if (file.exists()) {
			monitorInformation.setStatus(MonitorInformation.BULB_RED);
			BufferedReader br = null;
			try {
				br = new BufferedReader(new FileReader(file));
				String line = null;
				while ((line = br.readLine()) != null) {
					if (line.contains("Message")) {
						String msg = line.substring(line.indexOf("=") + 1, line.length());
						monitorInformation.clearAlarmStatus();
						monitorInformation.setAlarmStatus(msg);
					}
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		} else {
			monitorInformation.setStatus(MonitorInformation.BULB_GREEN);
			monitorInformation.clearAlarmStatus();
			if (file.getName().contains("roll_snap_alarm")) {
				monitorInformation.setAlarmStatus("Snapshot taken successfully");
			} else if (file.getName().contains("ombs_backup_alarm")) {
				monitorInformation.setAlarmStatus("OMBS backup taken!!");
			}
		}
	}

	private void checkEngineLicenseInfo(final Environment environment, final MonitorInformation monitorInformation) {
		final SystemCommand systemCommand = new SystemCommand();
		try {
			if (ENIQServiceStatusInfo.isLicManagerOffline()) {
				log.info("Failed to get License status for Engine as License Manager is down.");
				monitorInformation.setMessage("Failed to get License status for Engine as License Manager is down.");
				monitorInformation.setIsDetails(true);
				return;
			}
			if (Environment.Type.STATS.equals(environment.getType())) {
				final String statusOutput = systemCommand
						.runCmdPlain(MonitorInformation.STATS_CMD_STARTER_LICENSE_STATUS);
				if (!statusOutput.equalsIgnoreCase(MonitorInformation.STATS_CMD_STARTER_LICENSE_VALID)) {
					log.info("Starter license  " + MonitorInformation.STATS_STARTER_LICENSE + " has expired.");
					monitorInformation.setMessage(MonitorInformation.STATS_STARTER_LICENSE_EXPIRED);
					monitorInformation.setIsDetails(true);
				}
			} else if (Environment.Type.EVENTS.equals(environment.getType())
					|| Environment.Type.MIXED.equals(environment.getType())) {
				final String statusOutput = systemCommand
						.runCmdPlain(MonitorInformation.EVENTS_CMD_STARTER_LICENSE_STATUS);
				if (!statusOutput.equalsIgnoreCase(MonitorInformation.EVENTS_CMD_STARTER_LICENSE_VALID)) {
					log.info(statusOutput);
					log.info("Starter license CXC4012080 has expired.");
					monitorInformation.setMessage(MonitorInformation.EVENTS_STARTER_LICENSE_EXPIRED);
					monitorInformation.setIsDetails(true);
				}
			} else {
				log.info("Unknown ENIQ environment. License information not available.");
				monitorInformation.setMessage("");
				monitorInformation.setIsDetails(false);
			}
		} catch (final IOException ioe) {
			log.error("Exception ", ioe);
		}
	}

	/**
	 * Looks up and connect to the Transfer engine or scheduler.
	 * 
	 * @param service
	 *            - service name either TransferEngine or Scheduler
	 * @return javax.rmi.Remote
	 * @throws NotBoundException
	 * @throws RemoteException
	 * @throws MalformedURLException
	 */
	private Remote rmiConnect(final String serName, final String serviceRef) throws RMIConnectException {
		/*
		 * #ENGINE_HOSTNAME = localhost #ENGINE_PORT = 1200 #ENGINE_REFNAME =
		 * TransferEngine # #SCHEDULER_HOSTNAME = localhost #SCHEDULER_PORT =
		 * 1200 #SCHEDULER_REFNAME = Scheduler
		 */

		// 20111119 EANGUAN:: To get the hostname based upon the service name ::
		// change for SMF/HSS IP
		// String hostNameByServiceName = "localhost" ;//
		// try{//
		// hostNameByServiceName = ServicenamesHelper.getServiceHost(serName,
		// "localhost");//
		// }catch(final Exception e){//
		// hostNameByServiceName = "localhost" ;//
		// }//
		// final String rmiURL = "//" + hostNameByServiceName + ":" + rmiPort +
		// "/" + serviceRef;
		try {
			final String rmiString = Helper.getEnvEntryString("rmiurl");
			final String rmiURL = "//" + rmiString + "/" + serviceRef;
			Remote termi = null;
			if (serName.contains("engine")) {
				termi = (Remote) Naming.lookup(RmiUrlFactory.getInstance().getEngineRmiUrl());
			} else if (serName.contains("scheduler")) {
				termi = (Remote) Naming.lookup(RmiUrlFactory.getInstance().getSchedulerRmiUrl());
			} else if (serName.contains("licenceservice")) {
				termi = (Remote) Naming.lookup(RmiUrlFactory.getInstance().getLicmgrRmiUrl());
			} else if (serName.contains("fls")) {
				termi = (Remote) Naming.lookup(com.distocraft.dc5000.common.RmiUrlFactory.getInstance().getMultiESRmiUrl(EnmInterUtils.getEngineIP()));
			} else {
				termi = (Remote) Naming.lookup(rmiURL);
			}
			return termi;
		} catch (NotBoundException e) {
			throw new RMIConnectException("RMI connection fails to connect to the service ref: " + serviceRef
					+ "  Caused NotBoundException: " + e.getMessage());
		} catch (MalformedURLException e) {
			throw new RMIConnectException("RMI connection fails to connect to the service ref: " + serviceRef
					+ " Caused MalformedURLException: " + e.getMessage());
		} catch (RemoteException e) {
			throw new RMIConnectException("RMI connection fails to connect to the service ref: " + serviceRef
					+ " Caused RemoteException: " + e.getMessage());
		}

	}

	private MonitorInformation getCounterVolume(Context ctx) throws SQLException {
		Connection connDwh = null;
		MonitorInformation info = new MonitorInformation();
		if (ENIQServiceStatusInfo.isRepDBOnline() && ENIQServiceStatusInfo.isDwhDBOnline()) {

			connDwh = DatabaseConnections.getDwhDBConnection().getConnection();

			Statement statement = null;
			ResultSet result = null;
			DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			Date date = new Date();
			String currentDate = dateFormat.format(date);
			Calendar now = Calendar.getInstance();
			now.add(Calendar.DATE, -1);
			int dates = now.get(Calendar.DATE);
			int monthes = now.get(Calendar.MONTH) + 1;
			String d1, mon = null;
			if (dates < 10) {
				d1 = "0" + dates;
			} else {
				d1 = String.valueOf(dates);
			}

			if (monthes < 10) {
				mon = "0" + monthes;
			} else {
				mon = String.valueOf(monthes);
			}

			String previousDay = now.get(Calendar.YEAR) + "-" + mon + "-" + d1;
			log.info("Current Date:" + currentDate);
			ctx.put("previousDay", previousDay);
			// currentDate = "2015-04-10";
			double totalCounterVolume = 0L;
			long CVRawCount = 0, CVRANCount = 0, CVGRANCount = 0, CVLTECount = 0, CVTotalCount = 0;
			String[] values = getDateTime();
			try {
				
				statement = connDwh.createStatement();
				/*
				 * Counter volume is calculated for all ROPs and normalised to
				 * 15 min ROP. If the there's daily loading of bulk cm then the
				 * GSM,3G,4G counters are doubled else the actual value of these
				 * counters are added to total counter volume.
				 * 1million of SOEM data is equivalent to 8 million according to 
				 * FS document for 15min ROP interval data.
				 * 1440min(24Hr) ROP data has been converted to 15mins by multiplying
				 * sum of counter volume to 1/96.
				 */
				String sqlRawDataCount = "select sum(countersum) from ("
						+ "select sum(num_of_counters)/count(distinct rop_starttime) as countersum from log_session_adapter where ROP_STARTTIME >= '"
						+ values[95] + "' and ROP_STARTTIME < '" + values[0]
						+ "'  and datediff(minute,rop_starttime,rop_endtime) = 15 and SOURCE not in ('INTF_DC_E_BULK_CM','INTF_DIM_E_SOEM_MBH_ASCII','INTF_DC_E_ML_HC_E','INTF_DC_E_OPT1600_1200','INTF_DC_E_OPT_OMS3200','INTF_DC_E_OPT800_1400','INTF_DC_E_TNSPPT','INTF_DC_E_OPT_MHL3000') group by source union all "
						+ "select (sum(num_of_counters)/count(distinct rop_starttime))*8 as countersum from log_session_adapter where ROP_STARTTIME >= '"
						+ values[95] + "' and ROP_STARTTIME < '" + values[0]
						+ "'  and datediff(minute,rop_starttime,rop_endtime) = 15 and SOURCE in ('INTF_DIM_E_SOEM_MBH_ASCII','INTF_DC_E_ML_HC_E','INTF_DC_E_OPT1600_1200','INTF_DC_E_OPT_OMS3200','INTF_DC_E_OPT800_1400','INTF_DC_E_TNSPPT','INTF_DC_E_OPT_MHL3000') group by source union all "
						+ "select (sum(num_of_counters)/(count(distinct rop_starttime)/15))*1.25 as countersum from log_session_adapter where ROP_STARTTIME >= '"
						+ values[95] + "' and ROP_STARTTIME < '" + values[0]
						+ "'  and datediff(minute,rop_starttime,rop_endtime) = 1 and SOURCE != 'INTF_DC_E_BULK_CM' group by source union all "
						+ "select (sum(num_of_counters)/(count(distinct rop_starttime)/3))*1.25 as countersum from log_session_adapter where ROP_STARTTIME >= '"
						+ values[95] + "' and ROP_STARTTIME < '" + values[0]
						+ "'  and datediff(minute,rop_starttime,rop_endtime) = 5 and SOURCE not in ('INTF_DC_E_BULK_CM','INTF_DC_E_EVENTS_ERBS_ENM','INTF_DC_E_EVENTS_ERBS') group by source union all "
						+ "select sum(num_of_counters)/(count(distinct rop_starttime)/3) as countersum from log_session_adapter where ROP_STARTTIME >= '"
						+ values[95] + "' and ROP_STARTTIME < '" + values[0]
						+ "'  and datediff(minute,rop_starttime,rop_endtime) = 5 and SOURCE in ('INTF_DC_E_EVENTS_ERBS_ENM','INTF_DC_E_EVENTS_ERBS') group by source union all "
						+ "select (sum(num_of_counters)/(count(distinct rop_starttime)/0.5))*1.25 as countersum from log_session_adapter where ROP_STARTTIME >= '"
						+ values[95] + "' and ROP_STARTTIME < '" + values[0]
						+ "'  and datediff(minute,rop_starttime,rop_endtime) = 30 and SOURCE != 'INTF_DC_E_BULK_CM' group by source union all "
						+ "select (sum(num_of_counters)/(count(distinct rop_starttime)/0.25))*1.25 as countersum from log_session_adapter where ROP_STARTTIME >= '"
						+ values[95] + "' and ROP_STARTTIME < '" + values[0]
						+ "'  and datediff(minute,rop_starttime,rop_endtime) = 60 and SOURCE != 'INTF_DC_E_BULK_CM' group by source union all "
						+ "select (sum(num_of_counters)/(count(distinct rop_starttime)/0.04166))*1 as countersum from log_session_adapter where ROP_STARTTIME >= '"
						+ values[95] + "' and ROP_STARTTIME < '" + values[0]
						+ "'  and datediff(minute,rop_starttime,rop_endtime) = 360 and SOURCE != 'INTF_DC_E_BULK_CM' group by source union all "
						+ "select (sum(num_of_counters)/(count(distinct rop_starttime)/0.02083))*1 as countersum from log_session_adapter where ROP_STARTTIME >= '"
						+ values[95] + "' and ROP_STARTTIME < '" + values[0]
						+ "'  and datediff(minute,rop_starttime,rop_endtime) = 720 and SOURCE != 'INTF_DC_E_BULK_CM' group by source union all "
						+ "select (sum(num_of_counters)/(count(distinct rop_starttime)/0.010416))*8 as countersum from log_session_adapter where ROP_STARTTIME >= '"
						+ values[95] + "' and ROP_STARTTIME < '" + values[0]
						+ "'  and datediff(minute,rop_starttime,rop_endtime) = 1440 and SOURCE in ('INTF_DIM_E_SOEM_MBH_ASCII','INTF_DC_E_ML_HC_E','INTF_DC_E_OPT1600_1200','INTF_DC_E_OPT_OMS3200','INTF_DC_E_OPT800_1400','INTF_DC_E_TNSPPT','INTF_DC_E_OPT_MHL3000') group by source union all "
						+ "select (sum(num_of_counters)/(count(distinct rop_starttime)/0.010416))*1 as countersum from log_session_adapter where ROP_STARTTIME >= '"
						+ values[95] + "' and ROP_STARTTIME < '" + values[0]
						+ "'  and datediff(minute,rop_starttime,rop_endtime) = 1440 and SOURCE not in ('INTF_DIM_E_SOEM_MBH_ASCII','INTF_DC_E_ML_HC_E','INTF_DC_E_OPT1600_1200','INTF_DC_E_OPT_OMS3200','INTF_DC_E_OPT800_1400','INTF_DC_E_TNSPPT','INTF_DC_E_OPT_MHL3000','INTF_DC_E_BULK_CM') group by source) totalCounters";
										
						result = statement.executeQuery(sqlRawDataCount);
				if (result != null) {
							while (result.next()) {
							 CVRawCount = result.getLong(1);
							 log.info("Counter volume for RAW Data is : " + CVRawCount);
							}
						}
				String sqlGRAN = "if exists (select 1 from sysobjects where name='DIM_E_GRAN_CELL')"
						+ "select count(distinct CELL_ID )*1000000/3000 from DIM_E_GRAN_CELL where status = 'ACTIVE'";
						result = statement.executeQuery(sqlGRAN);
				if (result != null) {
							while (result.next()) {
							CVGRANCount = result.getLong(1);
							log.info("Counter volume for GSM is : " + CVGRANCount);
							}
						}
				String sqlRAN = "if exists (select 1 from sysobjects where name='DIM_E_RAN_UCELL')"
						+ "select count(distinct UCELL_ID)*1000000/3000 from DIM_E_RAN_UCELL where status = 'ACTIVE'";
					result = statement.executeQuery(sqlRAN);
				if (result != null) {
						while (result.next()) {
						 CVRANCount = result.getLong(1);
						 log.info("Counter volume for 3G is : " + CVRANCount);
						}		
					}
				String sqlLTE = "if exists (select 1 from sysobjects where name='DIM_E_LTE_EUCELL')"
						+ "select count(distinct EUtranCellId)*1000000/3000 from DIM_E_LTE_EUCELL where status = 'ACTIVE'";
							result = statement.executeQuery(sqlLTE);
				if (result != null) {
							while (result.next()) {
							CVLTECount = result.getLong(1);
							log.info("Counter volume for 4G is  : " + CVLTECount);
							}
						}
				String sqlBULKCM = "select count(*) from log_session_adapter where ROP_STARTTIME >= '" + values[95]
						+ "' and ROP_STARTTIME < '" + values[0] + "'  and SOURCE = 'INTF_DC_E_BULK_CM'";
							result = statement.executeQuery(sqlBULKCM);
				if (result != null) {
							while (result.next()) {
								int BULKCMDataCount = result.getInt(1);
								if (BULKCMDataCount == 0) {
									//CVTotalCount =  (CVRawCount + CVRANCount + CVGRANCount + CVLTECount);
									CVTotalCount = CVRawCount;
						} else {
							//CVTotalCount = (CVRawCount + ((CVRANCount + CVLTECount) * 2) + CVGRANCount);
							CVTotalCount = CVRawCount + (CVRANCount + CVLTECount) * 2;
								}
							}
						}
				totalCounterVolume = CVTotalCount / 1000000;
			} catch (SQLException se) {// TODO Auto-generated catch block
				log.warn("SQL Exception while fetching counter volume from the database " + se.getMessage());
			} catch (Exception e) {
				log.warn(" Exception while fetching counter volume " + e);
			} finally {
				try {
					if (result != null) {
					result.close();
				}
					if (statement != null) {
					statement.close();
				}
					if (connDwh != null) {
					connDwh.close();
				}
				} catch (Exception e) {
					log.warn("Error in closing the connection objects " + e.getMessage());
				}
			}
			log.info("CounterVolumes : " + totalCounterVolume);
			ctx.put("countVol", totalCounterVolume);
			int maxCounterVolume = checkSystem();
			// EQEV-43690
			//show max counter volume supported by the server
			ctx.put("maxCountVol", maxCounterVolume);
			// Fix for HU57014
			if (maxCounterVolume == 0) {
				info.setStatus(MonitorInformation.BULB_YELLOW);// WARNING
			} else if (totalCounterVolume > maxCounterVolume) {
				info.setStatus(MonitorInformation.BULB_RED);
			} else {
				info.setStatus(MonitorInformation.BULB_GREEN);
			}
		} else {
			ctx.put("countVol", 0.0);
			info.setStatus(MonitorInformation.BULB_YELLOW);
		}
		return info;
	}

	public int checkSystem() {
		boolean errorChecker = false;// Fix for HU57014
		try {

			String systemCommandString = "";
			String checkNoOfMainDb = "";
			final String user = "dcuser";
			String checkBlade = "raw";
			String getGenType="";
			String genTypeCommand=MonitorInformation.genTypeCommand;
			final String service_name = "scheduler";
			List<Meta_databases> mdList = DBUsersGet.getMetaDatabases(user, service_name);
			
			if (mdList.isEmpty()) {
				mdList = DBUsersGet.getMetaDatabases(user, service_name);
				if (mdList.isEmpty()) {
					throw new Exception(
							"Could not find an entry for " + user + ":" + service_name + " in engine! (was is added?)");
				}
			}
			final String password = mdList.get(0).getPassword();
			
			/*
			 * checkNoOfMainDb = "cat /eniq/installation/config/lun_map.ini |grep -i mainDB|wc -line";
			String checkMainDb = RemoteExecutor.executeComand(user, password, service_name, checkNoOfMainDb);
			checkMainDb = checkMainDb.trim();
			*/
			/*
			 * Check the deployment type of the server. Whether it's small ,
			 * medium or large
			 */
			systemCommandString = "cat /eniq/installation/config/extra_params/deployment";
			String deploymentType = RemoteExecutor.executeComand(user, password, service_name, systemCommandString);
			
			if (deploymentType.trim().equalsIgnoreCase("small")) {
				systemCommandString = MonitorInformation.SERVER_TYPE;
				checkBlade = RemoteExecutor.executeComand(user, password, service_name, systemCommandString);
				if (checkBlade.contains("raw")) {
					return 20;
				} else {
					//for compact rack
					getGenType=RemoteExecutor.executeComand(user, password, service_name,genTypeCommand );
					if(getGenType.toLowerCase().contains("gen8")) {
						return 5;
					}
					else if(getGenType.toLowerCase().contains("gen9")) {
						
						return 10;
					}
					else if(getGenType.toLowerCase().contains("gen10")){
						return 15;
					}
					
				}	
			} else if (deploymentType.trim().equalsIgnoreCase("medium")) {
			/**	// Fix for HU99159
				systemCommandString = "/usr/sbin/prtconf | grep 'Memory size:'";
				// top command was changed to prtconf command because top
				// command was failing in remoteExecutor method.
				String getBladeType = RemoteExecutor.executeComand(user, password, service_name, systemCommandString);
				String[] memVal = new String[5];
				int memInGb = 0;
				if (!getBladeType.isEmpty()) {
					memVal = getBladeType.split(" ");
					memInGb = Math.round((float) (Integer.parseInt(memVal[2].trim()) / 1024.0));
				}
				log.debug("Physical memory of server:" + memInGb + "GB [ " + getBladeType + " ]");
				if (memInGb == 512) {
					return 120;
				} else if (memInGb == 256) {
					return 80;
				} else {
					// check again???????
					log.debug("Setting default counter volume limit '20'");
					return 20;
				}**/
				systemCommandString = MonitorInformation.SERVER_TYPE;
				checkBlade = RemoteExecutor.executeComand(user, password, service_name, systemCommandString);
				if (!checkBlade.contains("raw")) {
					return 150;//cost effective rack
				} else {
					//EQEV-43254 ; returning cv as 80M irrespective of the RAM size.
					return 80; 
				}
			} 
			
			else if (deploymentType.trim().equalsIgnoreCase("large") || deploymentType.trim().equalsIgnoreCase("extralarge")) {
				int checkMainDb=0;
				final String query = "select count(*) as mainDbCount from sp_iqfile() where dbfilename like 'main%'";
				
				try(Connection dwhdbMainDB = DatabaseConnections.getDwhDBConnectionWithDBA().getConnection();
						Statement statement = dwhdbMainDB.createStatement();
						ResultSet rs = statement.executeQuery(query);
				) {
					log.info("Checking MainDB count");
					while (rs.next()) {
						checkMainDb = rs.getInt("mainDbCount");
						log.info("MainDB count is " + checkMainDb);
					}
				}
				catch(Exception e)
				{
				    log.info("Exception in getting the mainDB count. "+e);
				}
				counter_volume_file_exists = counter_volume_file.exists();
				
				if (deploymentType.trim().equalsIgnoreCase("large")) {
				systemCommandString = MonitorInformation.SERVER_TYPE;
				checkBlade = RemoteExecutor.executeComand(user, password, service_name, systemCommandString);
				
				    if (!checkBlade.contains("raw")) {
					    return 200; //Scenario NOT Applicable
				    } else {
					    if (checkMainDb < 21) {
						   if (counter_volume_file_exists) {
							   return 240;
						   } else {
							   return 160;
						   }
					    }

					    if (checkMainDb >= 21) {
						   if (counter_volume_file_exists) {
							   return 500;
						   } else {
							   return 320;
						   }
					    }
                     }
				}  else if (deploymentType.trim().equalsIgnoreCase("extralarge")) {
		              if (checkMainDb >= 33 && checkMainDb < 50){
			              if (counter_volume_file_exists) {
				              return 750;
			              } else {
				              return 500;
			              }
		              }
		  
		              if (checkMainDb == 50) {
			              if (counter_volume_file_exists) {
				              return 1000;
			              } else {
				              return 750;
			              }
		              }
	            }
		
		  } else {
				// check again???????
					return 5;
			}
		} catch (final JSchException e) {
			errorChecker = true;// Fix for HU57014
			// e.printStackTrace();
			String a = e.getMessage();
			if (a.contains("Auth fail")) {
     		System.out.println("UserId or password is wrong");
     	}
		} catch (final Exception e) {
			errorChecker = true;// Fix for HU57014
			e.printStackTrace();
		}
		// Fix for HU57014
		if (errorChecker) {
	    		log.info("Could not calculate the maximum Counter Volume. ");     
	    		return 0;
		} else {
	    		return 80;	    		
	    	}			
	 }

	public String[] getDateTime() {
		Calendar now = Calendar.getInstance();
		int minute = now.get(Calendar.MINUTE);
		int value = now.get(Calendar.MINUTE);
		;
		if (minute > 0 && minute < 15) {
			minute = 0;
		} else if (minute > 15 && minute < 30) {
			minute = 15;
		} else if (minute > 30 && minute < 45) {
			minute = 30;
		} else {
			minute = 45;
		}

		int newValue = value - minute;
		newValue = newValue + 15;
		now = Calendar.getInstance();
		now.add(Calendar.MINUTE, -newValue);
		String h = null;
		String m = null;
		String d, mo = null;
		String a[] = new String[98];
		for (int i = 0; i < 97; i++) {
			now.add(Calendar.MINUTE, -15);
			int hours = now.get(Calendar.HOUR_OF_DAY);
			if (hours < 10) {
				h = "0" + hours;
			} else {
				h = String.valueOf(hours);
			}

			int minutes = now.get(Calendar.MINUTE);
			if (minutes < 10) {
				m = "0" + minutes;
			} else {
				m = String.valueOf(minutes);
			}
			int days = now.get(Calendar.DATE);
			if (days < 10) {
				d = "0" + days;
			} else {
				d = String.valueOf(days);
			}

			int months = now.get(Calendar.MONTH) + 1;
			if (months < 10) {
				mo = "0" + months;
			} else {
				mo = String.valueOf(months);
			}

			a[i] = now.get(Calendar.YEAR) + "-" + mo + "-" + d + " " + h + ":" + m + ":" + "00";
		}
		return a;

	}

	public void applicationStatusForMultipleHost(final List<String> servicesForThisHost,
			final MonitorInformation monitorInformation, final Context context) {

		MonitorInformation glassfishInfo = (MonitorInformation) context.get(GLASSFISH_INFO);

		Iterator<String> service_Host = servicesForThisHost.iterator();
		while (service_Host.hasNext()) {
			String host_service_name = (String) service_Host.next();
			String host_service_temp = host_service_name.trim();
			if (host_service_temp.startsWith(MonitorInformation.EC)) {
				if (!ECStatus.ecIsRunning(host_service_temp)) {
					if (servicesForThisHost.size() == 1) {
						monitorInformation.setStatus(MonitorInformation.BULB_RED);
					} else {
						monitorInformation.setStatus(MonitorInformation.BULB_YELLOW);
					}
				}
			}
			
			if (host_service_temp.equals(MonitorInformation.Engine)) {

				if (ENIQServiceStatusInfo.isLdapOffline() || ENIQServiceStatusInfo.isEngineOffline()
						|| ENIQServiceStatusInfo.isSchedulerOffline() || ENIQServiceStatusInfo.isDwhDBOffline()
						|| ENIQServiceStatusInfo.isEtlDBOffline() || ENIQServiceStatusInfo.isLicManagerOffline()
						|| !ECStatus.ecIsRunning(MonitorInformation.CONTROL_ZONE_NAME)) {
					monitorInformation.setStatus(MonitorInformation.BULB_YELLOW);
				}
			}

			if (host_service_temp.startsWith(MonitorInformation.Dwh_reader)) {

				try {
					// serviceToReaderOrWriterMap would be empty for single
					// blade environment
					// check size first and only get status of node for
					// multiblade
					if (serviceToReaderOrWriterMap.size() > 0) {
						Map<String, String> typeStatusMap = serviceToReaderOrWriterMap.get(host_service_temp);
						for (String type : typeStatusMap.keySet()) {

							final String status = typeStatusMap.get(type);

							if (status.equalsIgnoreCase(MonitorInformation.Active_dwh)) {
								monitorInformation.setStatus(MonitorInformation.BULB_GREEN);
							}

							else {

								monitorInformation.setStatus(MonitorInformation.BULB_RED);
							}
						}
					}
				} catch (Exception e) {
					log.error("problem getting reader status for " + host_service_temp + "\n" + e);
				}
			}

			if (host_service_temp.equals(MonitorInformation.Glassfish)) {
				if (glassfishInfo.isGreen()) {
					monitorInformation.setStatus(MonitorInformation.BULB_GREEN);
				} else if (glassfishInfo.isRed()) {
					monitorInformation.setStatus(MonitorInformation.BULB_RED);
				} else {
					monitorInformation.setStatus(MonitorInformation.BULB_YELLOW);
				}
			}

		}

	}

	public void applicationStatusForSingleHost(final List<String> servicesForThisHost,
			final MonitorInformation monitorInformation, final Context context, final String host) {
		MonitorInformation glassfishInfo = (MonitorInformation) context.get(GLASSFISH_INFO);
		List<String> service_list = new ArrayList<String>();
		service_list = hostToServiceMap.get(host);
		Iterator<String> service_iterator = service_list.iterator();
		String serviceName;

		while (service_iterator.hasNext()) {
			serviceName = (String) service_iterator.next();

			if (serviceName.startsWith(MonitorInformation.EC) && !ECStatus.ecIsRunning(serviceName)) {
				monitorInformation.setStatus(MonitorInformation.BULB_YELLOW);
			}
		}

		if (!ECStatus.ecIsRunning(MonitorInformation.CONTROL_ZONE_NAME)) {
			monitorInformation.setStatus(MonitorInformation.BULB_YELLOW);
		}

		if (ENIQServiceStatusInfo.isLdapOffline() == true || ENIQServiceStatusInfo.isEngineOffline() == true
				|| ENIQServiceStatusInfo.isSchedulerOffline() == true || ENIQServiceStatusInfo.isDwhDBOffline() == true
				|| ENIQServiceStatusInfo.isEtlDBOffline() == true || ENIQServiceStatusInfo.isLicManagerOffline() == true
				|| glassfishInfo.isRed() == true) {
			monitorInformation.setStatus(MonitorInformation.BULB_YELLOW);
		}
	}

	public List<MonitorInformation> passwordDetails(Context context, Log log){

		final List<MonitorInformation> monitorInformations = new ArrayList<>();
		BufferedReader reader = null;
		Stream<String> lines = null;
		try {
			reader = new BufferedReader(new FileReader("/eniq/sw/conf/queryUserConf.cfg"));
			lines = reader.lines();
			boolean red = false;
			Iterator<String> itr = lines.iterator();
			itr.next();
			HashMap<String, Integer> users = new HashMap<>();
			while (itr.hasNext()) {
				String line = itr.next().trim();
				int alreadyExist = 0;
				if (!(line == null || (" ").equals(line) || ("").equals(line))) {
					String[] details = line.split("::");
					MonitorInformation monitorInformation = new MonitorInformation();
					String userName = details[0].trim();
					String expiry = details[6].trim();
					int passwordGraceTime = Integer.parseInt(details[7].trim());
					if (users.get(userName) == null) {
						users.put(userName, 1);
					} else {
						alreadyExist = 1;
					}
					if (alreadyExist == 0) {
						if (!(("0").equalsIgnoreCase(expiry))) {
							int year = Integer.parseInt(expiry.split("-")[0]);
							int month = Integer.parseInt(expiry.split("-")[1]);
							int days = Integer.parseInt(expiry.split("-")[2]);
							LocalDate actualExpiryDate = LocalDate.of(year, month, days).plusDays(7);
							LocalDate today = LocalDate.now();
							int daysToExpire = (int) Duration.between(today.atStartOfDay(), actualExpiryDate.atStartOfDay()).toDays();

							if (daysToExpire <= 0) {
								monitorInformation.setUsername(userName);
								monitorInformation.setStatus(MonitorInformation.BULB_RED);
								red = true;
								monitorInformations.add(monitorInformation);
							} else if (daysToExpire <= passwordGraceTime) {
								monitorInformation.setUsername(userName);
								monitorInformation.setStatus(MonitorInformation.BULB_YELLOW);
								monitorInformation.setActualExpiryDate(actualExpiryDate);
								monitorInformation.setDaysToExpire(daysToExpire);
								monitorInformations.add(monitorInformation);
								log.debug(monitorInformation.getUsername()+" Database user details: Actual Expiry Date- "+monitorInformation.getActualExpiryDate()+
								" Days To Expire- "+monitorInformation.getDaysToExpire());
							}
						}
					}
				}
			}
			log.debug("Customized Database users: "+users);
			if (monitorInformations.isEmpty()) {
				context.put("areUsersPresent", users.isEmpty());
				context.put("Empty", true);
			}
			if (red) {
				context.put("color", -1);
			} else {
				context.put("color", 1);
			}
		} catch (FileNotFoundException e) {
			context.put("errorMessage", true);
			log.warn("Database User information file not available: " +e);
		}finally {
			try {
				if(lines != null)
					lines.close();
				if(reader != null)
					reader.close();
			} catch (IOException e) {
				log.warn("Unable to close the Buffered Reader: "+ e);
			}
		}
		return monitorInformations;
	}
	
	// To display os users password expiry details in system status page if
	// password aging(node hardening optional feature) is applied
	private boolean osPasswordDetails(Context ctx) {
		String passDetails = "chage -l dcuser";
		String passDetailsRoot = "sudo chage -l root";
		String dcuserDetails = null;
		String rootDetails = null;
		String check = "never";
		try {
			dcuserDetails = RemoteExecutor.executeComandSshKey(APPLICATION_USER, HOST_ADD, passDetails);
			rootDetails = RemoteExecutor.executeComandSshKey(APPLICATION_USER, HOST_ADD, passDetailsRoot);
			List<String> dcuserArr = Arrays.asList(dcuserDetails.split("\n"));
			List<String> rootArr = Arrays.asList(rootDetails.split("\n"));
			Optional<String> dcuserOption = dcuserArr.stream().filter(s -> s.contains("Password expires")).findFirst();
			Optional<String> rootOption = rootArr.stream().filter(s -> s.contains("Password expires")).findFirst();
			String dcuserExpiryDate = getExpiryDate(dcuserOption);
			String rootExpiryDate = getExpiryDate(rootOption);
			log.info("dcuserExpiryDate:" + dcuserExpiryDate + "rootExpiryDate:" + rootExpiryDate);
			final List<MonitorInformation> monitorInformationList = new ArrayList<>();
			Optional<String> opdcuser = dcuserArr.stream().filter(s -> s.contains("Number of days of warning")).findFirst();
			Optional<String> oproot = rootArr.stream().filter(s -> s.contains("Number of days of warning")).findFirst();
			if ((dcuserExpiryDate.equals(check) && rootExpiryDate.equals(check))
					|| ("".equalsIgnoreCase(dcuserExpiryDate) || "".equalsIgnoreCase(rootExpiryDate))) {
				return false;
			} else {
				if (check.equalsIgnoreCase(dcuserExpiryDate)) {
					MonitorInformation miUser = getMonitorInformationObject("root", rootExpiryDate, oproot);
					ctx.put(PASS_AGING, miUser.getStatus());
					monitorInformationList.add(miUser);
				} else if (check.equalsIgnoreCase(rootExpiryDate)) {
					MonitorInformation miUser = getMonitorInformationObject("dcuser", dcuserExpiryDate, opdcuser);
					ctx.put(PASS_AGING, miUser.getStatus());
					monitorInformationList.add(miUser);
				} else {
					MonitorInformation miDcuser = getMonitorInformationObject("dcuser", dcuserExpiryDate, opdcuser);
					MonitorInformation miRoot = getMonitorInformationObject("root", rootExpiryDate, oproot);
					setPassAging(miDcuser.getStatus(), miRoot.getStatus(), ctx);
					log.info(PASS_AGING+"value is: "+ctx.get(PASS_AGING));
					monitorInformationList.add(miDcuser);
					monitorInformationList.add(miRoot);
				}
			}
			ctx.put("monitorInformationOSList", monitorInformationList);
		} catch (JSchException | IOException e) {
			log.warn("Unable to run chage command: " + e);
			return false;
		}
		return true;
	}

	private String getExpiryDate(Optional<String> userOption) {
		if(userOption.isPresent())
			return userOption.get().split(":")[1].trim();
		return "";
	}

	// Set password aging to show in the adminui if for both OS user password aging is applied
	private void setPassAging(int dcuserExpiryStatus, int rootExpiryStatus, Context ctx) {
		if(dcuserExpiryStatus== -1 || rootExpiryStatus ==  -1){
			ctx.put(PASS_AGING, -1);
		}else if(dcuserExpiryStatus== 0 || rootExpiryStatus ==  0){
			ctx.put(PASS_AGING, 0);
		}else{
			ctx.put(PASS_AGING, 1);
		}
	}
	// Get MonitorInformation Object
	private MonitorInformation getMonitorInformationObject(String userName, String expiryDate, Optional<String> daysBeforeWarning) {
		MonitorInformation miObject = new MonitorInformation();
		int warningDays = 0;
		LocalDate dateOfExpiry = getDate(expiryDate);
		if(daysBeforeWarning.isPresent())
			warningDays =  Integer.parseInt(daysBeforeWarning.get().split(":")[1].trim());
		LocalDate date = LocalDate.now();
		int daysToExpire = (int) Duration.between(date.atStartOfDay(), dateOfExpiry.atStartOfDay()).toDays();
		int result = daysToExpire - warningDays;
		if (result <= 0 && result >= -warningDays) {
			miObject.setStatus(0);
		} else if (result < -warningDays) {
			miObject.setStatus(-1);
		} else{
			miObject.setStatus(1);
		}
		miObject.setUsername(userName);
		miObject.setPassExpiryDate(expiryDate);
		log.info("Username= "+miObject.getUsername()+"ExpiryDate= "+miObject.getPassExpiryDate()+"Status= "+miObject.getStatus());
		return miObject;
	}
	
	// Convert string date into localDate object
	private LocalDate getDate(String date) {
		String[] monDateYear = date.split(",");
		String[] monDate = monDateYear[0].split(" ");
		String mon = monDate[0].trim();
		int dayOfMonth = Integer.parseInt(monDate[1].trim());
		int year = Integer.parseInt(monDateYear[1].trim());
		DateTimeFormatter format = DateTimeFormatter.ofPattern("MMM");
		TemporalAccessor in = format.withLocale(Locale.ENGLISH).parse(mon);
		int month = in.get(ChronoField.MONTH_OF_YEAR);
		return LocalDate.of(year, month, dayOfMonth);
	}
	private int certificateAlarm(Log log) throws Exception {
		int yellow = 0;
		CertificateExpiryDetails certificate = new CertificateExpiryDetails();
		List<CertificateInformation> certificateInformation = certificate.setCertificateInformation();
		for(CertificateInformation information : certificateInformation){
			if(("Red").equals(information.getColour())){
				return -1;
			}
			else if(("Yellow").equals(information.getColour())){
				yellow = 1;
			}
		}
		Map<String, List<CertificateInformation>> otherCertificateDetails = certificate.setOtherCerfiticateDetails();
		log.info("otherCertificateDetails:---------------"+otherCertificateDetails);
		for(Entry<String, List<CertificateInformation>> tableName : otherCertificateDetails.entrySet()) {
			List<CertificateInformation> otherCertificateInformation = tableName.getValue();
			for(CertificateInformation information : otherCertificateInformation){
				if(("Red").equals(information.getColour())){
					return -1;
				}
				else if(("Yellow").equals(information.getColour())){
					yellow = 1;
				}
			}
		}
		if(yellow == 1){
			return 0;
		}
		return 1;
	}
}
		