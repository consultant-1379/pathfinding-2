package com.distocraft.dc5000.common;

import com.ericsson.eniq.common.INIGet;
import com.ericsson.eniq.common.RemoteExecutor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.jcraft.jsch.JSchException;

import sun.rmi.runtime.Log;

/**
 * Helper class to figure out the network name to use for a servicename
 * Get the network address to use for a service base on the rules below:
 * <p/>
 * servicename defined in service_names file and hosts file --> return servicename
 * servicename not defined in service_names file but is defined in hosts file --> return servicename
 * <p/>
 * servicename defined in service_names file but not in hosts file:
 * (C) servicename hostname entry in service_names file is defined in hosts file --> return service_names servicename:hostname entry
 * (D) servicename hostname entry is not defined in hosts file --> return service_names servicename:ipaddress entry
 * <p/>
 * No servicename entry in service_names file or hosts file --> return defaultValue
 */

public class ServicenamesHelper {

	/**
	 * service_names regex matcher/splitter, Format is <ip_address>::<hostname>::<service>
	 */
	private static final String GROUP_REGEX = "(.*)::(.*)::(.*)";
	/**
	 * Splitter helper
	 */
	private static final Pattern splitter = Pattern.compile(GROUP_REGEX);
	/**
	 * Default instance
	 */
	private static final ServicenamesHelper helper = new ServicenamesHelper();
	/**
	 * Command to count CPU cores
	 */
	private static final String HOST_CORE_COUNT_COMMAND = "cat /proc/cpuinfo |grep "+"\"core id\"" + " | wc -l";
	/**
	 * Command to count CPU's
	 */
	private static final String HOST_CPU_COUNT_COMMAND = "cat /proc/cpuinfo |grep "+"\"physical id\"" + " | sort -u | wc -l";
	/**
	 * conf dir property name, defaults to CONF_DIR_DEFAULT
	 */
	private static final String CONF_DIR = "CONF_DIR";
	/**
	 * Default value for ${CONF_DIR}
	 */
	private static final String CONF_DIR_DEFAULT = "/eniq/sw/conf";
	/**
	 * DWH ini file name
	 */
	private static final String DWH_INI_FILENAME = "dwh.ini";
	/**
	 * NIQ ini file name
	 */
	private static final String NIQ_INI_FILENAME = "niq.ini";
	/**
	 * Block name for DWH_WRITER settings in the niq.ini file
	 */
	private static final String INI_DWH_WRITER = "DWH_WRITER";
	/**
	 * Block name for DWH_READER settings in the niq.ini file
	 */
	private static final String INI_DWH_READER = "DWH_READER";
	/**
	 * Block name for default DWH settings in the niq.ini file
	 */
	private static final String INI_DWH = "DWH";
	/**
	 * Service name key for DWH_READER and DWH_WRITER entries in the niq.ini file
	 */
	private static final String SERVICE_NAME = "Service_Name";
	/**
	 * Service name key for default DWH entry in the niq.ini file
	 */
	private static final String SERVER_NAME = "ServerName";
	/**
	 * Valid service names used with in Events and Stats. these are the names with 
	 * Single occurrence.
	 */
	public static final List<String> VALID_SO_SERVICENAMES = Collections.unmodifiableList(Arrays.asList
			("controlzone", "dwhdb", "engine", 
					"glassfish", "ldapserver", "licenceservice", 
					"repdb", "scheduler", "webserver")); //add "frh" in VALID_SO_SERVICENAMES for frhserver information fetching.
	/**
	 * Valid service names used with in Events and Stats. these are the names with
	 * multiple occurrences like ec_1, dwh_reader_1
	 */
	public static final List<String> VALID_MO_SERVICENAMES = Collections.unmodifiableList(Arrays.asList
			("dwh_reader_", "ec_", "ec_lteefa_",
					"ec_ltees_", "ec_sgeh_", "dwh_writer_","engine_"));
	
	private static final String MULTI_ENGINE_TAG = "engine";
	
	private static Map<String, ServiceHostDetails> configuredEngines;
	/**
	 * Same as * @see #getServiceHost(java.lang.String, java.lang.String) but the default value null <code>null</code>
	 *
	 * @param servicename The servicename to look up e.g. engine
	 * @return The network address the servicename process in associated with or <code>null</code> if nothing could be found
	 * @throws IOException Errors reading the hosts or service_names file
	 */
	public static String getServiceHost(final String servicename) throws IOException {
		return getServiceHost(servicename, null);
	}

	/**
	 * Get the network address to use for a service base on the rules below:
	 * [service_names file -> /eniq/sw/cong/service_names]
	 * [hosts -> /etc/hosts]
	 * <p/>
	 * (A) servicename defined in service_names file and hosts file --> return servicename
	 * (B) servicename not defined in service_names file but is defined in hosts file --> return servicename
	 * <p/>
	 * servicename defined in service_names file but not in hosts file:
	 * (C) servicename hostname entry in service_names file is defined in hosts file --> return service_names servicename:hostname entry
	 * (D) servicename hostname entry is not defined in hosts file --> return service_names servicename:ipaddress entry
	 * <p/>
	 * (E) No servicename entry in service_names file or hosts file --> return defaultValue
	 *
	 * @param servicename  The servicename to look up e.g. engine
	 * @param defaultValue A default value to return in not network mapping can be found
	 * @return The network address the servicename process in associated with
	 *         or <code>defaultValue</code> if nothing could be found
	 * @throws IOException Errors reading the hosts or service_names file
	 */
	public static String getServiceHost(final String servicename, final String defaultValue) throws IOException {
		final Map<String, String> hosts = getHosts();
		final Map<String, ServiceHostDetails> services = getServiceDetails();
		if (services.containsKey(servicename)) {
			//The servicename is defined in the service_names file
			if (hosts.containsKey(servicename)) {
				// The service name is in the /etc/hosts file to can use the servicename
				/* (A) */
				return servicename;
			} else {
				//No servicename entry in the /etc/hosts file, get the hostname defined in the service_names file
				final ServiceHostDetails details = services.get(servicename);
				//Check if the host name is defined in the hosts file
				if (hosts.containsKey(details.getServiceHostname())) {
					/* (C) */ // Can use the hostname
					return details.getServiceHostname();
				} else {
					/* (D) */ // No hostname in the hosts file, use the ip address from the service_names entry and hope thats OK
					return details.getServiceIpAddress();
				}
			}
		} else {//Not in service_names (new service????)
			if (hosts.containsKey(servicename)) { // but it's defined in the /etc/hosts file
				/* (B) */
				return servicename;
			}
		}
		//Neither in service_names nor /etc/hosts, return default value
		/* (E) */
		return defaultValue;
	}

	/**
	 * Get the hosts file location, defaults to /etc/hosts
	 *
	 * @return host file
	 * @throws FileNotFoundException if the hosts file can't be found
	 */
	
	static File hostsFile;

	public static File getHostsFile() throws FileNotFoundException {
		final File file = new File(System.getProperty("ETC_DIR", "/etc"),
				"hosts");

		if (hostsFile == null) {
			hostsFile = new File(System.getProperty("ETC_DIR", "/etc"), "hosts");
			if (!file.exists()) {
				throw new FileNotFoundException(file.getPath());
			}
		}
		return hostsFile;
	}

	public static void setHostsFile(File hostsFile) {
		ServicenamesHelper.hostsFile = hostsFile;
	}

	/**
	 * Get the service_names file location, defaults to ${CONF_DIR}/service_names
	 *
	 * @return service_names file
	 * @throws FileNotFoundException if the service_names file can't be cound
	 */
	public static File getServicenamesFile() throws FileNotFoundException {
		final File file = new File(System.getProperty("CONF_DIR", "/eniq/sw/conf"),
				System.getProperty("service_names", "service_names"));
		if (!file.exists()) {
			throw new FileNotFoundException(file.getPath());
		}
		return file;
	}

	/**
	 * Get the hosts file contents, the map keys are usually ipaddress and the values are hostnames
	 *
	 * @return hosts file entries
	 * @throws IOException If the hosts file cant be read
	 */
	public static Map<String, String> getHosts() throws IOException {
		return helper.parseHosts();
	}

	/**
	 * Get the service_names file contents
	 *
	 * @return service_names entries
	 * @throws IOException If the service_names file cant be ready
	 */
	public static Map<String, ServiceHostDetails> getServiceDetails() throws IOException {
		return helper.parseServiceNames();
	}

	/**
	 * Parse the hosts file
	 *
	 * @return .
	 * @throws IOException If the file isnt found or cant be read
	 */
	private Map<String, String> parseHosts() throws IOException {
		final File hostsFile = getHostsFile();
		final BufferedReader reader = new BufferedReader(new FileReader(hostsFile));
		String line;
		final Map<String, String> hostsMappings = new HashMap<String, String>();
		try {
			while ((line = reader.readLine()) != null) {
				line = line.trim();
				if (line.startsWith("#") || line.startsWith("::1") || line.length() == 0) {
					continue;
				}
				final String[] tokens = line.split("\\s+");
				if (tokens.length >= 2) {
					final String ipAddress = tokens[0];
					for (int i = 1; i < tokens.length; i++) {
						final String hostname = tokens[i];
						//            if(hostsMappings.containsKey(hostname)){
						//              System.out.println("Warning, duplicate hostname entry '"+
						//                hostname+"' in " + hostsFile.getPath() + ", overwriting " + hostsMappings.get(hostname) +
						//                " with " + ipAddress);
						//            }
						hostsMappings.put(hostname, ipAddress);
					}
				}
			}
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				/*-*/
			}
		}
		return hostsMappings;
	}
	
	/**
	 * Returns the available engine instances.
	 * 
	 * @return
	 * @throws IOException
	 */
	public static Map<String, ServiceHostDetails> getEngines(Logger log) throws IOException {
		Map<String, ServiceHostDetails> services = helper.parseServiceNames();
		log.log(Level.INFO, "Services present :"+services);
		Map<String, ServiceHostDetails> result = services.entrySet()
		.stream()
		.filter(entry -> entry.getKey().startsWith(MULTI_ENGINE_TAG))
		.collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue));
		log.log(Level.INFO, "Engines present :"+result);
		return result;
	}
	
	public static synchronized String getServiceName(String service, String ipAddress, Logger log) throws IOException {
		if ("Engine".equalsIgnoreCase(service)) {
			if (configuredEngines == null) {
				configuredEngines = new ConcurrentHashMap<>();
				Map<String, ServiceHostDetails> services = helper.parseServiceNames();
				if (log != null) {
					log.log(Level.INFO, "Services present :" + services);
				}
				for (Map.Entry<String, ServiceHostDetails> entry : services.entrySet()) {
					if (entry.getKey().startsWith(MULTI_ENGINE_TAG)
							&& entry.getValue().getServiceIpAddress().equals(ipAddress)) {
						configuredEngines.put(entry.getValue().getServiceIpAddress(), entry.getValue());

					}
				}
			}
			return configuredEngines.get(ipAddress).getServiceName();
		}
		return "";
	}

	/**
	 * Parse the service_names file
	 *
	 * @return .
	 * @throws IOException If the file isnt found or cant be read
	 */
	private Map<String, ServiceHostDetails> parseServiceNames() throws IOException {
		final File serviceNamesFile = getServicenamesFile();
		final BufferedReader reader = new BufferedReader(new FileReader(serviceNamesFile));
		String line;
		final Map<String, ServiceHostDetails> services = new HashMap<>();
		try {
			while ((line = reader.readLine()) != null) {
				line = line.trim();
				if (line.startsWith("#") || line.length() == 0) {
					// comment|empty line...
					continue;
				}
				//System.out.println("Service Names : Line :" +line);
				final Matcher matcher = splitter.matcher(line);
				if (matcher.matches() && matcher.groupCount() == 3) {
					final ServiceHostDetails details = createServiceHostDetails(matcher.group(3), matcher.group(2), matcher.group(1));
					//System.out.println("Service Names : " +details.getServiceName()+ " is conforming ");
					if (validateServiceNames(details.getServiceName())){
						//System.out.println("Service Names : " +details.getServiceName()+ " is valid");
						services.put(details.getServiceName(), details);
					}
					/*if ((!details.getServiceName().toLowerCase().contains("wifi"))&&(!details.getServiceName().toLowerCase().contains("oss"))&&(!details.getServiceName().toLowerCase().contains("cep"))){
					services.put(details.getServiceName(), details);
					}*/
				}
			}
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				/*-*/
			}
		}
		return services;
	}
	/**
	 * validating the service names for ENIQ Events and Stats. The valid service list only includes 
	 * <p/>
	 * Single instance services :
	 * "controlzone", "dwhdb", "engine", "glassfish", "ldapserver", "licenceservice", "repdb", "scheduler", "webserver", "mediator"
	 * <p/>
	 * Multiple instance services
	 * "dwh_reader_1", "dwh_reader_2", "ec_1", "ec_2", "ec_lteefa_1", "ec_lteefa_2", "ec_ltees_1", "ec_ltees_2", "ec_sgeh_1", "ec_sgeh_2" etc
	 * <p/>
	 * If service name doesnt match this list it wont be added to the services map.
	 * This is to avoid the usage of service name file for other purpose like WIFI, OSS-Aliases, CEP etc.
	 * 
	 * @param serviceName
	 * @return
	 */
	public static boolean validateServiceNames(final String serviceName) {
		for (String sname : VALID_SO_SERVICENAMES) {
			if (serviceName.toLowerCase().matches(sname)) { //change to if (serviceName.toLowerCase().contains(sname)) for frh
				return true;
			}
		}

		for (String sname : VALID_MO_SERVICENAMES) {
			if (serviceName.toLowerCase().matches(sname+"[0-9]+")){
				return true;
			}
		}
		return false;
	}

	ServiceHostDetails createServiceHostDetails(final String serviceName, final String serviceHost, final String ipAddress){
		return new ServiceHostDetails(serviceName, serviceHost, ipAddress);
	}

	/**
	 * Get the core count for the service host
	 *
	 * @param hostDetails Host info
	 * @param username ssh username
	 * @return number of cores
	 * @throws IOException If unable to run remote command
	 */
	public int getServiceHostCoreCount(final ServiceHostDetails hostDetails, final String username) throws IOException {
		String cmdOutput;
		final String realHost = getServiceHost(hostDetails.getServiceName());
		try {
			cmdOutput = RemoteExecutor.executeComandSshKey(username, realHost, HOST_CORE_COUNT_COMMAND);
		} catch (JSchException e) {
			e.printStackTrace();
			cmdOutput = "null";
		}
		return Integer.parseInt(cmdOutput.trim());
	}


	/**
	 * Get the CPU count for the service host
	 *
	 * @param hostDetails Host info
	 * @param username ssh username
	 * @param password The password if the JVM is running on windows, not used on *nix
	 * @return number of cores
	 * @throws IOException If unable to run remote command
	 */
	public int getServiceHostCpuCount(final ServiceHostDetails hostDetails, final String username, final String password) throws IOException {
		String cmdOutput;
		final String realHost = getServiceHost(hostDetails.getServiceName());
		final String pathSeparator = System.getProperty("java.path.separator");
		try {
			if(":".equals(pathSeparator)) {
				cmdOutput = RemoteExecutor.executeComandSshKey(username, realHost, HOST_CPU_COUNT_COMMAND);
			} else {
				//running from IDE
				cmdOutput = RemoteExecutor.executeComand(username, password, realHost, HOST_CPU_COUNT_COMMAND);
			}
		} catch (JSchException e) {
			e.printStackTrace();
			cmdOutput = "-1";// Code Fix for TR HU38725
		}
		return Integer.parseInt(cmdOutput.trim());
	}

	/**
	 * Get a list of all the IQ writer nodes in the system.
	 * Looks in niq.ini for DWH_WRITER entries, if defined, get the service_names.
	 * If no DWH_WRITER block defined, look for DWH_READER block and use the first reader defined (usually dwh_reader_1)
	 * If no DWH_READER block defined, look for the default DWH servicename entry
	 *
	 * @return List of IQ writer nodes. Will always return at least one entry
	 * @throws IOException Errors reading ini files.
	 */
	public static List<String> getWriterNodes() throws IOException {
		return helper.getIqWriterNodes();
	}

	public static List<String> getReaderNodes() throws IOException {
		return helper.getIqReaderNodes();
	}

	private List<String> getIqReaderNodes() throws IOException {
		// First look for dwh.ini file. If it isn't found, fall back to niq.ini
		final File ini = getNiqIni();

		// Read the contents of the ini file and the writer and reader blocks
		final INIGet iniReader = new INIGet();
		final Map<String, Map<String, String>> iniConfig = iniReader.readIniFile(ini);

		final Map<String, String> ini_readers = iniConfig.get(INI_DWH_READER);
		final Map<String, String> ini_writers = iniConfig.get(INI_DWH_WRITER);


		final List<String> readerBlocks = new ArrayList<String>();
		if (null != ini_readers) {
			// ini_readers will be null if the block is not defined in the ini file
			readerBlocks.addAll(ini_readers.keySet());
		}

		final List<String> availableReaderNames = new ArrayList<String>();
		if(!readerBlocks.isEmpty() && (ini_writers == null || ini_writers.size() == 0)){
			// Have readers but no writers so skip the first reader as that's now a writer
			readerBlocks.remove(0);
		}
		for(String block : readerBlocks){
			final Map<String, String> readerInfo = iniConfig.get(block);
			if(readerInfo == null){
				throw new NoSuchElementException("No block ["+block+"] defined in " + ini.getPath());
			}
			final String readerName = readerInfo.get(SERVICE_NAME);
			if(readerName == null){
				throw new NoSuchElementException("No "+SERVER_NAME+" entry defined in block ["+block+"] in " + ini.getPath());
			}
			if(!availableReaderNames.contains(readerName)){
				availableReaderNames.add(readerName);
			}
		}

		if(availableReaderNames.isEmpty()){
			// No dwh_readers avaiable, default to dwhdb
			final Map<String, String> dwh = iniConfig.get(INI_DWH);
			if (dwh != null) {
				final String dwhName = dwh.get(SERVER_NAME);
				if (dwhName == null) {
					throw new NoSuchElementException("No "+SERVER_NAME+" entry defined in block ["+INI_DWH+"] in " + ini.getPath());
				}
				availableReaderNames.add(dwhName);
			} else {
				throw new NoSuchElementException("No ["+INI_DWH+"] block defined in " + ini.getPath());
			}
		}
		return availableReaderNames;
	}

	private List<String> getIqWriterNodes() throws IOException {

		// First look for dwh.ini file. If it isn't found, fall back to niq.ini
		final File ini = getNiqIni();

		// Read the contents of the ini file and the writer and reader blocks
		final INIGet iniReader = new INIGet();
		final Map<String, Map<String, String>> iniConfig = iniReader.readIniFile(ini);

		final Map<String, String> writers = iniConfig.get(INI_DWH_WRITER);
		final Map<String, String> readers = iniConfig.get(INI_DWH_READER);

		// build list of nodes
		final List<String> writerNodes = new ArrayList<String>();

		if (writers != null) {
			//Writers are defined, get their service_names
			for (String writerName : writers.keySet()) {
				final Map<String, String> writerSettings = iniConfig.get(writerName);
				if (writerSettings != null) {
					final String sName = writerSettings.get(SERVICE_NAME);
					if (sName != null) {
						writerNodes.add(sName);
					}
				}
			}
		}

		if(writerNodes.size()==0 && readers != null) {
			//No writers defined so get first reader node...
			final String dwh_reader_1 = readers.keySet().iterator().next();
			final Map<String, String> readerSettings = iniConfig.get(dwh_reader_1);
			if (readerSettings != null) {
				final String sName = readerSettings.get(SERVICE_NAME);
				if (sName != null) {
					writerNodes.add(sName);
				}
			}
		}

		if(writerNodes.size() == 0) {
			// No dwh_reader_X or dwh_writer_X entries found, default back to dwhdb
			//Get the DWH settings...
			final Map<String, String> dwh = iniConfig.get(INI_DWH);
			if (dwh != null) {
				final String dwhName = dwh.get(SERVER_NAME);
				if (dwhName != null) {
					writerNodes.add(dwhName);
				}
			}
		}

		return writerNodes;
	}

	/**
	 * Get all service nodes defined in service_names
	 * @return List of all services as defined in ${CONF_DIR}/service_names
	 * @throws java.io.IOException On errors reading ${CONF_DIR}/service_names
	 */
	public static Set<String> getAllServiceNodes() throws IOException {
		return getServiceDetails().keySet();
	}

	/**
	 * Get a list of all the IQ nodes defined in the system.
	 *
	 * Looks in niq.ini for DWH_WRITER, DWH_READER and the default DWH servicename entry
	 *
	 * @return List of IQ nodes. Will always return at least one entry
	 * @throws IOException Errors reading ini files.
	 */
	public static List<String> getAllIQNodes() throws IOException {
		final File ini = getNiqIni();
		// Read the contents of the ini file and the writer and reader blocks
		final INIGet iniReader = new INIGet();
		final Map<String, Map<String, String>> iniConfig = iniReader.readIniFile(ini);

		final Map<String, String> dwh = iniConfig.get(INI_DWH);
		final Map<String, String> writers = iniConfig.get(INI_DWH_WRITER);
		final Map<String, String> readers = iniConfig.get(INI_DWH_READER);

		// build list of nodes
		final List<String> definedNodes = new ArrayList<String>();

		final String dwhName = dwh.get(SERVER_NAME);
		definedNodes.add(dwhName);

		if (writers != null) {
			//Writers are defined, get their service_names
			for (String writerName : writers.keySet()) {
				final Map<String, String> writerSettings = iniConfig.get(writerName);
				final String sName = writerSettings.get(SERVICE_NAME);
				definedNodes.add(sName);
			}
		}

		if(readers != null) {
			//Readers are defined, get their service_names
			for (String readerName : readers.keySet()) {
				final Map<String, String> readerSettings = iniConfig.get(readerName);
				final String sName = readerSettings.get(SERVICE_NAME);
				definedNodes.add(sName);
			}
		}

		return definedNodes;
	}

	private static File getNiqIni(){
		File ini = new File(System.getProperty(CONF_DIR, CONF_DIR_DEFAULT), DWH_INI_FILENAME);
		if (!ini.exists()) {
			ini = new File(System.getProperty(CONF_DIR, CONF_DIR_DEFAULT), NIQ_INI_FILENAME);
		}
		return ini;
	}


	/**
	 * Struct for service_names entries
	 */
	public final class ServiceHostDetails {

		private final String hostName;
		private final String ipAddress;
		private final String serviceName;

		ServiceHostDetails(final String serviceName, final String servicehost, final String ipaddress) {
			this.serviceName = serviceName;
			this.hostName = servicehost;
			this.ipAddress = ipaddress;
		}

		/**
		 * Get the physical hostname associated with the servicename
		 *
		 * @return physical hostname associated with the servicename
		 */
		public String getServiceHostname() {
			return hostName;
		}

		/**
		 * Get the ipaddress associated with the servicename
		 *
		 * @return ipaddress associated with the servicename
		 */
		public String getServiceIpAddress() {
			return ipAddress;
		}

		/**
		 * The servicename
		 *
		 * @return servicename
		 */
		public String getServiceName() {
			return serviceName;
		}

		@Override
		public String toString() {
			return "ServiceHostDetails [hostName=" + hostName + ", ipAddress=" + ipAddress + ", serviceName="
					+ serviceName + "]";
		}

	}

}

