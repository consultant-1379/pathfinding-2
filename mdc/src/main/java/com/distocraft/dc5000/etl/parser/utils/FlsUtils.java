package com.distocraft.dc5000.etl.parser.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;


public class FlsUtils {

	private static Set<String> flsEnabledSet;
	
	private static Map<String, String> enmShortHostNames;
	
	private static Logger log;
	
	static {
		log = Logger.getLogger("etlengine.Engine");
		initFlsEnabledCache();
	}
	
	public static boolean isFlsEnabled(String ossId) {
		if (flsEnabledSet != null) {
			return flsEnabledSet.contains(ossId);
		} else {
			return false;
		}
		
	}
	
	public static String getEnmShortHostName(String enmAlias) {
		if (enmShortHostNames != null) {
			return enmShortHostNames.get(enmAlias);
		}
		return null;
	}
	
	public static void initFlsEnabledCache() {
		File flsConf = new File("/eniq/installation/config/fls_conf");
		if (flsConf.exists()) {
			try (BufferedReader reader = new BufferedReader(new FileReader(flsConf));){
				flsEnabledSet = new HashSet<>();
				String ossId = null;
				while ((ossId = reader.readLine()) != null) {
					flsEnabledSet.add(ossId);
				}
				populateShortHostName();
			} catch (FileNotFoundException e) {
				log.log(Level.INFO, "/eniq/installation/config/fls_conf not found");
			} catch (IOException e) {
				log.log(Level.WARNING, "unable to read FLS configuration files", e);
			}
		} else {
			log.log(Level.INFO, "/eniq/installation/config/fls_conf not found");
		}
	}
	
	private static void populateShortHostName() throws IOException {
		Map<String, String> ossRefMap = new HashMap<>();
		enmShortHostNames = new HashMap<>();
		File ref = new File ("/eniq/sw/conf/.oss_ref_name_file");
		File enm = new File ("/eniq/sw/conf/enmserverdetail");
		try(BufferedReader inputOSS = new BufferedReader(new FileReader(ref));
		BufferedReader inputENM = new BufferedReader(new FileReader(enm));) {
			String ossRefLine;
			while ((ossRefLine = inputOSS.readLine()) != null) {
				String[] odd = ossRefLine.split("\\s+");
				ossRefMap.put(odd[1], odd[0]);
			}
			String enmLine;
			String enmShortHostName = null;
			String ossAlias = null;
			String ossIp = null;
			while ((enmLine = inputENM.readLine()) != null) {
				String[] tokens = enmLine.split("\\s+");
				ossIp = tokens[0];
				if (ossRefMap.containsKey(ossIp)) {
					ossAlias = ossRefMap.get(ossIp);
					if (flsEnabledSet.contains(ossAlias)) {
						enmShortHostName = tokens[1].split("\\.")[0];
						enmShortHostNames.put(ossAlias, enmShortHostName);
					}
				}
			}
		} catch (FileNotFoundException e) {
			log.log(Level.INFO,"Files .oss_ref_name_file or enmserverdetail not found :");
		}
	}
}
