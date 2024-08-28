package com.ericsson.eniq.parser.transformation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.Logger;

public class ConfigLookup implements Transformation {
	
	private String propertySourceName = null;
	private String src = null;
	private String name = null;
	private String tgt = null;
	private static Map<String,String> eniqOssIntegration = null;
	private static Map<String,String> ossMappingToIP = null;
	private static Map<String,Map<String, String>> configPropertiesMap = new HashMap<String,Map<String,String>>();
	private final static String ENMSERVERDETAILSFILE_PATH= "/eniq/sw/conf/enmserverdetail";
	private final static String OSSREFERENCEFILE_PATH = "/eniq/sw/conf/.oss_ref_name_file";
	private final static String DATASPLITTING_REGEX = "\\s+";
	
	
	// ZBYLVIJ JIRA EQEV-40682
	final static  String ENM_MOUNTINFO_PATH = "/eniq/connectd/mount_info/";
	final static  String OSSREF_FILE_PATH = "/eniq/connectd/mount_info/.oss_ref_name_file";
	

	@Override
	public void configure(String name, String src, String tgt, Properties props, Logger clog)
			throws ConfigException {
		this.src = src;
		this.tgt = tgt;
		this.name = name;
		
		propertySourceName = props.getProperty("propertyname");
		
		//Need to move to cache~~
		
		eniqOssIntegration = new HashMap<String,String>();
		
		ossMappingToIP = new HashMap<String,String>();
		//populatingTransformation(clog);
	}
				
		// File ossReferenceFile = new File(OSSREFERENCEFILE_PATH);
	public Map populatingTransformation(Logger clog){
		clog.info("populatingTransformation called");
		File ossReferenceFile = new File(OSSREF_FILE_PATH);
		
		BufferedReader ossReferenceReader = null;
		
		clog.info("inside the configLookup path.");
		
		try{
				
		//extractENMConfigToIPMap();
		extractEnmMountInfoFiles(ENM_MOUNTINFO_PATH);
		
		if(ossReferenceFile.exists()){
			Set<String> ossDataStream = new HashSet<String>();
			String currentLineInOss;
			String extractedCurrentLineInOss = null;
			
			ossReferenceReader = new BufferedReader(new FileReader(ossReferenceFile));
			
			while((currentLineInOss = ossReferenceReader.readLine()) != null){
					if(!currentLineInOss.equals("")){
						if(ossMappingToIP.size() != 0 && (extractedCurrentLineInOss = (String) ossMappingToIP.get(currentLineInOss.split(DATASPLITTING_REGEX)[1])) != null){
							eniqOssIntegration.put(currentLineInOss.split(DATASPLITTING_REGEX)[0],extractedCurrentLineInOss);
						}
						else{
							eniqOssIntegration.put(currentLineInOss.split(DATASPLITTING_REGEX)[0],"OSS");
						}
					}
				}
			if(eniqOssIntegration.size() != 0)
				configPropertiesMap.put("eniqossmapping", eniqOssIntegration);
			else {
				clog.info("ConfigLookup transformation is not populated due to unavailablity of data.");
			}
		} else {
			clog.fatal("File doesn't exsist. Could not load the eniq oss mapping.");
		}
		} catch(Exception e){
			clog.fatal("Error while configuring the configLookup transformation -"+e);
		} finally {
			try {
				if(ossReferenceFile != null)
					ossReferenceReader.close();
			} catch (IOException e) {
		// TODO Auto-generated catch block
				clog.fatal("Unable to close reader stream from configLookup transformation -"+e);
			} catch (NullPointerException e) {
				clog.fatal("Unable to close reader stream from configLookup transformation -"+e);
			}
		
	}
		clog.info("Config data "+configPropertiesMap);
		return eniqOssIntegration;
		
	}
	
	private void extractENMConfigToIPMap() throws Exception{
		File enmServerDetailsFile = new File(ENMSERVERDETAILSFILE_PATH);
		if(enmServerDetailsFile.exists()){
			BufferedReader enmFileReader = null;
			try{
				enmFileReader = new BufferedReader(new FileReader(enmServerDetailsFile));
				String currentLineInENM;
				while((currentLineInENM = enmFileReader.readLine()) != null){
					ossMappingToIP.put(currentLineInENM.split(DATASPLITTING_REGEX)[0].trim(),"ENM");
				}
			} finally {
				try {
					enmFileReader.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}

	/**
	 * Extract the list of IP's where fs_mount_list file contains pmic 
	 * @param enmFileSys - mount_info dir
	 * @throws Exception
	 */
	
	private void extractEnmMountInfoFiles(String enmFileSys) throws Exception {

		File enmFile = new File(enmFileSys);
		BufferedReader buffReader = null;

		if (enmFile.exists()) {
			// get all the files from a directory
			File[] fList = enmFile.listFiles();
			for (File file : fList) {
				if (file.isFile()) {
					if (file.exists()) {
						try {
							buffReader = new BufferedReader(new FileReader(file));

							String currentLine;
							while ((currentLine = buffReader.readLine()) != null) {
								if (currentLine.contains("pmic") && !currentLine.equals("")) {
									ossMappingToIP.put(currentLine.split(DATASPLITTING_REGEX)[0].trim(), "ENM");
								}
							}
						} catch (IOException e) {
							e.printStackTrace();
						} finally {
							try {
								if (buffReader != null)
									buffReader.close();
							} catch (IOException ex) {
								ex.printStackTrace();
							}
						}
					}
				} else if (file.isDirectory()) {
					extractEnmMountInfoFiles(file.getAbsolutePath());
				}
			}
		}
	}
	
	@Override
	public void transform(Map data, Logger clog) throws Exception {
		
		/*clog.debug("ConfigLookup.transform: src = " + src);
		clog.debug("ConfigLookup.transform: data = " + data);
		clog.debug("ConfigLookup.transform: tgt = " + tgt);
		clog.debug("ConfigLookup.transform: PropertyName = " + propertySourceName);
		clog.debug("ConfigLookup.transform: lookupMap = " + configPropertiesMap);
		
		if(src != null || propertySourceName != null){
			String propertyValue = configPropertiesMap.size() != 0 ? configPropertiesMap.get(propertySourceName).get(data.get(src)) : "";
			//if(propertyValue != null)
				data.put(tgt,propertyValue);
		} else {
			data.put(tgt, null);
		}*/
		data.put(tgt, "ENM");
	}

	@Override
	public String getSource() throws Exception {
		// TODO Auto-generated method stub
		return src;
	}

	@Override
	public String getTarget() throws Exception {
		// TODO Auto-generated method stub
		return tgt;
	}

	@Override
	public String getName() throws Exception {
		// TODO Auto-generated method stub
		return name;
	}

}
