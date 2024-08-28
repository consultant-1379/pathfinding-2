package com.ericsson.eniq.parser.GPP32435Parser;

import java.io.FileInputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.Map.Entry;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import com.ericsson.eniq.parser.EntityResolver;
import com.ericsson.eniq.parser.MeasurementFileFactory;
import com.ericsson.eniq.parser.MeasurementFileFactory.Channel;
import com.ericsson.eniq.parser.SourceFile;
import com.ericsson.eniq.parser.cache.DFormat;
import com.ericsson.eniq.parser.cache.DItem;
import com.ericsson.eniq.parser.cache.DataFormatCache;

/**
 * 3GPP TS 32.435 Parser <br>
 * 
 * <br>
 * Configuration: <br>
 * <br>
 * Database usage: Not directly <br>
 * <br>
 * <br>
 * Version supported: v 7.20 <br>
 * <br>
 * Copyright Ericsson 2008 <br>
 * <br>
 * $id$ <br>
 * 
 * <br>
 * <br>
 * <table border="1" width="100%" cellpadding="3" cellspacing="0">
 * <tr bgcolor="#CCCCFF" class="TableHeasingColor">
 * <td colspan="4"><font size="+2"><b>Parameter Summary</b></font></td>
 * </tr>
 * <tr>
 * <td><b>Name</b></td>
 * <td><b>Key</b></td>
 * <td><b>Description</b></td>
 * <td><b>Default</b></td>
 * </tr>
 * <tr>
 * <td>Vendor ID mask</td>
 * <td>3GPP32435Parser.vendorIDMask</td>
 * <td>Defines how to parse the vendorID</td>
 * <td>.+,(.+)=.+</td>
 * </tr>
 * <tr>
 * <td>Vendor ID from</td>
 * <td>3GPP32435Parser.readVendorIDFrom</td>
 * <td>Defines where to parse vendor ID (file/data supported)<br /> 
 * <td>data</td>
 * </tr>
 * <tr>
 * <td>Fill empty MOID</td>
 * <td>3GPP32435Parser.FillEmptyMOID</td>
 * <td>Defines whether empty moid is filled or not (true/ false)</td>
 * <td>true</td>
 * </tr>
 * <tr>
 * <td>Fill empty MOID style</td>
 * <td>3GPP32435Parser.FillEmptyMOIDStyle</td>
 * <td>Defines the style how moid is filled (static/inc supported)</td>
 * <td>inc</td>
 * </tr>
 * <tr>
 * <td>Fill empty MOID value</td>
 * <td>3GPP32435Parser.FillEmptyMOIDValue</td>
 * <td>Defines the value for the moid that is filled</td>
 * <td>0</td>
 * </tr>
 * </table>
 * <br>
 * <br>
 * <table border="1" width="100%" cellpadding="3" cellspacing="0">
 * <tr bgcolor="#CCCCFF" class="TableHeasingColor">
 * <td colspan="2"><font size="+2"><b>Added DataColumns</b></font></td>
 * </tr>
 * <tr>
 * <td><b>Column name</b></td>
 * <td><b>Description</b></td>
 * </tr>
 * <tr>
 * <td>collectionBeginTime</td>
 * <td>contains the begin time of the whole collection</td>
 * </tr>
 * <tr>
 * <td>objectClass</td>
 * <td>contains the vendor id parsed from MOID</td>
 * </tr>
 * <tr>
 * <td>MOID</td>
 * <td>contains the measured object id</td>
 * </tr>
 * <tr>
 * <td>filename</td>
 * <td>contains the filename of the inputdatafile.</td>
 * </tr>
 * <tr>
 * <td>PERIOD_DURATION</td>
 * <td>contains the parsed duration of this measurement</td>
 * </tr>
 * <tr>
 * <td>DATETIME_ID</td>
 * <td>contains the counted starttime of this measurement</td>
 * </tr>
 * <tr>
 * <td>DC_SUSPECTFLAG</td>
 * <td>contains the suspected flag value</td>
 * </tr>
 * <tr>
 * <td>DIRNAME</td>
 * <td>Contains full path to the inputdatafile.</td>
 * </tr>
 * <tr>
 * <td>JVM_TIMEZONE</td>
 * <td>contains the JVM timezone (example. +0200)</td>
 * </tr>
 * <tr>
 * <td>vendorName</td>
 * <td>contains the vendor name</td>
 * </tr>
 * <tr>
 * <td>fileFormatVersion</td>
 * <td>contains the version of file format</td>
 * </tr>
 * <tr>
 * <td>dnPrefix</td>
 * <td>contains the dn prefix</td>
 * </tr>
 * <tr>
 * <td>localDn</td>
 * <td>contains the local dn</td>run
 * </tr>
 * <tr>
 * <td>managedElementLocalDn</td>
 * <td>contains the local dn of managedElement element</td>
 * </tr>
 * <tr>
 * <td>elementType</td>
 * <td>contains the element type</td>
 * </tr>
 * <tr>
 * <td>userLabel</td>
 * <td>contains the user label</td>
 * </tr>
 * <tr>
 * <td>swVersion</td>
 * <td>contains the software version</td>
 * </tr>
 * <tr>
 * <td>endTime</td>
 * <td>contains the granularity period end time</td>
 * </tr>
 * <tr>
 * <td>measInfoId</td>
 * <td>contains the measInfoId</td>
 * </tr>
 * <tr>
 * <td>jobId</td>
 * <td>contains the jobId</td>
 * </tr>
 * <tr>
 * <td>&lt;measType&gt; (amount varies based on measurement executed)</td>
 * <td>&lt;measValue&gt; (amount varies based on measurement executed)</td>
 * </tr>
 * </table>
 * <br>
 * <br>
 * 
 * @author pylkk?nen <br>
 * <br>
 * 
 */

public class Parser extends DefaultHandler implements Callable<Boolean> {

	// Virtual machine timezone unlikely changes during execution of JVM
		private static final String JVM_TIMEZONE = (new SimpleDateFormat("Z"))
				.format(new Date());
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ssZ");

		/**
		 * 3GPP 7.20
		 */
		private String fileFormatVersion;
		private String vendorName;
		private String dnPrefix;
		private String fsLocalDN;
		private String elementType;
		private String meLocalDN;
		private String userLabel;
		private String swVersion;
		private String collectionBeginTime;
		private String granularityPeriodDuration;
		private String granularityPeriodEndTime;
		private String repPeriodDuration;
		private String jobId;
		private String measInfoId;
		private HashMap measNameMap;
		private HashMap clusterMap;

		// Since special handling of #-mark causes parsing to fail in some cases,
		// let's keep the "original" counterrnames in the map also without special
		// #-handling
		private HashMap<String, String> origMeasNameMap;

		private String suspectFlag = "";
		private String measIndex;
		private String measValueIndex;
		private String measObjLdn;

		private String charValue;

		private SourceFile sf;
		
		private String interfacename;
		
		private Map<String, Channel> channelMap = new HashMap<>();

		private String objectClass;

		private String objectMask;

		private String matchObjectMaskAgainst;

		private String readVendorIDFrom;

		private boolean fillEmptyMoid = true;

		private String fillEmptyMoidStyle = "";

		private String fillEmptyMoidValue = "";
		
		private String objectClassPostFix = "";

		private String oldObjClass;

		private Channel measFile = null;

		private Channel vectorMeasFile = null;

		private Logger log;
		
		private String techPack;

		private String setType;

		private String setName;

		private int status = 0;

		private String workerName = "";

		final private List errorList = new ArrayList();

		private boolean hashData = false;

		private boolean dataFormatNotFound = false;

		private String interfaceName = "";

		private HashMap<String, HashMap<String, HashMap<String, String>>> loaderClass;

		private HashMap<String, HashMap<String, String>> moidMap;

		private HashMap<String, String> counterMap;

		private HashMap<String, String> objectMap;

		private HashMap<String, String> counterValueMap;

		private HashMap<String, HashMap<String, String>> dataMap;

		private Map<String, Map<String, String>> vectorBinValueMap = new HashMap<String, Map<String, String>>();

		private HashMap<String, Map<String, Map<String, String>>> vectorData = new HashMap<String, Map<String, Map<String, String>>>();
		
		private HashMap<String, String> measInfoAndLdn = new HashMap<String, String>();

		private String rangeCounterTag = "VECTOR";

		private String cmVectorTag = "CMVECTOR";

		private String rangeColunName = ""; // name of the range column

		private String rangePostfix = ""; // string that is added to the

		private String compressedVectorTag = "COMPRESSEDVECTOR";

		private Set compressedVectorCounters;

		private boolean createOwnVectorFile = true;

		private String keyColumnTag = "KEY";

		private String vectorPostfix = null;

		private Map measurement;

		final static private String delimiter = ",";

		private String measInfoIdPattern;

		DataFormatCache dfc = DataFormatCache.getCache();

		// For L17A Differentiated Observability Flex Counter handling
		private String flexPostfix = null; // Post-fix to MOID for Flex tables
		private boolean hasFlexCounters = false; // If flex counters not required
		private boolean createOwnFlexFile = true; // To keep it consistent with
													// vector implementation/future
													// impact
		
		
		private Channel flexMeasFile = null;
		private String flexCounterTag; // Tag to differentiate flex counters in
										// techpack
		private HashMap flexValueMap; // Map to store flex counter values
		private HashMap <String, Map> flexFilterMap; // Map to arrange data per filter
		private HashMap flexMoidMap; // Map to arrange data per MOID
		private Map flexCounterBin; // Map to store list of Flex counters in an MOID
									// tag
		
		//Fields for handling dynamic counters
		private String dynPostfix = null; 
		private boolean hasDynCounters = false; 
		private boolean createOwnDynFile = true;
		private Channel dynMeasFile = null;
		private String dynCounterTag;
		private Map<String, Map<String,String>> dynIndexMap; 
		private Map<String, Map<String, Map<String,String>>> dynMoidMap; 
		private Map<String, Object> dynCounterBin;
		private String dynMask;
		private int dynPartIndex;
		private int dynIndexIndex;
		private int staticPartIndex;

		private long parseStartTime;
		private long fileSize = 0L;
		private long totalParseTime = 0L;
		private int fileCount = 0;
		
		private Set<String> measInfoPrefixSet;
		private boolean isPrefixMeasInfo = false;
		
		private static final String FLEX_COUNTER_PATTERN = "(.+?)_(.*)";
		
		private HashMap<String, String> measTypeMap=new HashMap<String,String>();
		
		/**
	   * 
	   */
		
		public Parser(final SourceFile sf, final String techPack,
				final String setType, final String setName, final String workerName){
			
			this.sf = sf;
			this.techPack = techPack;
			this.setType = setType;
			this.setName = setName;
			this.status = 1;
			this.workerName = workerName;

			String logWorkerName = "";
			if (workerName.length() > 0) {
				logWorkerName = "." + workerName;
			}

			log = LogManager.getLogger("etl." + techPack + "." + setType + "."
					+ setName + ".parser.xml3gppParser" + logWorkerName);

		}

		public int status() {
			return status;
		}

		public List errors() {
			return errorList;
		}

		@Override
		public Boolean call() {
			try {
				this.status = 2;
				
				parseStartTime = System.currentTimeMillis();
				long fileSize = 0L;
				int fileCount = 0;

				fileCount++;
				fileSize += sf.fileSize();
				parse(sf, techPack, setType, setName);
					
				totalParseTime = System.currentTimeMillis() - parseStartTime;
				if (totalParseTime != 0) {
					log.info("Parsing Performance :: " + fileCount + " files parsed in " + totalParseTime / 1000
							+ " sec, filesize is " + fileSize / 1000 + " Kb and throughput : " + (fileSize / totalParseTime)
							+ " bytes/ms.");
				}
			} catch (final Exception e) {
				// Exception catched at top level. No good.
				log.log(Level.WARN, "Worker parser failed to exception", e);
				errorList.add(e);
			} finally {
				this.status = 3;
			}
			return true;
		}

		/**
	   * 
	   */
	
		public void parse(final SourceFile sf, final String techPack,
				final String setType, final String setName) throws Exception {
			if (measFile != null) {
				try {
					log.log(Level.TRACE, "Closing Meas File");
				} catch (final Exception e) {
					log.log(Level.TRACE, "Worker parser failed to exception", e);
					throw new SAXException("Error closing measurement file");
				}
			}
			if (vectorMeasFile != null) {
				try {
					log.log(Level.TRACE, "Closing vectorMeasFile File");
				} catch (final Exception e) {
					log.log(Level.TRACE, "Worker parser failed to exception", e);
					throw new SAXException("Error closing measurement file");
				}

			}
			if (flexMeasFile != null) {
				try {
					log.log(Level.TRACE, "Closing flexMeasFile File");
				} catch (final Exception e) {
					log.log(Level.TRACE, "Worker parser failed to exception", e);
					throw new SAXException("Error closing flex measurement file");
				}

			}
			
			if (dynMeasFile != null) {
				try {
					log.log(Level.TRACE, "Closing dynMeasFile File");
				} catch (final Exception e) {
					log.log(Level.TRACE, "Worker parser failed to exception", e);
					throw new SAXException("Error closing dyn measurement file");
				}

			}
			
			this.measFile = null;
			final long start = System.currentTimeMillis();
			this.sf = sf;
			objectMask = sf.getProperty("x3GPPParser.vendorIDMask", ".+,(.+)=.+");
			measInfoIdPattern = sf.getProperty("x3GPPParser.vendorIDMask", "(.*)");
			matchObjectMaskAgainst = sf.getProperty(
					"x3GPPParser.matchVendorIDMaskAgainst", "subset");
			readVendorIDFrom = sf.getProperty("x3GPPParser.readVendorIDFrom",
					sf.getProperty("3GPP32435Parser.readVendorIDFrom", null));
			if ((null == readVendorIDFrom) || (readVendorIDFrom == "")) {
				readVendorIDFrom = sf.getProperty("x3GPPParser.readVendorIDFrom",
						"data");
			}
			fillEmptyMoid = "true".equalsIgnoreCase(sf.getProperty(
					"x3GPPParser.FillEmptyMOID", "true"));
			fillEmptyMoidStyle = sf.getProperty("x3GPPParser.FillEmptyMOIDStyle",
					"inc");
			fillEmptyMoidValue = sf.getProperty("x3GPPParser.FillEmptyMOIDValue",
					"0");
			hashData = "true".equalsIgnoreCase(sf.getProperty(
					"x3GPPParser.HashData",
					sf.getProperty("3GPP32435Parser.HashData", "false")));
			interfaceName = sf.getProperty("interfaceName", "");
			rangePostfix = sf.getProperty("x3GPPParser.RangePostfix",
					sf.getProperty("3GPP32435Parser.RangePostfix", "_DCVECTOR"));
			rangeColunName = sf.getProperty("x3GPPParser.RangeColumnName", sf
					.getProperty("3GPP32435Parser.RangeColumnName",
							"DCVECTOR_INDEX"));
			rangeCounterTag = sf.getProperty("x3GPPParser.RangeCounterTag",
					sf.getProperty("3GPP32435Parser.RangeCounterTag", "VECTOR"));
			cmVectorTag = sf.getProperty("x3GPPParser.cmVectorTag",
					sf.getProperty("3GPP32435Parser.cmVectorTag", "CMVECTOR"));

			compressedVectorTag = sf.getProperty("x3GPPParser.compressedVectorTag",
					sf.getProperty("3GPP32435Parser.compressedVectorTag",
							"COMPRESSEDVECTOR"));
			compressedVectorCounters = new HashSet();
			keyColumnTag = sf.getProperty("x3GPPParser.KeyColumnTag",
					sf.getProperty("3GPP32435Parser.KeyColumnTag", "KEY"));
			vectorPostfix = sf.getProperty("x3GPPParser.VectorPostfix",
					sf.getProperty("3GPP32435Parser.VectorPostfix", "_V"));

			// Flex counter initializations before parse
			flexPostfix = sf.getProperty("x3GPPParser.flexPostfix", "_FLEX");
			hasFlexCounters = "true".equalsIgnoreCase(sf.getProperty(
					"x3GPPParser.hasFlexCounters", "false"));
			createOwnFlexFile = "true".equalsIgnoreCase(sf.getProperty(
					"x3GPPParser.createOwnFlexFile", "false"));
			flexCounterTag = sf.getProperty("x3GPPParser.flexCounterTag",
					"FlexCounter");
			
			interfacename = sf.getProperty("interfaceName", "");
			dynPostfix = sf.getProperty("x3GPPParser.dynPostfix", "_DYN");
			hasDynCounters = "true".equalsIgnoreCase(sf.getProperty(
					"x3GPPParser.hasDynCounters", "false"));
			createOwnDynFile = "true".equalsIgnoreCase(sf.getProperty(
					"x3GPPParser.createOwnDynFile", "false"));
			dynCounterTag = sf.getProperty("x3GPPParser.dynCounterTag",
					"DynCounter");
			dynMask = sf.getProperty("x3GPPParser.dynMask",
					"(queue)(.+?)_(.+)");
			
			objectClassPostFix = sf.getProperty("x3GPPParser.objectClassPostFix", "");
			
			try {
				dynPartIndex = Integer.parseInt(sf.getProperty("x3GPPParser.dynPartIndex","1"));
			} catch (Exception e) {
				log.log(Level.WARN, "Not able to parse x3GPPParser.dynPartIndex, "
						+ "hence assigning the default value '1'",e );
				dynPartIndex = 1;
			}
			try {
				dynIndexIndex = Integer.parseInt(sf.getProperty("x3GPPParser.dynIndexIndex","2"));
			} catch (Exception e) {
				log.log(Level.WARN, "Not able to parse x3GPPParser.dynIndexIndex, "
						+ "hence assigning the default value '2'",e );
				dynIndexIndex = 2;
			}
			try {
				staticPartIndex = Integer.parseInt(sf.getProperty("x3GPPParser.staticPartIndex","3"));
			} catch (Exception e) {
				log.log(Level.WARN, "Not able to parse x3GPPParser.staticPartIndex, "
						+ "hence assigning the default value '3'",e );
				staticPartIndex = 3;
			}
			
			
			final String addVendorIDToDelimiter=sf.getProperty("addVendorIDToDelimiter",",");
			log.trace("addVendorIDToDelimiter value  is "+addVendorIDToDelimiter);
			
			final String addMeasInfoPrefix = sf.getProperty("addVendorIDTo", "");
			measInfoPrefixSet = new HashSet<>();

			if (addMeasInfoPrefix != null && !("").equals(addMeasInfoPrefix)) {
				final String[] addMeasInfoPrefixs = addMeasInfoPrefix.split(addVendorIDToDelimiter);
				for (int i = 0; i < addMeasInfoPrefixs.length; i++) {
					measInfoPrefixSet.add(addMeasInfoPrefixs[i]);
				}
			}
			
			final SAXParserFactory spf = SAXParserFactory.newInstance();
			// spf.setValidating(validate);

			final SAXParser parser = spf.newSAXParser();
			final XMLReader xmlReader = parser.getXMLReader();
			xmlReader.setContentHandler(this);
			xmlReader.setErrorHandler(this);

		//	xmlReader.setEntityResolver(new ENIQEntityResolver(log.getName()));
			xmlReader.setEntityResolver(new EntityResolver());
			final long middle = System.currentTimeMillis();
			xmlReader.parse(new InputSource(sf.getFileInputStream()));
			final long end = System.currentTimeMillis();
			log.log(Level.TRACE, "Data parsed. Parser initialization took "
					+ (middle - start) + " ms, parsing " + (end - middle)
					+ " ms. Total: " + (end - start) + " ms.");
			oldObjClass = null;
		}
		
		private Matcher getMatcher(String input, String pattern){
			Pattern p = Pattern.compile(pattern);
			return p.matcher(input);
		}

		/**
	   * 
	   */
		public void parse(final FileInputStream fis) throws Exception {

			final long start = System.currentTimeMillis();
			final SAXParserFactory spf = SAXParserFactory.newInstance();
			// spf.setValidating(validate);
			final SAXParser parser = spf.newSAXParser();
			final XMLReader xmlReader = parser.getXMLReader();
			xmlReader.setContentHandler(this);
			xmlReader.setErrorHandler(this);
			final long middle = System.currentTimeMillis();
			xmlReader.parse(new InputSource(fis));
			final long end = System.currentTimeMillis();
			log.log(Level.TRACE, "Data parsed. Parser initialization took "
					+ (middle - start) + " ms, parsing " + (end - middle)
					+ " ms. Total: " + (end - start) + " ms.");
		}

		
		public HashMap strToMap(final String str) {

			final HashMap hm = new HashMap();
			int index = 0;
			if (str != null) {

				// list all triggers
				final StringTokenizer triggerTokens = new StringTokenizer(str, " ");
				while (triggerTokens.hasMoreTokens()) {
					index++;
					hm.put("" + index, triggerTokens.nextToken());
				}
			}

			return hm;
		}

		/**
		 * Event handlers
		 */
		@Override
		public void startDocument() {
		}

		@Override
		public void endDocument() throws SAXException {

		}

		@Override
		public void startElement(final String uri, final String name,final String qName, final Attributes atts) throws SAXException {

			charValue = "";

			if (qName.equals("fileHeader")) {
				this.fileFormatVersion = atts.getValue("fileFormatVersion");
				this.vendorName = atts.getValue("vendorName");
				this.dnPrefix = atts.getValue("dnPrefix");
			} else if (qName.equals("fileSender")) {
				this.fsLocalDN = atts.getValue("localDn");
				this.elementType = atts.getValue("elementType");
			} else if (qName.equals("measCollec")) {
				if (atts.getValue("beginTime") != null) {
					// header
					collectionBeginTime = atts.getValue("beginTime");
				} else if (atts.getValue("endTime") != null) {
					// footer
				}
			} else if (qName.equals("measData")) {
				measNameMap = new HashMap();
				clusterMap = new HashMap();

				origMeasNameMap = new HashMap<String, String>();

				if (hashData) {
					loaderClass = new HashMap<String, HashMap<String, HashMap<String, String>>>();
					objectMap = new HashMap<String, String>();
					dataMap = new HashMap<String, HashMap<String, String>>();
					flexMoidMap = new HashMap();
					dynMoidMap = new HashMap<>();
				}

			} else if (qName.equals("managedElement")) {
				this.meLocalDN = atts.getValue("localDn");
				this.userLabel = atts.getValue("userLabel");
				this.swVersion = atts.getValue("swVersion");
			} else if (qName.equals("measInfo")) {
				this.measInfoId = atts.getValue("measInfoId");
				if (measInfoPrefixSet.contains(measInfoId)) {
					isPrefixMeasInfo = true;
				}
				/*
				 * //This is modified for JIRA EQEV-35781 if(this.measInfoId !=
				 * null){ //This is modified for JIRA EQEV-35535 if
				 * (measInfoId.contains("=")) { //measInfoId =
				 * measInfoId.substring(measInfoId.lastIndexOf("=") + 1,
				 * measInfoId.length()); measInfoId = parseFileName(measInfoId,
				 * measInfoIdPattern);
				 * 
				 * } }
				 */

				if (hashData && readVendorIDFrom.equals("measInfoId")) {
					if (measInfoId.contains("=")) {
						getLoaderName(parseFileName(measInfoId, measInfoIdPattern));
					} else {
						getLoaderName(measInfoId);
					}
				}
			} else if (qName.equals("job")) {
				this.jobId = atts.getValue("jobId");
			} else if (qName.equals("granPeriod")) {
				granularityPeriodDuration = getSeconds(atts.getValue("duration"));
				granularityPeriodEndTime = atts.getValue("endTime");
			} else if (qName.equals("repPeriod")) {
				repPeriodDuration = getSeconds(atts.getValue("duration"));
			} else if (qName.equals("measTypes")) {
			} else if (qName.equals("measType")) {
				measIndex = atts.getValue("p");
			} else if (qName.equals("measValue")) {
				this.suspectFlag = "";
				this.measObjLdn = atts.getValue("measObjLdn");
				if(readVendorIDFrom.equals("data")||readVendorIDFrom.equals("measObjLdn"))
				{
					String objectClassForAddVendorID=parseFileName(measObjLdn,objectMask);
		            log.trace("Result of extracting the tag id "+objectClassForAddVendorID);
		            String addVendorIdFromProperty="";
		            if(measInfoPrefixSet.contains(objectClassForAddVendorID))
		            {
		            	addVendorIdFromProperty=objectClassForAddVendorID+"_";
		            }
		            
		            for(String measIndexFromMap:measTypeMap.keySet())
	            	{
	            		String charValueAfterMeasObjectLdn=addVendorIdFromProperty+measTypeMap.get(measIndexFromMap);
	            		measNameMap.put(measIndexFromMap, extractCounterName(charValueAfterMeasObjectLdn));
	    				clusterMap.put(measIndexFromMap, clusterField);
	    				origMeasNameMap.put(measIndexFromMap, charValueAfterMeasObjectLdn);
	            	}
		            measTypeMap.clear();
		   
				}
		           
		            
	            
				handleTAGmoid(measObjLdn);
				if (hashData && readVendorIDFrom.equalsIgnoreCase("measInfoId")) {
					counterValueMap = new HashMap<String, String>();
					flexFilterMap = new HashMap();
					dynIndexMap = new HashMap<>();
				} else if (hashData
						&& (readVendorIDFrom.equalsIgnoreCase("measObjLdn") || readVendorIDFrom
								.equalsIgnoreCase("data")|| readVendorIDFrom.equalsIgnoreCase("file"))) {

					counterValueMap = new HashMap<String, String>();
					flexFilterMap = new HashMap();
					dynIndexMap = new HashMap<>();
					log.log(Level.TRACE, "objectClass before getting loader name : "+ objectClass);
					getLoaderName(objectClass);

				} else {
					try {
						if (sf != null) {
							if ((oldObjClass == null)
									|| !oldObjClass.equals(objectClass)) {
								// close meas file
								if (measFile != null) {
									log.log(Level.TRACE, "R:measFile not null");
									measFile=null;
								}
								if (vectorMeasFile != null) {
									log.log(Level.TRACE,
											"R:vectormeasFile not null");
									vectorMeasFile=null;
									
								}
								if (flexMeasFile != null) {
									log.log(Level.TRACE, "R:flexMeasFile not null");
									flexMeasFile=null;
								}
								if (dynMeasFile != null) {
									log.log(Level.TRACE, "R:dynMeasFile not null");
									dynMeasFile=null;
								}
								// create new measurementFile
								DFormat dfmm = dfc.getDataFormatByInterfaceAndTagId(interfaceName,
										objectClass);
								if(dfmm != null)
								{
									measFile = getChannel("objectClass");
								}
								

								DFormat df = dfc.getDataFormatByInterfaceAndTagId(interfaceName,
										objectClass + vectorPostfix);

								if (df != null) {
									vectorMeasFile = getChannel(objectClass + vectorPostfix);
								}
								if (hasFlexCounters) {
									df = dfc.getDataFormatByInterfaceAndTagId(interfaceName, objectClass + flexPostfix);
									if (createOwnFlexFile && df != null) {
										flexMeasFile = getChannel(objectClass+ flexPostfix);
									}
								}
								if (hasDynCounters) {
									df = dfc.getDataFormatByInterfaceAndTagId(interfaceName,
											objectClass + dynPostfix);
									if (createOwnDynFile && df != null) {
										dynMeasFile = getChannel(objectClass+ dynPostfix);
									}
								}
								oldObjClass = objectClass;

							}
						}
					} catch (final Exception e) {
						log.log(Level.TRACE, "Error opening measurement data", e);
						e.printStackTrace();
						throw new SAXException("Error opening measurement data: "
								+ e.getMessage(), e);
					}
				}
			} else if (qName.equals("measResults")) {
			} else if (qName.equals("r")) {
				this.measValueIndex = atts.getValue("p");
			} else if (qName.equals("suspect")) {
			} else if (qName.equals("fileFooter")) {
			}
		}
		
			
		private void getLoaderName(String tagId) {
			String loaderName = null;
			DataFormatCache cache = DataFormatCache.getCache();
			log.log(Level.TRACE, "get dataformat with interface : "+interfaceName +" tagId : " +tagId);
			DFormat df = cache.getDataFormatByInterfaceAndTagId(interfaceName, tagId);
			if (df == null) {
				log.log(Level.TRACE, "cannot get dataformat , trying with vectorPostfix");
				df = cache.getDataFormatByInterfaceAndTagId(interfaceName, tagId + vectorPostfix);
			}
			if (df == null && hasFlexCounters) {
				log.log(Level.TRACE, "cannot get dataformat , trying with flexPostfix");
				df = cache.getDataFormatByInterfaceAndTagId(interfaceName, tagId + flexPostfix);
			}
			if (df == null && hasDynCounters) {
				log.log(Level.TRACE, "cannot get dataformat , trying with dynPostFix");
				df = cache.getDataFormatByInterfaceAndTagId(interfaceName, tagId + dynPostfix);
			}
			if (df == null) {
				log.log(Level.TRACE, "dataformat not found for interface : "+interfaceName +" and tagId : " +tagId);
				dataFormatNotFound = true;
			} else {
				loaderName = df.getFolderName();
				objectMap.put(tagId, loaderName);
				if (!loaderClass.containsKey(loaderName)) {
					moidMap = new HashMap<>();
					loaderClass.put(loaderName, moidMap);
				} else {
					moidMap = loaderClass.get(loaderName);
				}
			}
			log.log(Level.TRACE, "populated objectMap : "+objectMap);
			log.log(Level.TRACE, "Obtained Loader name = " + loaderName);
		}

		private void handleTAGmoid(String value) {

			this.objectClass = "";
				
			if ("file".equalsIgnoreCase(readVendorIDFrom)) {
				objectClass = parseFileName(sf.getName(), objectMask)+objectClassPostFix;

			} else if ("data".equalsIgnoreCase(readVendorIDFrom)
					|| "measObjLdn".equalsIgnoreCase(readVendorIDFrom)) {

				// if moid is empty and empty moids are filled.
				if (fillEmptyMoid && (value.length() <= 0)) {
					if (fillEmptyMoidStyle.equalsIgnoreCase("static")) {
						value = fillEmptyMoidValue;
					} else {
						value = measValueIndex + "";
					}
				}

				// read vendor id from data
				objectClass = parseFileName(value, objectMask)+objectClassPostFix;

			} else if ("measInfoId".equalsIgnoreCase(readVendorIDFrom)) {
				if (measInfoId.contains("=")) {
					objectClass = parseFileName(this.measInfoId, measInfoIdPattern);
				} else {
					objectClass = measInfoId+objectClassPostFix;
				}
			}
			
			if (hasFlexCounters) {
				final DFormat df = dfc.getDataFormatByInterfaceAndTagId(interfaceName,
						objectClass + flexPostfix);
				if (df != null) {
					final Set flexCounters = getDataIDFromProcessInstructions(
							interfaceName, objectClass + flexPostfix,
							flexCounterTag);
					final Set keyColumns = getDataIDFromProcessInstructions(
							interfaceName, objectClass + flexPostfix, keyColumnTag);
					flexCounterBin = new HashMap();
					flexCounterBin.put("filename", sf.getName());
					flexCounterBin.put("flexCounters", flexCounters);
					flexCounterBin.put("keyColumns", keyColumns);
				}
			}
			
			if (hasDynCounters) {
				final DFormat df = dfc.getDataFormatByInterfaceAndTagId(interfaceName,
						objectClass + dynPostfix);
				if (df != null) {
					final Set<String> dynCounters = getDataIDFromProcessInstructions(
							interfaceName, objectClass + dynPostfix,
							dynCounterTag);
					final Set<String> keyColumns = getDataIDFromProcessInstructions(
							interfaceName, objectClass + dynPostfix, keyColumnTag);
					dynCounterBin = new HashMap<>();
					dynCounterBin.put("filename", sf.getName());
					dynCounterBin.put("dynCounters", dynCounters);
					dynCounterBin.put("keyColumns", keyColumns);
				}
			}
			
			final DFormat df = dfc.getDataFormatByInterfaceAndTagId(interfaceName, objectClass
					+ vectorPostfix);
			if (df != null) {
				log.log(Level.TRACE, "T:ObjectClass:" + objectClass);
				// VECTOR counters
				final Set rangeCounters = getDataIDFromProcessInstructions(
						interfaceName, objectClass + vectorPostfix, rangeCounterTag);
				log.log(Level.TRACE, "T:ObjectClass:" + objectClass
						+ ", vector Counters = " + rangeCounters);

				compressedVectorCounters.addAll(getDataIDFromProcessInstructions(
						interfaceName, objectClass + vectorPostfix,
						compressedVectorTag));

				rangeCounters.addAll(compressedVectorCounters);
				// we search instructions also from objectClass+vectorPostfix

				final Set keyColumns = getDataIDFromProcessInstructions(
						interfaceName, objectClass + vectorPostfix, keyColumnTag);

				// we search instructions also from objectClass+vectorPostfix

				keyColumns.addAll(getDataIDFromProcessInstructions(interfaceName,
						objectClass + vectorPostfix, keyColumnTag));

				// CMVECTOR counters
				final Set cmVectorCounters = getDataIDFromProcessInstructions(
						interfaceName, objectClass + vectorPostfix, cmVectorTag);

				cmVectorCounters.addAll(getDataIDFromProcessInstructions(
						interfaceName, objectClass + vectorPostfix, cmVectorTag));

				// new measurement started
				measurement = new HashMap();
				measurement.put("filename", sf.getName());
				measurement.put("rangeCounters", rangeCounters);
				measurement.put("cmVectorCounters", cmVectorCounters);
				measurement.put("keyColumns", keyColumns);

			}
		}

		private Set getDataIDFromProcessInstructions(final String interfacename,
				final String objectClass, final String key) {
			final Set result = new HashSet();
			try {

				final DFormat df = dfc.getDataFormatByInterfaceAndTagId(interfacename,
						objectClass);

				final List dItemList = df.getDitems();

				final Iterator iter = dItemList.iterator();
				while (iter.hasNext()) {

					final DItem di = (DItem) iter.next();
					if (di.getProcessInstruction() != null) {
						final StringTokenizer token = new StringTokenizer(
								di.getProcessInstruction(), ",");
						while (token.hasMoreElements()) {
							final String t = (String) token.nextElement();

							if (t.equalsIgnoreCase(key)) {
								result.add(di.getDataID());
							}
						}
					}
				}

				if (result.size() == 0) {
					log.log(Level.TRACE, "ResultSet not updated");
				} else {
					log.log(Level.TRACE, "ResultSet  updated");
				}
			} catch (Exception e) {
				log.warn("Error while retrieving DataIDs from ProcessInstructions");

			}

			return result;

		}

		final private Map getKeyCounters(final Map datarow) {

			final Set keyColumns = ((Set) datarow.get("keyColumns"));

			final HashMap keyMap = new HashMap();

			// create map that contains all keys to be added to every new datarow
			final Iterator keyIter = keyColumns.iterator();
			// loop all key columns
			while (keyIter.hasNext()) {
				final String key = (String) keyIter.next();
				// add key columns from original datarow.
				keyMap.put(key, datarow.get(key));
			}

			return keyMap;
		}

		/**
		 * Rips PT and S values off from the value.
		 * 
		 * @param value
		 *            Contains the duration value
		 * @return the duration in seconds
		 */
		private String getSeconds(final String value) {
			String result = null;
			if (value != null){
			if (value.contains("S"))
			{
			result = value.substring(2, value.indexOf('S'));	
			}
			if (value.contains("M"))
			{
			String result_min = value.substring(2,value.indexOf('M'));
			Integer minute = Integer.parseInt(result_min);
			int result_mins = minute * 60;
			result = String.valueOf(result_mins);
			}
			}
			return result;
		}

		private String nameField = "";
		private String clusterField = "";

		private String extractCounterName(final String counterName) {
			final int index1 = counterName.indexOf("#");
			final int index2 = counterName.indexOf(".", index1);
			if (index1 >= 0) {
				if (index2 > index1) { // Format NAME#cluster.NAME -> NAME.NAME and
										// Cluster
					nameField = counterName.substring(0, index1)
							+ counterName.substring(index2, counterName.length());
					clusterField = counterName.substring(index1 + 1, index2);
				} else { // Format NAME#Cluster -> NAME and Cluster
					nameField = counterName.substring(0, index1);
					clusterField = counterName.substring(index1 + 1,
							counterName.length());
				}
			} else { // Format NAME -> NAME
				nameField = counterName;
				clusterField = "";
			}
			return nameField;
		}

		@Override
		public void endElement(final String uri, final String name,
				final String qName) throws SAXException {
			// log.log(Level.FINEST, "endElement(" + uri + "," + name + "," + qName
			// + "," + charobjectClass + ")");

			if (qName.equals("fileHeader")) {
			} else if (qName.equals("fileSender")) {
			} else if (qName.equals("measCollec")) {
			} else if (qName.equals("measData")) {
				if (hashData) {
					handleTAGMeasData();
				}
			} else if (qName.equals("managedElement")) {
			} else if (qName.equals("measInfo")) {
				dataFormatNotFound = false;
				isPrefixMeasInfo = false;
			} else if (qName.equals("job")) {
			} else if (qName.equals("granPeriod")) {
			} else if (qName.equals("repPeriod")) {
			} else if (qName.equals("measTypes")) {
				measNameMap = strToMap(charValue);
			} else if (qName.equals("measType")) {
				
				if(readVendorIDFrom.equalsIgnoreCase("data")||readVendorIDFrom.equalsIgnoreCase("measObjLdn"))
				{
					measTypeMap.put(measIndex,charValue);
				}
				else if(readVendorIDFrom.equalsIgnoreCase("measInfoId"))
				{
					
					if (isPrefixMeasInfo) {
						 charValue = measInfoId+"_"+charValue;
					}
						measNameMap.put(measIndex, extractCounterName(charValue));
						clusterMap.put(measIndex, clusterField);

						origMeasNameMap.put(measIndex, charValue);
				}

			} else if (qName.equals("measValue") && !dataFormatNotFound) {
				if (hashData) {
					handleMoidMap(measObjLdn);
				} else {
					try {

						if (measFile == null && vectorMeasFile == null
								&& flexMeasFile == null && dynMeasFile == null) {
							System.err.println("Measurement file null");
							log.log(Level.TRACE, "PERIOD_DURATION: "
									+ granularityPeriodDuration);
							log.log(Level.TRACE, "repPeriodDuration: "
									+ repPeriodDuration);
							// DATETIME_ID calculated from end time
							final String begin = calculateBegintime();
							if (begin != null) {
								log.log(Level.TRACE, "DATETIME_ID: " + begin);
							}
							log.log(Level.TRACE, "collectionBeginTime: "
									+ collectionBeginTime);
							log.log(Level.TRACE, "DC_SUSPECTFLAG: " + suspectFlag);
							log.log(Level.TRACE, "filename: "
									+ (sf == null ? "dummyfile"
											: sf.getName()));
							log.log(Level.TRACE, "JVM_TIMEZONE: " + JVM_TIMEZONE);
							log.log(Level.TRACE,
									"DIRNAME: "
											+ (sf == null ? "dummydir"
													: sf.getDir()));
							log.log(Level.TRACE, "measInfoId: " + measInfoId);
							log.log(Level.TRACE, "MOID: " + measObjLdn);
							log.log(Level.TRACE, "objectClass: " + objectClass);
							log.log(Level.TRACE, "vendorName: " + vendorName);
							log.log(Level.TRACE, "fileFormatVersion: "
									+ fileFormatVersion);
							log.log(Level.TRACE, "dnPrefix: " + dnPrefix);
							log.log(Level.TRACE, "localDn: " + fsLocalDN);
							log.log(Level.TRACE, "managedElementLocalDn: "
									+ meLocalDN);
							log.log(Level.TRACE, "elementType: " + elementType);
							log.log(Level.TRACE, "userLabel: " + userLabel);
							log.log(Level.TRACE, "swVersion: " + swVersion);
							// collectionEndTime received so late, that migth not be
							// used
							log.log(Level.TRACE, "endTime: "
									+ granularityPeriodEndTime);
							log.log(Level.TRACE, "jobId: " + jobId);
						} else {
							if(measFile != null)
							{
								measFile.pushData("DC_SUSPECTFLAG", suspectFlag);
								measFile.pushData("MOID", measObjLdn);
								measFile.pushData(addValues());
							}

							if (createOwnFlexFile && hasFlexCounters
									&& flexMeasFile != null) {
								flexMeasFile.pushData("DC_SUSPECTFLAG", suspectFlag);
								flexMeasFile.pushData("MOID", measObjLdn);
								flexMeasFile.pushData(addValues());
							}
							
							if (createOwnDynFile && hasDynCounters
									&& dynMeasFile != null) {
								dynMeasFile.pushData("DC_SUSPECTFLAG", suspectFlag);
								dynMeasFile.pushData("MOID", measObjLdn);
								dynMeasFile.pushData(addValues());
							}

							if (vectorMeasFile != null) {
								int maxBinSize = 0;
								log.log(Level.TRACE, "E:StoreMap "
										+ vectorBinValueMap);
								for (String counterName : vectorBinValueMap
										.keySet()) {
									int binSize = vectorBinValueMap
											.get(counterName).size();
									if (binSize > maxBinSize) {
										maxBinSize = binSize;
									}

								}
								final String begin1 = calculateBegintime();
								String checkZero = "NIL";
								for (int i = 0; i < maxBinSize; i++) {
									String binIndex = i + "";
									int flag = 0;
									Map<String, String> value = new HashMap<String, String>();
									for (String counterName : vectorBinValueMap
											.keySet()) {
										value = vectorBinValueMap.get(counterName);

										if (value.containsKey(binIndex)
												&& binIndex != null) {
											if (flag == 0) {
												flag = 1;
											}
											if (checkZero.equals(value
													.get(binIndex))) {
												vectorMeasFile.pushData(counterName,
														null);
											} else {
												vectorMeasFile.pushData(counterName, value.get(binIndex));
											}
											log.log(Level.TRACE,
													"Q:CounterName" + counterName
															+ "Value"
															+ value.get(binIndex));
											log.trace("DCVECTOR_INDEX" + binIndex);
											vectorMeasFile.pushData(
													"DCVECTOR_INDEX", binIndex);
											vectorMeasFile.pushData(counterName
													+ rangePostfix, binIndex);
											vectorMeasFile.pushData("MOID",
													measObjLdn);
											vectorMeasFile.pushData(
													"DC_SUSPECTFLAG", suspectFlag);
											vectorMeasFile.pushData(addValues());
										}

									}
									try {
										if (flag == 1) {
											//vectorMeasFile.saveData();
										}
									} catch (final Exception e) {
										log.log(Level.WARN,
												"Error saving measurement data", e);
										e.printStackTrace();
										throw new SAXException(
												"Error saving measurement data: "
														+ e.getMessage(), e);

									}
								}

								Map<String, String> value = new HashMap<String, String>();
								for (String counterName : vectorBinValueMap
										.keySet()) {
									value = vectorBinValueMap.get(counterName);
									Iterator iterator = value.keySet().iterator();
									while (iterator.hasNext()) {
										String i1 = (String) iterator.next();
										if (Integer.parseInt(i1) > maxBinSize) {
											if (value.containsKey(i1) && i1 != null
													&& value.get(i1) != null) {
												if (checkZero.equals(value.get(i1))) {
													vectorMeasFile.pushData(
															counterName, null);
												} else {
													vectorMeasFile.pushData(
															counterName,
															value.get(i1));
												}
												log.log(Level.TRACE,
														"Q:CounterName"
																+ counterName
																+ "Value"
																+ value.get(i1));
												vectorMeasFile.pushData(
														"DCVECTOR_INDEX", i1);
												vectorMeasFile.pushData(counterName
														+ rangePostfix, i1);
												vectorMeasFile.pushData("MOID",
														measObjLdn);
												vectorMeasFile.pushData(
														"DC_SUSPECTFLAG",
														suspectFlag);
												vectorMeasFile.pushData(addValues());

												try {
												//	vectorMeasFile.saveData();
												} catch (final Exception e) {
													log.log(Level.WARN,
															"Error saving measurement data",
															e);
													e.printStackTrace();
													throw new SAXException(
															"Error saving measurement data: "
																	+ e.getMessage(),
															e);

												}
											}
										}
									}

								}

							}
							vectorBinValueMap.clear();
						}

					} catch (final Exception e) {
						log.log(Level.TRACE, "Error saving measurement data", e);
						e.printStackTrace();
						throw new SAXException("Error saving measurement data: "
								+ e.getMessage(), e);
					}
				}
			} else if (qName.equals("measResults")) {
				final Map measValues = strToMap(charValue);
				if (measValues.keySet().size() == measNameMap.keySet().size()) {
					final Iterator it = measValues.keySet().iterator();
					while (it.hasNext()) {
						final String s = (String) it.next();
						String origValue = (String) measValues.get(s);
						if ((origValue != null)
								&& (origValue.equalsIgnoreCase("NIL") || origValue
										.trim().equalsIgnoreCase(""))) {
							origValue = null;
							log.trace("Setting the value to null as in-valid data being received from the Node");
						}
						if (hashData) {
							boolean isFlexVal = false;
							boolean isDynVal = false;
							if (hasFlexCounters && flexCounterBin != null) {
								isFlexVal = checkIfFlex((String) measNameMap.get(s));
							}
							if (hasDynCounters && dynCounterBin != null) {
								isDynVal = checkIfDyn((String) measNameMap.get(s));
							}
							if (!isFlexVal && !isDynVal) {
								counterValueMap.put((String) measNameMap.get(s),
										origValue);
								log.log(Level.TRACE,
										(String) measNameMap.get(measValueIndex)
												+ ": " + origValue);
							}
						} else {
							if (measFile == null) {
								System.out.println((String) measNameMap.get(s)
										+ ": " + origValue);
							} else {
								measFile.pushData((String) measNameMap.get(s),
										origValue);
								log.log(Level.TRACE, (String) measNameMap.get(s)
										+ ": " + origValue);
							}
						}
					}
				} else {
					log.warn("Data contains one or more r-tags than mt-tags");
				}
			} else if (qName.equals("r") && !dataFormatNotFound) {
				if (hashData) {
					if (measNameMap.get(measValueIndex) != null) {
						String origValue = charValue;
						boolean isFlexCounter = false;
						boolean isDynCounter = false;
						if (hasFlexCounters && flexCounterBin != null) {
							isFlexCounter = checkIfFlex((String) measNameMap
									.get(measValueIndex));
						}
						if (hasDynCounters && dynCounterBin != null) {
							isDynCounter = checkIfDyn((String) measNameMap
									.get(measValueIndex));
						}
						if ((origValue != null)) {
							Pattern whitespace = Pattern.compile("\\s+");
							Matcher matcher = whitespace.matcher(origValue);
							origValue = matcher.replaceAll("");
						}
						if ((origValue != null)
								&& (origValue.equalsIgnoreCase("NIL") || origValue
										.trim().equalsIgnoreCase(""))) {
							origValue = null;
							log.trace("Hashdata: Setting the value to null as in-valid data being received from the Node");
						}

						if (origValue != null) {
							if (origValue.contains(",")) {
								final Map keyCounters = new HashMap();// getKeyCounters(measurement);
								final String counterName = origMeasNameMap
										.get(measValueIndex);
								Map<String, String> tmpMap = handleVectorCounters(
										measurement, keyCounters, counterName,
										origValue);
								vectorBinValueMap.put(counterName, tmpMap);
							} else if (!isFlexCounter && !isDynCounter) {
								counterValueMap.put(
										(String) measNameMap.get(measValueIndex),
										origValue);
								counterValueMap.put("clusterId",
										(String) clusterMap.get(measValueIndex));

								counterValueMap.put(
										origMeasNameMap.get(measValueIndex),
										origValue);

								log.log(Level.TRACE,
										(String) measNameMap.get(measValueIndex)
												+ ": " + origValue);
								log.log(Level.TRACE,
										origMeasNameMap.get(measValueIndex) + ": "
												+ origValue);
							}
						}
					} else {
						log.warn("Data contains one or more r-tags than mt-tags");
					}
				} else {
					if (measNameMap.get(measValueIndex) != null) {
						boolean isFlexCounter = false;
						if (hasFlexCounters && flexCounterBin != null) {
							isFlexCounter = checkIfFlex((String) measNameMap
									.get(measValueIndex));
						}
						
						boolean isDynCounter = false;
						if (hasDynCounters && dynCounterBin != null) {
							isDynCounter = checkIfDyn((String) measNameMap
									.get(measValueIndex));
						}

						String origValue = charValue;
						if ((origValue != null)) {
							Pattern whitespace = Pattern.compile("\\s+");
							Matcher matcher = whitespace.matcher(origValue);
							origValue = matcher.replaceAll("");
						}
						if ((origValue != null)
								&& (origValue.equalsIgnoreCase("NIL") || origValue
										.trim().equalsIgnoreCase(""))) {
							origValue = null;
							log.trace(" else block : Setting the value to null as in-valid data being received from the Node");
						}

						if (measFile == null && vectorMeasFile == null&&flexMeasFile == null &&dynMeasFile==null) {
							System.out.println((String) measNameMap
									.get(measValueIndex)
									+ ": "
									+ origValue
									+ " clusterId: "
									+ (String) clusterMap.get(measValueIndex));
							log.trace("origValue ====" + origValue);

						} else if (origValue != null) {

							String vector = origValue;
							if (vector.contains(",")) {
								final Map keyCounters = new HashMap(); 
								//final Map keyCounters = getKeyCounters(measurement);
								final String counterName = origMeasNameMap
										.get(measValueIndex);

								Map<String, String> tmpMap = handleVectorCounters(
										measurement, keyCounters, counterName,
										vector);
								vectorBinValueMap.put(counterName, tmpMap);

							} else if (hasFlexCounters && isFlexCounter && !flexFilterMap.isEmpty()) {
								final Iterator flexIter = flexFilterMap.keySet().iterator();
								while (flexIter.hasNext()) {
									flexMeasFile.pushData((Map) flexFilterMap.get(flexIter.next()));
								}
							} else if (hasDynCounters && isDynCounter && !dynIndexMap.isEmpty()) {
								final Iterator<String> dynIter = dynIndexMap.keySet().iterator();
								while (dynIter.hasNext()) {
									dynMeasFile.pushData(dynIndexMap.get(dynIter.next()));
								}
							} 
							
							else {
								log.log(Level.TRACE, "value" + ":" + origValue);
								measFile.pushData(
										(String) measNameMap.get(measValueIndex),
										origValue);
								measFile.pushData("clusterId",
										(String) clusterMap.get(measValueIndex));
								measFile.pushData(
										origMeasNameMap.get(measValueIndex),
										origValue);

							}

						}
					} else {
						log.warn("Data contains one or more r-tags than mt-tags");
					}
				}

			} else if (qName.equals("suspect")) {
				this.suspectFlag = charValue;
			} else if (qName.equals("fileFooter")) {
			}
		}

		private void handleMoidMap(String moid) {

			log.log(Level.TRACE, "Loader Name: " + moidMap.size());
			moid = moid.toUpperCase();

			if (moidMap.containsKey(moid)) {
				counterMap = moidMap.get(moid);
				log.log(Level.TRACE, "Loader Name: " + moidMap.get(moid));
				counterMap.putAll(counterValueMap);
				moidMap.put(moid, counterMap);
			} else {
				moidMap.put(moid, counterValueMap);
			}

			if (hasFlexCounters && !flexFilterMap.isEmpty()) {
				if(flexMoidMap.containsKey(moid)){
					HashMap FlexMap = (HashMap) flexMoidMap.get(moid);
					for(Entry<String, Map> entry : flexFilterMap.entrySet()){
						String key = (String) entry.getKey();
						HashMap<String,Object> value = (HashMap<String, Object>) entry.getValue();
						if(FlexMap.containsKey(key)){
							HashMap<String,Object> newFlexMap =  (HashMap<String, Object>) FlexMap.get(key);
							newFlexMap.putAll(value);
							FlexMap.put(key, newFlexMap);
						}else{
							FlexMap.put(key, value);
						}
					}      			
					flexMoidMap.put(moid,FlexMap);
				}
				else{
					flexMoidMap.put(moid,flexFilterMap);
				}
			}
			else{
				log.trace("flexfiltermap is empty");
			}
			
			if (hasDynCounters && !dynIndexMap.isEmpty()) {
				dynMoidMap.put(moid, dynIndexMap);
			}
			
			final HashMap<String, String> rowMap = new HashMap<String, String>();

			Iterator iter = moidMap.entrySet().iterator();
			if (iter == null) {
				log.log(Level.TRACE, "Loader Name:is null");
			}

			rowMap.put("PERIOD_DURATION", granularityPeriodDuration);
			log.log(Level.TRACE, "PERIOD_DURATION: " + granularityPeriodDuration);
			rowMap.put("repPeriodDuration", repPeriodDuration);
			log.log(Level.TRACE, "repPeriodDuration: " + repPeriodDuration);
			// DATETIME_ID calculated from end time
			final String begin = calculateBegintime();
			if (begin != null) {
				rowMap.put("DATETIME_ID", begin);
				log.log(Level.TRACE, "DATETIME_ID: " + begin);
			}
			rowMap.put("collectionBeginTime", collectionBeginTime);
			log.log(Level.TRACE, "collectionBeginTime: " + collectionBeginTime);
			rowMap.put("DC_SUSPECTFLAG", suspectFlag);
			log.log(Level.TRACE, "DC_SUSPECTFLAG: " + suspectFlag);
			rowMap.put("filename",
					(sf == null ? "dummyfile" : sf.getName()));
			log.log(Level.TRACE, "filename: "
					+ (sf == null ? "dummyfile" : sf.getName()));
			rowMap.put("JVM_TIMEZONE", JVM_TIMEZONE);
			log.log(Level.TRACE, "JVM_TIMEZONE: " + JVM_TIMEZONE);
			rowMap.put("DIRNAME",
					(sf == null ? "dummydir" : sf.getDir()));
			log.log(Level.TRACE, "DIRNAME: "
					+ (sf == null ? "dummydir" : sf.getDir()));
			rowMap.put("measInfoId", measInfoId);
			log.log(Level.TRACE, "measInfoId: " + measInfoId);
			rowMap.put("MOID", measObjLdn);
			log.log(Level.TRACE, "MOID: " + measObjLdn);
			rowMap.put("objectClass", objectClass);
			log.log(Level.TRACE, "objectClass: " + objectClass);
			rowMap.put("vendorName", vendorName);
			log.log(Level.TRACE, "vendorName: " + vendorName);
			rowMap.put("fileFormatVersion", fileFormatVersion);
			log.log(Level.TRACE, "fileFormatVersion: " + fileFormatVersion);
			rowMap.put("dnPrefix", dnPrefix);
			log.log(Level.TRACE, "dnPrefix: " + dnPrefix);
			rowMap.put("localDn", fsLocalDN);
			log.log(Level.TRACE, "localDn: " + fsLocalDN);
			rowMap.put("managedElementLocalDn", meLocalDN);
			log.log(Level.TRACE, "managedElementLocalDn: " + meLocalDN);
			rowMap.put("elementType", elementType);
			log.log(Level.TRACE, "elementType: " + elementType);
			rowMap.put("userLabel", userLabel);
			log.log(Level.TRACE, "userLabel: " + userLabel);
			rowMap.put("swVersion", swVersion);
			log.log(Level.TRACE, "swVersion: " + swVersion);
			// collectionEndTime received so late, that migth not be used
			rowMap.put("endTime", granularityPeriodEndTime);
			log.log(Level.TRACE, "endTime: " + granularityPeriodEndTime);
			rowMap.put("jobId", jobId);
			log.log(Level.TRACE, "jobId: " + jobId);
			log.log(Level.TRACE, "objectmap before accessing : "+objectMap);
			final String loadname = objectMap.get(objectClass);
			
			log.log(Level.TRACE, "Before forming composite key : moid: " + moid + " loaderName : "+loadname);
			dataMap.put(moid + loadname, rowMap);

			Map<String, Map<String, String>> vectorBinValueMap1 = new HashMap<String, Map<String, String>>();
			for (String counterName1 : vectorBinValueMap.keySet()) {
				vectorBinValueMap1.put(counterName1,
						vectorBinValueMap.get(counterName1));
			}
			vectorBinValueMap.clear();

			if (vectorData.containsKey(measObjLdn)) {

				Map<String, Map<String, String>> vectorBinValueMap2 = new HashMap<String, Map<String, String>>();
				vectorBinValueMap2 = vectorData.get(measObjLdn);
				vectorBinValueMap1.putAll(vectorBinValueMap2);
				vectorBinValueMap2.clear();
			}
			vectorData.put(measObjLdn, vectorBinValueMap1);
			measInfoAndLdn.put(objectClass + "-" + measObjLdn, objectClass);

		}

		private void handleTAGMeasData() throws SAXException {

			if (!loaderClass.isEmpty() && !objectMap.isEmpty()) {

				//final HashMap<String, String> objectMapClone = new HashMap<String, String>();
				final HashMap<String, ArrayList<String>> objectMapClone = new HashMap<String, ArrayList<String>>();
				final Iterator<String> objectIter = objectMap.keySet().iterator();
				
				while (objectIter.hasNext()) {
					final String tagID = objectIter.next();
					final String loader = objectMap.get(tagID);
					log.log(Level.TRACE, "Loader:", loader);
					
					if(!objectMapClone.containsKey(loader)){
						ArrayList<String> objectMapClonelist=new ArrayList<String>();
						objectMapClone.put(loader,objectMapClonelist);
					}
					objectMapClone.get(loader).add(tagID);
					
				}
				

				final Iterator<String> loadIter = loaderClass.keySet().iterator();
				log.log(Level.TRACE, "loader class keys : "+loaderClass.keySet());

				while (loadIter.hasNext()) {
					final String loaderName = loadIter.next();
					final HashMap<String, HashMap<String, String>> moid = loaderClass
							.get(loaderName);
					boolean flexFlag=false;
					boolean dynFlag=false;
					boolean mainFlag=false;
					Map<String,String> vectorMap = new HashMap<>();
					ArrayList<String> objectClassCloneList = objectMapClone.get(loaderName);
					for(String 	objectClassClone : 	objectClassCloneList)
					{
						log.log(Level.TRACE, "objectClassClone : "+objectClassClone);
						DFormat df = dfc.getDataFormatByInterfaceAndTagId(interfaceName,
											objectClassClone + vectorPostfix);

					if (df != null) {
						try {

							log.log(Level.TRACE,
									"Creating MeasureMentFile for TagID "
											+ objectClassClone + vectorPostfix
											+ "and Interface Name" + interfaceName);

							vectorMeasFile = getChannel(objectClassClone + vectorPostfix);
						} catch (final Exception e) {
							log.log(Level.TRACE,
									"Error opening vector measurement data", e);
							e.printStackTrace();
							throw new SAXException(
									"Error opening vector measurement data: "
											+ e.getMessage(), e);
						}
						for (String counterName1 : vectorData.keySet()) {
							
							if(objectClassClone.equals(measInfoAndLdn.get(objectClassClone + "-" + counterName1))){
								log.log(Level.TRACE, "is a vector MO");
								Map<String, Map<String, String>> vectorBinValueMap = new HashMap<>();
								Map<String, String> value1 = new HashMap<>();
								int maxBinSize = 0;
								vectorBinValueMap = vectorData.get(counterName1);
								for (String counterName : vectorBinValueMap
										.keySet()) {
									int binSize = vectorBinValueMap
											.get(counterName).size();
									if (binSize > maxBinSize) {
										maxBinSize = binSize;
									}

								}
								int maxval = 0;
								for (String counterName : vectorBinValueMap
										.keySet()) {
									value1 = vectorBinValueMap.get(counterName);
									for (String i2 : value1.keySet()) {
										if (i2 != null || i2 != "0") {
											int binval = Integer.parseInt(i2);
											if (binval > maxval) {
												maxval = binval;
											}
										}
									}
								}
								final String begin1 = calculateBegintime();
								String checkZero = "NIL";

								for (int i = 0; i <= maxval; i++) {
									String binIndex = i + "";
									int flag = 0;
									Map<String, String> value = new HashMap<String, String>();
									for (String counterName : vectorBinValueMap
											.keySet()) {
										value = vectorBinValueMap.get(counterName);

										if (value.containsKey(binIndex)
												&& binIndex != null) {
											if (flag == 0) {
												flag = 1;
											}
											if (checkZero.equals(value
													.get(binIndex))) {
												vectorMeasFile.pushData(counterName,
														null);
											} else {
												vectorMap.put(counterName,value.get(binIndex));
											}
											log.log(Level.TRACE,
													"Q:CounterName" + counterName
															+ "Value"
															+ value.get(binIndex));
											log.trace("DCVECTOR_INDEX" + binIndex);
											
											vectorMap.put("DCVECTOR_INDEX", binIndex);
											vectorMap.put(counterName + rangePostfix, binIndex);
											vectorMap.put("MOID",counterName1);
											vectorMap.putAll(addValues());

										}
										vectorMeasFile.pushData(vectorMap);
									}
									try {
										if (flag == 1) {
										//	vectorMeasFile.saveData();
										}
									} catch (final Exception e) {
										log.log(Level.WARN,
												"Error saving measurement data", e);
										e.printStackTrace();
										throw new SAXException(
												"Error saving measurement data: "
														+ e.getMessage(), e);

									}
								}
							}

						}

							if ((vectorMeasFile != null)) {
							try {
								//vectorMeasFile.close();
							} catch (final Exception e) {
								log.log(Level.WARN,
										"Error closing measurement File", e);
								e.printStackTrace();
								throw new SAXException(
										"Error closing Measurement File: "
												+ e.getMessage(), e);
							}
						}
					}
					if (hasFlexCounters) {

						DFormat dff = dfc.getDataFormatByInterfaceAndTagId(interfaceName,
								objectClassClone + flexPostfix);
						if (dff != null&&!flexFlag) {
							if (createOwnFlexFile && flexMoidMap != null) {
								flexFlag=true;
								log.log(Level.TRACE,
										"Creating FLEX MeasureMentFile for TagID "
												+ objectClassClone + flexPostfix
												+ "and Interface Name"
												+ interfaceName);
								try {

									flexMeasFile = getChannel(objectClassClone+ flexPostfix);
								} catch (final Exception e) {
									log.log(Level.WARN,
											"Error opening measurement data", e);
									e.printStackTrace();
									throw new SAXException(
											"Error opening measurement data: "
													+ e.getMessage(), e);
								}

								final Iterator<String> moidIter = moid.keySet()
										.iterator();
								while (moidIter.hasNext()) {
									final String moidName = moidIter.next();
									final HashMap filterMap = (HashMap) flexMoidMap.get(moidName);
									
								try{	
									final Iterator filterIter = filterMap.keySet().iterator();
									while (filterIter.hasNext()) {
										final HashMap valMap = (HashMap) filterMap
												.get(filterIter.next());
										flexMeasFile.pushData(valMap);
										try {
										//	flexMeasFile.saveData();
										} catch (final Exception e) {
											log.log(Level.TRACE,"Error saving measurement data",e);
											e.printStackTrace();
											throw new SAXException("Error saving measurement data: " + e.getMessage(), e);
										}
									}
								}
								catch(final Exception e)
								{
									
									log.log(Level.TRACE, "key is not found in flexMoidMap,for Key : "+moidName);
								}
								}
									if ((flexMeasFile != null)) {
									try {
									//	flexMeasFile.close();
									} catch (final Exception e) {
										log.log(Level.TRACE,
												"Error closing measurement File", e);
										e.printStackTrace();
										throw new SAXException(
												"Error closing Measurement File: "
														+ e.getMessage(), e);
									}
								}
							}
						}

					}
					
					if (hasDynCounters) {

						DFormat dff = dfc.getDataFormatByInterfaceAndTagId(interfaceName,
								objectClassClone + dynPostfix);
						if (dff != null&&!dynFlag) {
							if (createOwnDynFile && dynMoidMap != null) {
								dynFlag=true;
								log.log(Level.TRACE,
										"Creating DYN MeasureMentFile for TagID "
												+ objectClassClone + dynPostfix
												+ "and Interface Name"
												+ interfaceName);
								try {

									dynMeasFile = getChannel(objectClassClone+ dynPostfix);
								} catch (final Exception e) {
									log.log(Level.WARN,
											"Error opening measurement data", e);
									e.printStackTrace();
									throw new SAXException(
											"Error opening measurement data: "
													+ e.getMessage(), e);
								}

								final Iterator<String> moidIter = moid.keySet()
										.iterator();
								while (moidIter.hasNext()) {
									final String moidName = moidIter.next();
									final Map<String, Map<String, String>> indexMap =  dynMoidMap.get(moidName);
									
								try{	
									final Iterator<String> indexIter = indexMap.keySet().iterator();
									while (indexIter.hasNext()) {
										final Map<String, String> valMap = indexMap
												.get(indexIter.next());
										dynMeasFile.pushData(valMap);
										try {
										//	dynMeasFile.saveData();
										} catch (final Exception e) {
											log.log(Level.TRACE,"Error saving measurement data",e);
											e.printStackTrace();
											throw new SAXException("Error saving measurement data: " + e.getMessage(), e);
										}
									}
								}
								catch(final Exception e)
								{
									
									log.log(Level.TRACE, "key is not found in dynMoidMap,for Key : "+moidName);
								}
								}
									if ((dynMeasFile != null)) {
									try {
									//	dynMeasFile.close();
									} catch (final Exception e) {
										log.log(Level.TRACE,
												"Error closing measurement File", e);
										e.printStackTrace();
										throw new SAXException(
												"Error closing Measurement File: "
														+ e.getMessage(), e);
									}
								}
							}
						}

					}
					
					DFormat dfm = dfc.getDataFormatByInterfaceAndTagId(interfaceName,
							objectClassClone);

					if (dfm != null&&!mainFlag) {
						Map<String,String> measMap = new HashMap<>();
						mainFlag=true;
						log.log(Level.TRACE, "Creating MeasureMentFile for TagID "
								+ objectClassClone + "and Interface Name"
								+ interfaceName);
						try {

							measFile = getChannel(objectClassClone);
						} catch (final Exception e) {
							log.log(Level.WARN,
									"Error opening measurement data", e);
							e.printStackTrace();
							throw new SAXException(
									"Error opening measurement data: "
											+ e.getMessage(), e);
						}

						final Iterator<String> moidIter = moid.keySet().iterator();
						while (moidIter.hasNext()) {
							final String moidName = moidIter.next();
							final HashMap<String, String> counter = moid
									.get(moidName);

							log.log(Level.TRACE, " saving from dataMap : moid: " + moidName + " loaderName : "+loaderName);
							measMap.putAll(counter);
							measMap.putAll(dataMap.get(moidName + loaderName));
							try {
						//		measFile.saveData();
							} catch (final Exception e) {
								log.log(Level.TRACE,
										"Error saving measurement data", e);
								e.printStackTrace();
								throw new SAXException(
										"Error saving measurement data: "
												+ e.getMessage(), e);

							}

						}
						measFile.pushData(measMap);
							if ((measFile != null)) {
							try {
							//	measFile.close();
							} catch (final Exception e) {
								log.log(Level.TRACE,
										"Error closing measurement File", e);
								e.printStackTrace();
								throw new SAXException(
										"Error closing Measurement File: "
												+ e.getMessage(), e);
							}
						}
					}
				  }
				  objectClassCloneList.clear();
				}
				vectorData.clear();
				measInfoAndLdn.clear();
				objectMapClone.clear();
				moidMap.clear();
				loaderClass.clear();
			}
			

		}

		private Channel getChannel(String tagId) {
			try {
				String folderName = MeasurementFileFactory.getFolderName(sf, interfacename, tagId, log);
				Channel channel;
				if (folderName != null) {
					if ((channel = channelMap.get(folderName)) != null) {
						return channel;
					}
					channel = MeasurementFileFactory.createChannel(sf, tagId, folderName, log);
					channelMap.put(folderName, channel);
					return channel;
				}
			} catch (Exception e) {
				log.log(Level.WARN, "Exception while getting channel : ", e);
			}
			return null;
		}
		private String calculateBegintime() {
			String result = null;
			try {
				String granPeriodETime = granularityPeriodEndTime;
				if (granPeriodETime.matches(".+\\+\\d\\d(:)\\d\\d")
						|| granPeriodETime.matches(".+\\-\\d\\d(:)\\d\\d")) {
					granPeriodETime = granularityPeriodEndTime.substring(0,
							granularityPeriodEndTime.lastIndexOf(":"))
							+ granularityPeriodEndTime
									.substring(granularityPeriodEndTime
											.lastIndexOf(":") + 1);
				}
				granPeriodETime = granPeriodETime.replaceAll("[.]\\d{3}", "");
				if (granPeriodETime.endsWith("Z")) {
					granPeriodETime = granPeriodETime.replaceAll("Z", "+0000");
				}

				final Date end = simpleDateFormat.parse(granPeriodETime);
				final Calendar cal = Calendar.getInstance();
				cal.setTime(end);
				final int period = Integer.parseInt(granularityPeriodDuration);
				cal.add(Calendar.SECOND, -period);
				result = simpleDateFormat.format(cal.getTime());
			} catch (final ParseException e) {
				log.log(Level.WARN, "Worker parser failed to exception", e);
			} catch (final NumberFormatException e) {
				log.log(Level.WARN, "Worker parser failed to exception", e);
			} catch (final NullPointerException e) {
				log.log(Level.WARN, "Worker parser failed to exception", e);
			}
			return result;
		}


		/**
		 * Extracts a substring from given string based on given regExp
		 * 
		 */
		public String parseFileName(final String str, final String regExp) {

			final Pattern pattern = Pattern.compile(regExp);
			final Matcher matcher = pattern.matcher(str);
			String result = "";

			if (matchObjectMaskAgainst.equalsIgnoreCase("whole")) {
				// Find a match between regExp and whole of str, and return group 1
				// as result
				if (matcher.matches()) {
					result = matcher.group(1);
					log.trace(" regExp (" + regExp + ") found from " + str + "  :"
							+ result);
				} else {
					log.warn("String " + str + " doesn't match defined regExp "
							+ regExp);
				}
			} else {
				// Find a match between regExp and return group 1 as result.
				if (matcher.find()) {
					result = matcher.group(1);
					log.trace(" regExp (" + regExp + ") found from subset of "
							+ str + "  :" + result);
				} else {
				log.warn("No subset of String " + str
								+ " matchs defined regExp " + regExp);
					
					
				}
			}

			return result;

		}

		@Override
		public void characters(final char[] ch, final int start, final int length)
				throws SAXException {
			for (int i = start; i < (start + length); i++) {
				// If no control char
				if ((ch[i] != '\\') && (ch[i] != '\n') && (ch[i] != '\r')
						&& (ch[i] != '\t')) {
					charValue += ch[i];
				}
			}
		}

		/**
		 * This method handleVectorCounters is used to handle the vector counters.
		 * 
		 * @input Input to the handleVectorCounters is the
		 *        counterName,counterValues,MeasurementFile
		 * @return Output is a Map containing the bins and the value for bins.
		 */
		final private Map<String, String> handleVectorCounters(final Map datarow,
				final Map keyMap, String counter, String value) {

			final Map<String, String> tmpMap = new TreeMap<String, String>();
			final Map<String, Map<String, String>> tmpMap1 = new HashMap<String, Map<String, String>>();
			final Map newtmpMap = new HashMap();
			List<Integer> rangeindex = new ArrayList<Integer>();
			final Map compressvectorkeymap = new HashMap();
			final Map compressvectorvaluemap = new HashMap();
			final Map cmvectorkeymap = new HashMap();
			final Map cmvectorvaluemap = new HashMap();
			try {
				int max = 0;
				String tmp = value;

				// get VECTOR counters
				final Set rangeCounters = ((Set) datarow.get("rangeCounters"));
				log.log(Level.TRACE, "T:Range Counters Set : " + rangeCounters
						+ "<<");

				Iterator iter = rangeCounters.iterator();
				while (iter.hasNext()) {

					final String key = (String) iter.next();
					log.log(Level.TRACE, "T:Key: " + key);

					if (counter.equals(key)) {

						// BREAK-OUT THE RANGE (VECTOR) INTO AN ArrayList:
						final StringTokenizer tokens = new StringTokenizer(value,
								delimiter, true);
						List<String> bins = new ArrayList(tokens.countTokens() + 1); // We
																						// do
																						// not
																						// know
																						// capacity
																						// needed,
																						// so
																						// make
																						// it
																						// bigger
																						// then
																						// needed
																						// (and
																						// reduce
																						// later).
						boolean prevTokenWasDelim = true;
						String currentToken;
						while (tokens.hasMoreTokens()) {
							currentToken = tokens.nextToken();
							if (!currentToken.equalsIgnoreCase(delimiter)) {
								if (currentToken.equalsIgnoreCase("Nil")
										|| currentToken.trim().equalsIgnoreCase("")) {
									currentToken = null;
									log.trace("delimiter:: " + delimiter);
								}
								bins.add(currentToken); // It's not a delimiter so
														// add it.
								prevTokenWasDelim = false;
							} else if (!prevTokenWasDelim) {
								prevTokenWasDelim = true; // It's a delimiter so we
															// don't add anything
							} else {
								bins.add(null); // It's a delimiter AND SO WAS THE
												// LAST ONE. This represents empty
												// input - so we add null.
							}
						}
						if (prevTokenWasDelim) { // This accounts for empty bin at
													// end of vector.
							bins.add(null);
						}
						((ArrayList) bins).trimToSize(); // Set the capacity of the
															// ArrayList to it's
															// size.

						// DECOMPRESS THE VECTOR IF REQUIRED:
						if (compressedVectorCounters.contains(key)) {
							rangeindex = getrangeindexfromcompressVector(bins);
							bins = getvaluesfromcompressVector(bins);
							compressvectorkeymap.put(key, rangeindex);
							if ((null == bins) || (null == rangeindex)) {
								log.trace("Vector " + key
										+ " is not having valid data.");
								datarow.put(key, null);
								continue;
							}
						}
						log.trace("rangeindex" + rangeindex);
						log.trace("bins" + bins);
						newtmpMap.put(key, bins);
						// COLLECT VECTOR IN A HashMap
						// tmpMap.put(key, bins);

						// IS IT THE LONGEST VECTOR SO FAR?
						if (bins.size() > max) {
							max = bins.size();
						}

						// IF IT'S REQUIRED, INSERT THE 1ST VALUE OF VECTOR (THE
						// ZERO INDEX) INTO datarow
						if (!tmpMap.isEmpty()) {
							datarow.put(key, bins.get(0));
						}
						iter = newtmpMap.keySet().iterator();
						List oldcompressvectorlist = new ArrayList();
						TreeSet<Integer> keylist = new TreeSet<Integer>();
						int compressvectorlen = -1;
						List<Integer> compressvectorkeylist = new ArrayList();
						// loop all range counters (columns)
						while (iter.hasNext()) {

							final String key1 = (String) iter.next();

							if (compressedVectorCounters.contains(key1)) {
								oldcompressvectorlist = (List) newtmpMap.get(key1);
								compressvectorkeylist = (List) compressvectorkeymap
										.get(key1);
								keylist.addAll(compressvectorkeylist);
								// Instanciate ArrayList to be returned (with
								// required capacity) and fill it with zeros
								List<String> result = new ArrayList();
								log.trace("max:" + max);
								/*
								 * for(int i=0;i<max;i++){ tmpMap.put(i+"", null);
								 * result.add(null); }
								 */
								log.trace("oldcompressvectorlist:"
										+ oldcompressvectorlist);
								log.trace("compressvectorkeylist:"
										+ compressvectorkeylist);
								// Add the values from input into the correct
								// position in the returned ArrayList (according to
								// their corresponding index)
								/*
								 * Iterator compressIter =
								 * compressvectorkeylist.iterator();
								 * while(compressIter.hasNext()){ int i =(Integer)
								 * compressIter.next(); log.info(i+"");
								 * tmpMap.put(compressvectorkeylist.get(i)+"",
								 * (String)oldcompressvectorlist.get(i)); }
								 */
								for (int i = 0; i < oldcompressvectorlist.size(); i++) {
									tmpMap.put(compressvectorkeylist.get(i) + "",
											(String) oldcompressvectorlist.get(i));
									/*
									 * if ( compressvectorkeylist.get(i)< max){
									 * tmpMap.put(compressvectorkeylist.get(i)+"",
									 * (String)oldcompressvectorlist.get(i));
									 * result.set(compressvectorkeylist.get(i),
									 * (String)oldcompressvectorlist.get(i)); }
									 * else{
									 * tmpMap.put(compressvectorkeylist.get(i)+"",
									 * (String)oldcompressvectorlist.get(i));
									 * result.
									 * add((String)oldcompressvectorlist.get(i)); }
									 */
								}
								if (result.size() > compressvectorlen) {
									compressvectorlen = result.size();
								}
								// if(result.size() > max){
								// max=result.size();
								// }
								newtmpMap.put(key1, result);
								tmpMap1.put(key1, tmpMap);
								compressvectorvaluemap.put(key1,
										oldcompressvectorlist);
							} else {
								for (int i = 0; i < max; i++) {
									tmpMap.put(i + "", (String) bins.get(i));
								}
							}
						}

						/*
						 * for(int i=0;i<bins.size();i++){ tmpMap.put(i+"",
						 * bins.get(i)+""); }
						 */

					}

				}

				// / get CMVECTOR counters
				final Set cmVectorCounters = ((Set) datarow.get("cmVectorCounters"));
				log.log(Level.TRACE, "CM Counters Set : " + cmVectorCounters
						+ "<<");

				// loop all CMVECTOR counters in this datarow
				Iterator cmIter = cmVectorCounters.iterator();
				while (cmIter.hasNext()) {
					final String key1 = (String) cmIter.next();
					if (counter.equals(key1)) {
						// musta add one delim to the end to make it work...
						tmp += delimiter;
						int i = 0;
						boolean prewWasDelim = true;
						final List list = new ArrayList();
						final StringTokenizer token = new StringTokenizer(tmp,
								delimiter, true);
						log.trace("Printing the tmp....." + tmp);
						List<String> bins = new ArrayList(token.countTokens() + 1);
						log.trace("Iterating through the whil for bins......");
						while (token.hasMoreTokens()) {

							final String tmptoken = token.nextToken();
							String tmpvalue = null;
							log.trace("Printing the token   " + tmptoken);
							if (prewWasDelim
									|| (tmptoken.equalsIgnoreCase(delimiter) && prewWasDelim)) {
								if (!tmptoken.equalsIgnoreCase(delimiter)) {
									if (tmptoken.equalsIgnoreCase("Nil")
											|| tmptoken.trim().equalsIgnoreCase("")) {
										log.trace("Nil or space has been occured.....");
										tmpvalue = null;
									} else {
										tmpvalue = tmptoken;
									}
								}

								log.trace("Value added to bins   " + tmptoken);
								bins.add(tmpvalue);
								i++;
							}

							prewWasDelim = false;
							if (tmptoken.equalsIgnoreCase(delimiter)) {
								prewWasDelim = true;
							}
						}

						if (cmVectorCounters.contains(key1)) {
							log.trace("In new tmp map  " + key1 + "  " + bins);
							newtmpMap.put(key1, bins);
							if ((null == bins) || (null == rangeindex)) {
								log.trace("Vector " + key1
										+ " is not having valid data.");
								datarow.put(key1, null);
								continue;
							}

						}

						if (!bins.isEmpty()) {
							log.trace("To set null at 0 index");
							// put the value from the first (zero) index into the
							// original datarow because CMVECTORs are on hand
							counterValueMap.put(
									(String) measNameMap.get(measValueIndex),
									bins.get(0));
							bins.set(0, null);
						}

						iter = newtmpMap.keySet().iterator();

						List oldcmvectorlist = new ArrayList();
						TreeSet<Integer> keylist = new TreeSet<Integer>();
						int cmvectorlen = -1;
						List<Integer> cmvectorkeylist = new ArrayList();

						while (iter.hasNext()) {
							final String key2 = (String) iter.next();
							if (cmVectorCounters.contains(key2)) {
								oldcmvectorlist = (List) newtmpMap.get(key2);

								// Instanciate ArrayList to be returned (with
								// required capacity) and fill it with zeros
								List<String> result = new ArrayList();
								log.trace("max:" + max);
								log.trace("oldcmvectorlist:" + oldcmvectorlist);

								for (int i2 = 1; i2 < oldcmvectorlist.size(); i2++) {
									tmpMap.put(i2 + "",
											(String) oldcmvectorlist.get(i2));
									log.trace("Printing the final map  " + i2 + "  "
											+ (String) oldcmvectorlist.get(i2));
								}

								if (result.size() > cmvectorlen) {
									cmvectorlen = result.size();
								}

								newtmpMap.put(key1, result);
								tmpMap1.put(key1, tmpMap);
								cmvectorvaluemap.put(key1, oldcmvectorlist);
							} else {
								for (int i1 = 0; i1 < max; i1++) {
									tmpMap.put(i1 + "", (String) bins.get(i1));

								}
							}
						}

					}

				}

			} catch (NullPointerException ne) {
				log.log(Level.TRACE, "Counter values are not present in database");
			} finally {
				return tmpMap;
			}
		}

		public Map addValues() {
			measurement = new HashMap();
			measurement.put("PERIOD_DURATION", granularityPeriodDuration);
			log.log(Level.TRACE, "PERIOD_DURATION: " + granularityPeriodDuration);
			measurement.put("repPeriodDuration", repPeriodDuration);
			log.log(Level.TRACE, "repPeriodDuration: " + repPeriodDuration);
			final String begin = calculateBegintime();
			if (begin != null) {
				measurement.put("DATETIME_ID", begin);
				log.log(Level.TRACE, "DATETIME_ID: " + begin);
			}
			measurement.put("collectionBeginTime", collectionBeginTime);
			log.log(Level.TRACE, "collectionBeginTime: " + collectionBeginTime);
			// measurement.put("DC_SUSPECTFLAG", suspectFlag);
			// log.log(Level.FINEST, "T:DC_SUSPECTFLAG: " + suspectFlag
			// +objectClass);
			measurement.put("filename", (sf == null ? "dummyfile"
					: sf.getName()));
			log.log(Level.TRACE, "filename: "
					+ (sf == null ? "dummyfile" : sf.getName()));
			measurement.put("JVM_TIMEZONE", JVM_TIMEZONE);
			log.log(Level.TRACE, "JVM_TIMEZONE: " + JVM_TIMEZONE);
			measurement.put("DIRNAME", (sf == null ? "dummydir"
					: sf.getDir()));
			log.log(Level.TRACE, "DIRNAME: "
					+ (sf == null ? "dummydir" : sf.getDir()));
			measurement.put("measInfoId", measInfoId);
			log.log(Level.TRACE, "measInfoId: " + measInfoId);
			measurement.put("objectClass", objectClass);
			log.log(Level.TRACE, "objectClass: " + objectClass);
			measurement.put("vendorName", vendorName);
			log.log(Level.TRACE, "vendorName: " + vendorName);
			measurement.put("fileFormatVersion", fileFormatVersion);
			log.log(Level.TRACE, "fileFormatVersion: " + fileFormatVersion);
			measurement.put("dnPrefix", dnPrefix);
			log.log(Level.TRACE, "dnPrefix: " + dnPrefix);
			measurement.put("localDn", fsLocalDN);
			log.log(Level.TRACE, "localDn: " + fsLocalDN);
			measurement.put("managedElementLocalDn", meLocalDN);
			log.log(Level.TRACE, "managedElementLocalDn: " + meLocalDN);
			measurement.put("elementType", elementType);
			log.log(Level.TRACE, "elementType: " + elementType);
			measurement.put("userLabel", userLabel);
			log.log(Level.TRACE, "userLabel: " + userLabel);
			measurement.put("swVersion", swVersion);
			log.log(Level.TRACE, "swVersion: " + swVersion);
			measurement.put("endTime", granularityPeriodEndTime);
			log.log(Level.TRACE, "endTime: " + granularityPeriodEndTime);
			measurement.put("jobId", jobId);
			log.log(Level.TRACE, "jobId: " + jobId);
			measurement.put("filename", sf.getName());
			measurement.put("JVM_TIMEZONE", JVM_TIMEZONE);
			measurement.put("DIRNAME", sf.getDir());

			return measurement;
		}

		public List getrangeindexfromcompressVector(List input) {

			if (null == input || input.get(0) == null) { // HQ59381 fix for null
															// pointer exception
				return null;
			}

			int inputSize = input.size();

			if (input.isEmpty() || (input.get(0).equals("") && inputSize == 1)) {
				return input; // Return input List if it is empty
			}

			final int expectedNumOfPairs;
			try {
				expectedNumOfPairs = Integer.parseInt((String) input.get(0)); // Find
																				// how
																				// many
																				// index
																				// value
																				// pairs
																				// there
																				// are
																				// in
																				// List.
			} catch (Exception e) {
				return null;
			}

			if (expectedNumOfPairs > 1024) { // Make sure it's not too big
				return null;
			}
			if (expectedNumOfPairs == 0 && inputSize == 1) {
				return null; // return input if it just has zero
			}
			if (inputSize % 2 == 0) { // Make sure it has odd size
				return null;
			}
			if (inputSize != (expectedNumOfPairs * 2) + 1) { // Check for correct
																// num of name-value
																// pairs. This also
																// catches negative
																// expectedNumOfPairs
				return null;
			}

			// This FOR loop does 3 things: sanity checks the indecies, finds the
			// highest one and puts them in their own ArrayList,
			int index;
			int highestIndex = -1;
			List<Integer> indecies = new ArrayList(expectedNumOfPairs);
			for (int i = 1; i < inputSize; i = i + 2) { // Takes the indecies from
														// input List and put the in
														// their own list
				try {
					index = Integer.parseInt((String) input.get(i));

				} catch (Exception e) {
					return null;
				}
				if (index < 0) {
					return null;
				}
				indecies.add(index);
				if (index > highestIndex) {
					highestIndex = index;
				}
			}
			if (highestIndex > 1024) { // Make sure highest index is not too big
				return null;
			}

			return indecies;
		}

		/**
		 * This method decompresses (decodes) a compressed vector.
		 * 
		 * @input A compressed vector as a List: first entry in list indicates
		 *        number of indexes, and following entries are alternatly an index
		 *        and a value.
		 * @return A decompressed (decoded) version of the input as a List,
		 *         containing the values in their corect position.
		 */
		public List getvaluesfromcompressVector(List input) {

			if (null == input || input.get(0) == null) { // HQ59381 fix for null
															// pointer exception
				return null;
			}

			int inputSize = input.size();

			if (input.isEmpty() || (input.get(0).equals("") && inputSize == 1)) {
				return input; // Return input List if it is empty
			}

			final int expectedNumOfPairs;
			try {
				expectedNumOfPairs = Integer.parseInt((String) input.get(0)); // Find
																				// how
																				// many
																				// index
																				// value
																				// pairs
																				// there
																				// are
																				// in
																				// List.
			} catch (Exception e) {
				return null;
			}

			if (expectedNumOfPairs > 1024) { // Make sure it's not too big
				return null;
			}
			if (expectedNumOfPairs == 0 && inputSize == 1) {
				return input; // return input if it just has zero
			}
			if (inputSize % 2 == 0) { // Make sure it has odd size
				return null;
			}
			if (inputSize != (expectedNumOfPairs * 2) + 1) { // Check for correct
																// num of name-value
																// pairs. This also
																// catches negative
																// expectedNumOfPairs
				return null;
			}

			// This FOR loop does 3 things: sanity checks the indecies, finds the
			// highest one and puts them in their own ArrayList,
			int index;
			int highestIndex = -1;
			List<Integer> indecies = new ArrayList(expectedNumOfPairs);
			for (int i = 1; i < inputSize; i = i + 2) { // Takes the indecies from
														// input List and put the in
														// their own list
				try {
					index = Integer.parseInt((String) input.get(i));

				} catch (Exception e) {
					return null;
				}
				if (index < 0) {
					return null;
				}
				indecies.add(index);
				if (index > highestIndex) {
					highestIndex = index;
				}
			}
			if (highestIndex > 1024) { // Make sure highest index is not too big
				return null;
			}

			// Instanciate ArrayList to be returned (with required capacity) and
			// fill it with zeros
			List<String> result = new ArrayList(highestIndex + 1);
			/*
			 * for(int i=0;i<=highestIndex;i++){ result.add("0"); }
			 */

			// Add the values from input into the correct position in the returned
			// ArrayList (according to their corresponding index)
			for (int i = 0; i < indecies.size(); i++) {
				// result.set(indecies.get(i), (String)input.get(i*2+2));
				result.add((String) input.get(i * 2 + 2));
			}

			return result;
		}

		// Flex specific methods

		/**
		 * Checks if a given counter is a Flex Counter
		 * 
		 */
		private boolean checkIfFlex(String counterName) {
			String counter = "";
			if (counterName.contains("_")) {
				String[] flexSplit = counterName.split("_");
				counter = flexSplit[0];
				if (((Set) flexCounterBin.get("flexCounters")).contains(counter)) {
					handlesFlex(true, counterName);
					return true;
				}
			} else {
				counter = counterName;
				if (((Set) flexCounterBin.get("flexCounters")).contains(counter)) {
					handlesFlex(false, counterName);
					return true;
				}
			}
			return false;
		}
		
		private boolean checkIfDyn(String counterName) {
			String counter = "";
			Matcher m = getMatcher(counterName, dynMask);
			if (m.matches()) {
				String dynPart = m.group(dynPartIndex);
				String dynIndex = m.group(dynIndexIndex);
				String staticPart = m.group(staticPartIndex);
				if(((Set)dynCounterBin.get("dynCounters")).contains(staticPart)) {
					handleDyn(dynPart, dynIndex, staticPart);
					return true;
				}
			}
			return false;
		}
		
		private void handleDyn(String dynPart, String dynIndex, String staticPart) {
			String counterVal = charValue;
			Map dynValueMap = dynIndexMap.get(dynIndex);
			if (dynValueMap == null) {
				dynValueMap = new HashMap<String, String>();
				dynValueMap.putAll(addValues());
			}
			dynValueMap.put(dynPart, dynIndex);
			dynValueMap.put(staticPart, counterVal);
			dynValueMap.put("DC_SUSPECTFLAG", suspectFlag);
			dynValueMap.put("MOID", measObjLdn);
			dynIndexMap.put(dynIndex, dynValueMap);
		}

		/**
		 * Handles Flex counters and populates flex counter map
		 * 
		 */

		private void handlesFlex(boolean hasFilter, String flexCounterFilter) {
			try {
				String flexCounter = "";
				String flexFilter = "";
				String flexHash = "";
				String origValue = charValue;
				if (hasFilter) {
					Matcher m = getMatcher(flexCounterFilter, FLEX_COUNTER_PATTERN);
					if (m.matches()) {
						if (m.groupCount() > 1) {
							flexCounter = m.group(1);
							flexFilter = m.group(2);
						} else {
							flexCounter = flexCounterFilter;
						}
					}
				} else {
					flexCounter = flexCounterFilter;
				}
				flexHash = Integer.toString(flexFilter.hashCode());
				log.log(Level.TRACE, " flex_counter = "+ flexCounter + " : filter = "+flexFilter);

				if ((origValue != null)
						&& (origValue.equalsIgnoreCase("NIL") || origValue.trim()
								.equalsIgnoreCase(""))) {
					origValue = null;
					log.trace("Setting the value to null as in-valid data being received from the Node");
				}

				if (!flexFilterMap.isEmpty() && flexFilterMap.containsKey(flexHash)) {
					// log.info("flexFilterMap contains flexHash : " + flexHash);

					flexValueMap = (HashMap) flexFilterMap.get(flexHash);
					// log.info("Case 2: got flexValueMap with size "+
					// flexValueMap.size()+ " value for "+ flexCounter +
					// "in flexValueMap is "
					// + flexValueMap.get(flexCounter) + " going to put value : \""
					// + origValue +"\"");

					flexValueMap.put(flexCounter, origValue);
				} else {
					flexValueMap = new HashMap();
					flexValueMap = (HashMap) addValues();
					flexValueMap.put("DC_SUSPECTFLAG", suspectFlag);
					flexValueMap.put("MOID", measObjLdn);
					flexValueMap.put("FLEX_FILTERNAME", flexFilter);
					flexValueMap.put("FLEX_FILTERHASHINDEX", flexHash);
					flexValueMap.put(flexCounter, origValue);
					// log.info("Case 1 : flexValueMap is newly created and inserted with size : "
					// +flexValueMap.size() +
					// " going to put value : \"" + origValue +"\"" +
					// " for counter : " + flexCounter);
				}
				// log.info("Putting value for filter: " + flexFilter + " : " +
				// flexHash);
				flexFilterMap.put(flexHash, flexValueMap);
				
			} catch (Exception e) {
				log.info("Exception occured trace below" + e.getMessage());
				e.getStackTrace();

			}
		}
	
}
