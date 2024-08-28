package com.ericsson.eniq.bulkcmparser.parser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xml.sax.helpers.DefaultHandler;

import com.ericsson.eniq.parser.MeasurementFileFactory;
import com.ericsson.eniq.parser.MeasurementFileFactory.Channel;
import com.ericsson.eniq.parser.SourceFile;

public class Parser extends DefaultHandler implements Callable<Boolean> {
	
	// Patterns
    private String fdnPattern = ".+,(.+)=.+";
    private String fdnValuePattern = ".+,(.+=.+)";
    private String snPattern = "(.+),ManagedElement=.+";
    private String moidPattern = ".+,(ManagedElement=.+)";
    private String extractArrayPattern = "\\{.+, (.+=\\[.+\\]), .+\\}";
    private String rncPattern = "SubNetwork=(.+),SubNetwork=.+,MeContext=(.+),ManagedElement=.+";
    private String rbsPattern = "SubNetwork=.+,SubNetwork=(.+),MeContext=(.+),ManagedElement=.+";
    private String allPattern = "SubNetwork=(.+),MeContext=(.+),ManagedElement=.+"; 
    private String meContextPattern = "MeContext=(.+),ManagedElement=.+";
    private String subNetworkPattern = "SubNetwork=(.+),ManagedElement=(.+)";
    private String aomPattern;
    private String cuPattern;
    private String releasePattern;
    private String hostnamePattern;
    private String datetimeIdPattern;
    private String ossIdPattern;
    private String timeZonePattern;
    
	// Programming constants
	private String EMPTY = "";
    private String EMPTY_TAG = "<empty>";
	private String DELIMITER = " : ";
	private String EMPTY_ARRAY = "[]";
    private String ARRAY_START = "[";
    private String ARRAY_END = "]";
    private String PARENT_START = "{";
    private String PARENT_END = "}";
    private String ARRAY_SEPRATOR = ", ";
    private String PARENT_ARRAY_SEPRATOR = "}, ";
    private String EQUALS = "=";
    private String UNDERSCORE = "_";
	
	
	private String dataValue;
	
	private Logger logger;
	
	// Identifiers
    private String snIdentifier;
    private String moidIdentifier;
    private String sequenceIdentifier;
    private String aomIdentifier;
    private String cuIdentifier;
    private String releaseIdentifier;
    private String dateTimeIdentifier;
    private String timezoneIdentifier;
    private String jvmTimezoneIdentifier;
    private String timelevelIdentifier;
    private String periodDurationIdentifier;
    private String ossIdIdentifier;
    private String sourceIdentifier;
    private String filenameIdentifier;
    private String dirnameIdentifier;
    
    // Naming constants
    private String defaultSnIdentifier = "SN";
    private String defaultFdnIdentifier = "FDN";
    private String defaultMoidIdentifier = "MOID";
    private String defaultTimezoneIdentifier = "DC_TIMEZONE";
    private String defaultJvmTimezoneIdentifier = "JVM_TIMEZONE";
    private String defaultAomIdentifier = "AOM";
    private String defaultCuIdentifier = "CU";
    private String defaultOssIdIdentifier = "OSS_ID";
    private String defaultTimelevelIdentifier = "TIMELEVEL";
    private String defaultSourceIdentifier = "DC_SOURCE";
    private String defaultReleaseIdentifier = "DC_RELEASE";
    private String defaultDateTimeIdentifier = "DATETIME_ID";
    private String defaultDirnameIdentifier = "DIRNAME";
    private String defaultFilenameIdentifier = "FILENAME";
    private String defaultPeriodDurationIdentifier = "PERIOD_DURATION";
    private String defaultSequenceIdentifier = "DCVECTOR_INDEX";
	private String elementIdentifier = "ELEMENT";
	private String elementParentIdentifier = "ELEMENTPARENT";
	private String elementTypeIdentifier = "ELEMENTTYPE";
	private String OSSRC = "OSS";
    private String RNC = "RNC";
	private String RBS = "RBS";
	private String elementType = OSSRC;
	
	private String aom;
    private String cu;
    private String release;
    private String hostname;
    private String datetimeId;
    private String timeZone;
    private String jvmTimeZone;
    private String ossId;
    private String timelevel;
    private String periodDuration;
    private String filename;
    private String dirname;
	
	private Map<String, String> dataMap = new HashMap<String, String>();
    private LinkedHashMap<Integer, Map<String, String>> vectorMap = new LinkedHashMap<Integer, Map<String, String>>();
    private HashMap<String, Pattern> elementFDNMap = new HashMap<String, Pattern>();
	private List<String> otherPatternList = new ArrayList<String>();
	
	private Map<String, Channel> channelMap = new HashMap<>();
	
    private SourceFile sf;
	private String interfaceName;
	private int status = 0;
	private long parseStartTime;
	private long fileSize = 0L;
	private long totalParseTime = 0L;
	private int fileCount = 0;
	
	Fdn fdn;
	
	public Parser(SourceFile sf,String dataValue, String interfaceName) {
		this.sf = sf;
		this.dataValue = dataValue;
		this.interfaceName = interfaceName;
		this.status = 1;
		logger = LogManager.getLogger("interfaceName"+interfaceName);
	}

	public int status() {
		return status;
	}
	@Override
	public Boolean call() throws Exception {

		try {

			this.status = 2;
			parseStartTime = System.currentTimeMillis();
				fileCount++;
				fileSize += sf.fileSize();

				try {
					parse(dataValue,sf);
				} catch (final Exception e) {
					e.printStackTrace();
				} finally {
				}
			totalParseTime = System.currentTimeMillis() - parseStartTime;
			if (totalParseTime != 0) {
				logger.info("Parsing Performance :: " + fileCount
						+ " files parsed in " + totalParseTime / 1000
						+ " sec, filesize is " + fileSize / 1000
						+ " Kb and throughput : " + (fileSize / totalParseTime)
						+ " bytes/ms.");
			}
		} catch (final Exception e) {
			logger.log(Level.WARN, "Worker parser failed to exception", e);
		} finally {
			this.status = 3;
		}
		return true;
		
	}
	public void parse(String bulkData,SourceFile sf ) {
		this.sf=sf;
		fdn = new Fdn(logger);
		getAndSetInterfaceProperties(sf);
		
		String eachLine = null;
		try(BufferedReader reader = new BufferedReader(new StringReader(bulkData))) {
		    while ((eachLine = reader.readLine()) != null ) {
		    	processData(eachLine);
		    }
		    writeDataElement();
		} catch (IOException x) {
			logger.log(Level.WARN, "IOException while reading file." , x);
		}
		
	}

	private void processData(String nextLine) {
		Scanner lineScanner = new Scanner(nextLine);
		lineScanner.useDelimiter(DELIMITER);
		if (lineScanner.hasNext()) {
			String name = lineScanner.next();
			String value = lineScanner.next();
			value = value.replaceAll("\"\"", EMPTY_TAG);
			value = value.replaceAll("\"", EMPTY);
			
			if (name.equalsIgnoreCase("managedElementType")){
				if ( value != null && value != EMPTY){
					elementType = value;
				} else {
					elementType = OSSRC;
				}
			}
			if (value.equalsIgnoreCase(EMPTY_TAG)) {
				dataMap.put(name, null);
			} else if (value.startsWith(ARRAY_START) && value.endsWith(ARRAY_END)) {
				handleArrayTag(name, value);
			} else if (value.startsWith(PARENT_START) && value.endsWith(PARENT_END)) {
				if (value.contains(ARRAY_START) && value.contains(ARRAY_END)){
					generateArrayTag(name, value);
				} else {
					handleParentChildTag(name, value, dataMap);
				}
			} else {
				dataMap.put(name, value);
			}
			
		} else {
		//	writeData();
		}
		lineScanner.close();
	}
	
		
	private void handleArrayTag(String name, String value) {
		
		if (value.equals(EMPTY_ARRAY)) {
			int indexCount = 0;
			Map<String, String> previousMap = vectorMap.get(indexCount);
			if (previousMap != null){
				previousMap.put(name, null);
				vectorMap.put(indexCount, previousMap);
			} else {
				Map<String, String> tempMap = new HashMap<String, String>();
				tempMap.put(name, null);
				vectorMap.put(indexCount, tempMap);
			}
		} else {
			value = value.replace(ARRAY_START, EMPTY).replace(ARRAY_END, EMPTY);
			if (value.contains(PARENT_START) && value.contains(PARENT_END)) {
				String[] indexValues = value.split(PARENT_ARRAY_SEPRATOR);
				int index = 0;
				for (String eachIndexValue : indexValues) {
					if (eachIndexValue.startsWith(PARENT_START) && eachIndexValue.endsWith(PARENT_END)){
						Map<String, String> previousMap = vectorMap.get(index);
						if (previousMap != null){
							handleParentChildTag(name, eachIndexValue, previousMap);
							vectorMap.put(index, previousMap);
						} else {
							Map<String, String> tempMap = new HashMap<String, String>();
							handleParentChildTag(name, eachIndexValue, tempMap);
							vectorMap.put(index, tempMap);
						}
						index++;
					} else {
						Map<String, String> previousMap = vectorMap.get(index);
						if (previousMap != null){
							previousMap.put(name, eachIndexValue);
							vectorMap.put(index, previousMap);
						} else {
							Map<String, String> tempMap = new HashMap<String, String>();
							tempMap.put(name, eachIndexValue);
							vectorMap.put(index, tempMap);
						}
					}
				}
			} else {
				String[] indexValues = value.split(ARRAY_SEPRATOR);
				int index = 0;
				for (String eachIndexValue : indexValues) {
					Map<String, String> previousMap = vectorMap.get(index);
					if (previousMap != null){
						previousMap.put(name, eachIndexValue);
						vectorMap.put(index, previousMap);
					} else {
						Map<String, String> tempMap = new HashMap<String, String>();
						tempMap.put(name, eachIndexValue);
						vectorMap.put(index, tempMap);
					}
					index++;
				}
			}
		}
	}

private void generateArrayTag(String name, String value) {
	Pattern pattern = Pattern.compile(extractArrayPattern);
	Matcher matcher = pattern.matcher(value);
	while (matcher.find()) {
	    for (int i = 1; i <= matcher.groupCount(); i++) {
	        String trans = matcher.group(i);
	        String keyName = trans.split(EQUALS)[0];
	        String keyValue = trans.split(EQUALS)[1];
	        keyValue = keyValue.replace(ARRAY_START, EMPTY).replace(ARRAY_END, EMPTY);
	        boolean first = true;
	        for (String eachString : keyValue.split(ARRAY_SEPRATOR)) {
	        	String nameValue = keyName + EQUALS + eachString;
	        	if (first){
	        		value = value.replace(trans, nameValue);
	        		first = false;
	        	} else {
	        		value = value + ARRAY_SEPRATOR + PARENT_START + nameValue + PARENT_END;
	        	}
	        }
	    }
	}
	
	String[] indexValues = value.split(PARENT_ARRAY_SEPRATOR);
	int index = 0;
	for (String eachIndexValue : indexValues) {
		if (!eachIndexValue.endsWith(PARENT_END)){
			eachIndexValue += PARENT_END;
		}
		Map<String, String> previousMap = vectorMap.get(index);
		if (previousMap != null){
			handleParentChildTag(name, eachIndexValue, previousMap);
			vectorMap.put(index, previousMap);
		} else {
			Map<String, String> tempMap = new HashMap<String, String>();
			handleParentChildTag(name, eachIndexValue, tempMap);
			vectorMap.put(index, tempMap);
		}
		index++;
	}
}

private void handleParentChildTag(String name, String value, Map<String, String> givenMap) {
	value = value.replace(PARENT_START, EMPTY).replace(PARENT_END, EMPTY);
	String eachValue = null;
	String eachKey = null;
	String val = null;
	for (String eachWord : value.split(ARRAY_SEPRATOR)){
		 
		if(!eachWord.contains(EQUALS)) {
			eachKey = val;
			eachValue = eachWord;
		}else {
	     eachKey = eachWord.split(EQUALS)[0];
		 eachValue = eachWord.split(EQUALS)[1];
		 val = eachKey;
		}
		if (eachValue != null || eachValue != EMPTY){
			givenMap.put(name + UNDERSCORE + eachKey, eachValue.toString());
		} else {
			givenMap.put(name + UNDERSCORE + eachKey, null);
		}
	}
}


private void getAndSetInterfaceProperties(SourceFile sf) {
	// All Identifiers
	snIdentifier = sf.getProperty("BCDParser.snIdentifier", defaultSnIdentifier);
	moidIdentifier = sf.getProperty("BCDParser.moidIdentifier", defaultMoidIdentifier);
	sequenceIdentifier = sf.getProperty("BCDParser.sequenceIdentifier", defaultSequenceIdentifier);
	aomIdentifier = sf.getProperty("BCDParser.aomIdentifier", defaultAomIdentifier);
	cuIdentifier = sf.getProperty("BCDParser.cuIdentifier", defaultCuIdentifier);
	releaseIdentifier = sf.getProperty("BCDParser.releaseIdentifier", defaultReleaseIdentifier);
	dateTimeIdentifier = sf.getProperty("BCDParser.dateTimeIdentifier", defaultDateTimeIdentifier);
	timezoneIdentifier = sf.getProperty("BCDParser.timezoneIdentifier", defaultTimezoneIdentifier);
	timelevelIdentifier = sf.getProperty("BCDParser.timelevelIdentifier", defaultTimelevelIdentifier);
	periodDurationIdentifier = sf.getProperty("BCDParser.periodDurationIdentifier", defaultPeriodDurationIdentifier);
	ossIdIdentifier = sf.getProperty("BCDParser.ossIdIdentifier", defaultOssIdIdentifier);
	sourceIdentifier = sf.getProperty("BCDParser.sourceIdentifier", defaultSourceIdentifier);
	dirnameIdentifier = sf.getProperty("BCDParser.dirnameIdentifier", defaultDirnameIdentifier);
	filenameIdentifier = sf.getProperty("BCDParser.filenameIdentifier", defaultFilenameIdentifier);
	jvmTimezoneIdentifier = sf.getProperty("BCDParser.jvmTimezoneIdentifier", defaultJvmTimezoneIdentifier);
	
	// Interface Patterns
	aomPattern = sf.getProperty("BCDParser.aomPattern", null);
	if (aomPattern != null) {
		aom = transformFileVariables("aomPattern", sf.getName(), aomPattern);
	} else {
		aom = null;
	}
	
	cuPattern = sf.getProperty("BCDParser.cuPattern", null);
	if (cuPattern != null) {
		cu = transformFileVariables("cuPattern", sf.getName(), cuPattern);
	} else {
		cu = null;
	}
	
	releasePattern = sf.getProperty("BCDParser.releasePattern", null);
	if (releasePattern != null) {
		release = transformFileVariables("releasefPattern", sf.getName(), releasePattern);
	} else {
		release = null;
	}
	
	hostnamePattern = sf.getProperty("BCDParser.hostnamePattern", null); 
	if (hostnamePattern != null) {
		hostname = transformFileVariables("hostnamePattern", sf.getName(), hostnamePattern);
	} else {
		hostname = null;
	}
	
	datetimeIdPattern = sf.getProperty("BCDParser.datetimeIdPattern", null);
	if (datetimeIdPattern != null) {
		datetimeId = transformFileVariables("datetimeIdPattern", sf.getName(), datetimeIdPattern);
	} else {
		datetimeId = null;
	}
	
	timeZonePattern = sf.getProperty("BCDParser.timeZonePattern", null);
	if (timeZonePattern != null) {
		timeZone = transformFileVariables("timeZonePattern", sf.getName(),
				timeZonePattern);
	} else {
		timeZone = null;
	}
	
	ossIdPattern = sf.getProperty("BCDParser.ossIdPattern", ".+/(.+)/.+/.+");
	if (ossIdPattern != null) {
		ossId = transformFileVariables("ossIdPattern", sf.getDir(), ossIdPattern);
	} else {
		ossIdPattern = null;
	}
	
	timelevel = sf.getProperty("BCDParser.timelevel", "24H");
	periodDuration = sf.getProperty("BCDParser.periodDuration", "1440");
	jvmTimeZone = new SimpleDateFormat("Z").format(new Date());
	filename = sf.getName();
	dirname = sf.getDir();
	// Add element patterns
	elementFDNMap.put(RNC, Pattern.compile(rncPattern));
	elementFDNMap.put(RBS, Pattern.compile(rbsPattern));
	
	otherPatternList.add(allPattern);
	otherPatternList.add(subNetworkPattern);
	otherPatternList.add(meContextPattern);
}

private String transformFileVariables(String transformation, String filename, String pattern) {
	String result = null;
	try {
		Pattern p = Pattern.compile(pattern);
		Matcher m = p.matcher(filename);
		if (m.matches()) {
			result = m.group(1);
		}
	} catch (PatternSyntaxException e) {
		logger.log(Level.ERROR, "Error performing transformFileVariables for " + transformation, e);
	}
	return result;
}

private void writeDataElement() {
	// Handle extra line
	if (dataMap.size() <= 0) {
		return;
	}

	try {
		String fullFdn = getFdnValue();
		String mo = fullFdn.split(EQUALS)[0];
		String id = fullFdn.split(EQUALS)[1];
		Channel dataFile = getMFile(mo);
		Channel vectorFile = getMFile(mo + "_V");
		logger.log(Level.INFO, "Trying to write MO " + mo + "=" + id + " to MeasurementFile.");
		
		if (dataFile != null){
			addCommonKeys(dataMap);
			dataFile.pushData(dataMap);
		}
		
		if (vectorFile != null){
			for (Entry<Integer, Map<String, String>> entry : vectorMap.entrySet()){
				int dcIndex = entry.getKey();
				Map<String, String> tempMap = entry.getValue();
				tempMap.put(sequenceIdentifier, Integer.toString(dcIndex));
				addCommonKeys(tempMap);
				vectorFile.pushData(tempMap);
			}
		}
		
	} catch (Exception e) {
		logger.log(Level.ERROR, "Exception while creating MeasurementFile ", e);
	} finally {
		dataMap.clear();
		vectorMap.clear();
	}
	
}

private String getFdnValue() {
	String fdn = dataMap.get(defaultFdnIdentifier);
	Pattern pattern = Pattern.compile(fdnValuePattern);
	Matcher matcher = pattern.matcher(fdn);
	if (matcher.matches()) {
		return matcher.group(1);
	} else {
		return null;
	}
}

private Channel getMFile(String mo) {
	try {
		Channel mf = channelMap.get(mo);
		if (mf == null) {
			mf = getChannel(mo);
			channelMap.put(mo, mf);
		}
		return mf;
	} catch (Exception e) {
		logger.log(Level.WARN, "Unable to create measurement file for " + mo, e);
		return null;
	}
}

private Channel getChannel(String tagId) {
	try {
		String folderName = MeasurementFileFactory.getFolderName(sf, interfaceName, tagId, logger);
		Channel channel;
		if (folderName != null) {
			if ((channel = channelMap.get(folderName)) != null) {
				return channel;
			}
			channel = MeasurementFileFactory.createChannel(sf, tagId, folderName, logger);
			channelMap.put(folderName, channel);
			return channel;
		}
	} catch (Exception e) {
		logger.log(Level.WARN, "Exception while getting channel : ", e);
	}
	return null;
}

private void addCommonKeys(Map<String, String> givenMap) {
	// Add SN, MOID
	String fdnData = dataMap.get(defaultFdnIdentifier); 
	fdn.handle(fdnData);
	givenMap.put(moidIdentifier, fdn.getMoid());
	givenMap.put(snIdentifier, fdn.getSn());
	// Add MO and ID. 
	String justFdn = getFdnValue();
	if ( justFdn != null ) {
		String fdnName = justFdn.split(EQUALS)[0];
		String fdnValue = justFdn.split(EQUALS)[1];
		givenMap.put(fdnName, fdnValue);
	}
	
	givenMap.put(jvmTimezoneIdentifier, jvmTimeZone);
	givenMap.put(timezoneIdentifier, timeZone);
	givenMap.put(aomIdentifier, aom);
	givenMap.put(cuIdentifier, cu);
	givenMap.put(ossIdIdentifier, ossId);
	givenMap.put(sourceIdentifier, hostname);
	givenMap.put(dirnameIdentifier, dirname);
	givenMap.put(filenameIdentifier, filename);
	givenMap.put(releaseIdentifier, release);
	givenMap.put(timelevelIdentifier, timelevel);
	givenMap.put(dateTimeIdentifier, datetimeId);
	givenMap.put(periodDurationIdentifier, periodDuration);
	
	// Add ELEMENTPARENT, ELEMENTTYPE, ELEMENT
	Map<String, String> elementMap = getElementData();
	for (String element : elementMap.keySet()){
		givenMap.put(element, elementMap.get(element));
	}
	
}

private HashMap<String, String> getElementData() {
	final HashMap<String, String> tmpMap = new HashMap<String, String>();
	tmpMap.put("ELEMENTTYPE", elementType);
	String element=fdn.getElement();
	if( element.contains(",") ){
			tmpMap.put("ELEMENT", element.substring(0, element.indexOf(",")));
	} else {
		tmpMap.put("ELEMENT", element);
	}
	tmpMap.put("ELEMENTPARENT",fdn.getElementParent());
	return tmpMap;
	
}

/*private Map<String, String> getElementData() {
	boolean notAMatch = false;
	final HashMap<String, String> tmpMap = new HashMap<String, String>();
	
	// Get full FDN String
	String fullFdn = dataMap.get(defaultFdnIdentifier);
	
	Pattern p = elementFDNMap.get(elementType);
	Matcher m = null;
	if (null == p) {
		// THIS FDN IS FROM OSSRC
		for (String patternStr : otherPatternList) {
			notAMatch = false;
			p = Pattern.compile(patternStr);
			m = p.matcher(fullFdn);
			if (m.matches()) {
				tmpMap.put(elementTypeIdentifier, elementType);
				if (patternStr.equalsIgnoreCase(meContextPattern)) {
					tmpMap.put(elementIdentifier, m.group(1));
				} else {
					String element = m.group(2);
					if (element.contains(",")) {
						tmpMap.put(elementParentIdentifier, m.group(1));
						tmpMap.put(elementIdentifier, element.substring(0, element.indexOf(",")));
					} else {
						tmpMap.put(elementParentIdentifier, m.group(1));
						tmpMap.put(elementIdentifier, element);
					}
				}
				break;
			} else {
				notAMatch = true;
			}
		}
	} else {
		m = p.matcher(fullFdn);
		if (m.matches()) {
			tmpMap.put(elementTypeIdentifier, elementType);
			tmpMap.put(elementParentIdentifier, m.group(1));
			tmpMap.put(elementIdentifier, m.group(2));
		} else {
			notAMatch = true;
		}
	}
	
	if (notAMatch) {
		logger.log(Level.WARN, "For FDN " + getFdn() + " (of element type " + elementType 
				+ ") could not get ELEMENT and ELEMENTPARENT using pattern " + p.pattern());
	}
	return tmpMap;
	
}*/

	/*private String getSn() {
		String fdn = dataMap.get(defaultFdnIdentifier);
		Pattern pattern = Pattern.compile(snPattern);
		Matcher matcher = pattern.matcher(fdn);
		if (matcher.matches()) {
			return matcher.group(1);
		} else {
			return null;
		}
	}

	private String getMoid() {
		String fdn = dataMap.get(defaultFdnIdentifier);
		Pattern pattern = Pattern.compile(moidPattern);
		Matcher matcher = pattern.matcher(fdn);
		if (matcher.matches()) {
			return matcher.group(1);
		} else {
			return null;
		}
	}
	
	private String getFdn() {
		String fullFdn = dataMap.get(defaultFdnIdentifier);
		Pattern pattern = Pattern.compile(fdnPattern);
		Matcher match = pattern.matcher(fullFdn);
		if (match.matches()){
			return match.group(1);
		} else {
			return null;
		}
	}*/
}

