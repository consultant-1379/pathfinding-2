package com.ericsson.eniq.parser;

import java.io.*;

import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.logging.log4j.Logger;

import com.ericsson.eniq.parser.binaryformatter.BinFormatter;
import com.ericsson.eniq.parser.cache.DFormat;
import com.ericsson.eniq.parser.cache.DItem;
import com.ericsson.eniq.parser.cache.DataFormatCache;
import com.ericsson.eniq.parser.cache.Transformer;
import com.ericsson.eniq.parser.cache.TransformerCache;

/**
 * MeasurementFile of parser. Takes parsed data in by addData methods and writes
 * default form output file. <br>
 * 
 * Configuration: none <br>
 * 
 * Database usage: Metadata <br>
 * 
 * $id$ <br>
 * 
 * 
 * 
 * 
 */
public class MeasurementFile {

	public String ossId = ""; // Eniq's identification for the OSS server from where parsed data originated.

	public static boolean moveNagged = false;

	public static final int IQ_TEXT = 0;

	public static final int IQ_BIN = 1;

	public static final int IQ_PARQUET = 2;

	public final int outputFormat;

	private SourceFile sf;

	private String techpack;

	private Logger log;

	private final boolean ignoreMetaData;

	private final String coldelimiter;

	private final String rowdelimiter;

	private final boolean delimitedHeader;

	private List<Map<String, String>> datalist;

	private Map<String, String> data;

	private int rowcount = 0;

	private static DataFormatCache dfCache = null;

	private DFormat dataformat = null;

	private Transformer transformerCache = null;

	private PrintWriter writer = null;

	private BufferedOutputStream bwriter = null; // For writing to measurement file in binary format mode.

	private int rowByteSize; // The size of a row in bytes when written to a binary format measurement file.

	private FileOutputStream fileOutputStream = null;

	private final boolean metadataFound = false;

	private String outputFileName = null;

	private String typeName = null;

	private HashSet<String> nullSet = null;

	private boolean debug = false;

	private boolean isTransformed = false;

	private static boolean testMode = false;

	private String tagID = null;

	private boolean isMeasFileReady = false;

	private String workerName = null;

	private String rowstatus = "ROWSTATUS";

	private String suspected = "SUSPECTED";

	private Charset charsetEncoding = null;

	private String nullValue = null;

	private long counterVolume = 0L;

	private String datetimeID = "";

	private String timelevel = "";

	public static int directory_count = -1;

	private static boolean flag = true;

	public int directoryNumber;

	String preAppend = "0";

	private final String parserType;

	private boolean checkTagId = false;

	/**
	 * 
	 * constructor
	 * 
	 * @param sfile
	 * @param tagID
	 * @param techPack
	 * @param set_type
	 * @param set_name
	 * @throws Exception
	 */
	MeasurementFile(final SourceFile sfile, final String tagID, final String techPack, final String set_type,
			final String set_name, final Logger log) throws Exception {

		this(sfile, tagID, techPack, set_type, set_name, "", log);

	}

	/**
	 * 
	 * constructor
	 * 
	 * @param sfile
	 * @param tagID
	 * @param techPack
	 * @param set_type
	 * @param set_name
	 * @throws Exception
	 */
	MeasurementFile(final SourceFile sfile, final String tagID, final String techPack, final String set_type,
			final String set_name, final String workerName, final Logger log) throws Exception {

		this.sf = sfile;

		this.techpack = techPack;

		this.workerName = workerName;

		this.log = log;

		ossId = sf.getProperty("ossId", "");

		// Find out the format of the output from parser (e.g. ASCII or binary).
		String outputFormatTemp = sf.getProperty("outputFormat", null);
		parserType = sf.getProperty("parserType", null);
		if (parserType != null && null == outputFormatTemp) {
			if (parserType.equalsIgnoreCase("MDC")) {
				outputFormatTemp = sf.getProperty("MDCParser.outputFormat", "0");
			} else if (parserType.equalsIgnoreCase("3gpp32435")) {
				outputFormatTemp = sf.getProperty("x3GPPParser.outputFormat", "0");
			} else {
				outputFormatTemp = "0";
			}
			log.debug("Parser Type is " + parserType);
			log.debug("Output Format is " + outputFormatTemp);
		} else {
			// Check output format
			log.debug("Parser Type :: " + parserType);
			log.debug("Output Format:: " + outputFormatTemp);
		}
		if (outputFormatTemp == null || outputFormatTemp.length() == 0) {
			outputFormatTemp = "0"; // Default ASCII.
			log.debug("Parser Type :: " + parserType);
			log.debug("Default Output Format:: " + outputFormatTemp);
		}
		outputFormat = Integer.parseInt(outputFormatTemp);
		ignoreMetaData = sf.getProperty("ignoreMetaData", "false").equals("true");

		coldelimiter = sf.getProperty("outputColDelimiter", "\t");

		rowdelimiter = sf.getProperty("outputRowDelimiter", "\n");

		delimitedHeader = sf.getProperty("delimitedHeader", "false").equals("true");

		nullValue = sf.getProperty("nullValue", null);

		checkTagId = "TRUE".equalsIgnoreCase(sf.getProperty("checkTagId", "false"));

		data = new HashMap<String, String>();

		datalist = new ArrayList<Map<String, String>>();

		if (dfCache == null) {
			dfCache = DataFormatCache.getCache();
		}

		final String interfaceName = sf.getProperty("interfaceName");
		rowstatus = sf.getProperty("rowstatus", "ROWSTATUS");
		suspected = sf.getProperty("suspected", "SUSPECTED");

		debug = "true".equalsIgnoreCase(sf.getProperty("debug", ""));

		if (interfaceName == null || interfaceName.equalsIgnoreCase("")) {
			log.warn("Invalid interface name: " + interfaceName);
			return;
		}
		log.debug("checkTagId:" + checkTagId);

		if (!checkTagId) {
			dataformat = dfCache.getDataFormat(tagID);
		}

		if (dataformat == null) {
			log.info("DataFormat not found for (tag: " + tagID + ") -> writing nothing.");
			return;
		}

		transformerCache = TransformerCache.getCache().getTransformer(dataformat.getTransformerID());

		log.info("Opening measurementFile TagID \"" + tagID + "\" Interface Name \"" + interfaceName + "\"");

		this.tagID = tagID;
		writer = null;
		bwriter = null; // FROM BIN
		rowByteSize = getRowByteSize(dataformat);

		final String nullStrings = sf.getProperty("nullStrings", null);
		if (nullStrings != null) {
			nullSet = new HashSet<String>();
			final StringTokenizer st = new StringTokenizer(nullStrings, ",");
			while (st.hasMoreTokens()) {
				nullSet.add(st.nextToken());
			}
		}

		this.charsetEncoding = getOutputCharsetEncoding();
		log.debug("Created new Measurement File for data " + getTagID() + " in worker{" + workerName + "}");
		isMeasFileReady = true;

	}

	public static int getRowByteSize(final DFormat dataformat) throws Exception {
		int sizeOfRow = 0;
		final Iterator<DItem> iterator = dataformat.getDitems().iterator();
		while (iterator.hasNext()) {
			final DItem mData = iterator.next();

			final String dataType = mData.getDataType();
			final int dataSize = mData.getDataSize();

			if (dataType.equalsIgnoreCase("bit")) {
				sizeOfRow += 2;
			} else if (dataType.equalsIgnoreCase("tinyint")) {
				sizeOfRow += 2;
			} else if (dataType.equalsIgnoreCase("smallint")) {
				sizeOfRow += 3;
			} else if (dataType.equalsIgnoreCase("int")) {
				sizeOfRow += 5;
			} else if (dataType.equalsIgnoreCase("integer")) {
				sizeOfRow += 5;
			} else if (dataType.equalsIgnoreCase("unsigned int")) {
				sizeOfRow += 5;
			} else if (dataType.equalsIgnoreCase("bigint")) {
				sizeOfRow += 9;
			} else if (dataType.equalsIgnoreCase("unsigned bigint")) {
				sizeOfRow += 9;
			} else if (dataType.equalsIgnoreCase("float")) {
				sizeOfRow += 9;
			} else if (dataType.equalsIgnoreCase("char")) {
				sizeOfRow += dataSize + 1;
			} else if (dataType.equalsIgnoreCase("varchar")) {
				sizeOfRow += dataSize + 1;
			} else if (dataType.equalsIgnoreCase("binary")) {
				sizeOfRow += dataSize + 1;
			} else if (dataType.equalsIgnoreCase("varbinary")) {
				sizeOfRow += 0;
			} else if (dataType.equalsIgnoreCase("date")) {
				sizeOfRow += 5;
			} else if (dataType.equalsIgnoreCase("time") || dataType.equalsIgnoreCase("datetime")
					|| dataType.equalsIgnoreCase("timestamp")) {
				sizeOfRow += 9;
			} else if (dataType.equalsIgnoreCase("numeric") || dataType.equalsIgnoreCase("decimal")) {
				if (dataSize <= 4) {
					sizeOfRow += 3;
				} else if ((dataSize >= 5) && (dataSize <= 9)) {
					sizeOfRow += 5;
				} else if ((dataSize >= 10) && (dataSize <= 18)) {
					sizeOfRow += 9;
				} else {
					sizeOfRow += 71;
				}
			} else {
				throw new Exception("Unsupported dataType found in dataformat: " + dataType + ".");
			}

		}
		return sizeOfRow;
	}

	private Charset getOutputCharsetEncoding() {
		final Share share = Share.instance();
		Charset cs = null;
		String dwhdbCharsetEncoding = null;

		try {
			dwhdbCharsetEncoding = (String) share.get("dwhdb_charset_encoding");
			if (null != dwhdbCharsetEncoding) {
				cs = Charset.forName(dwhdbCharsetEncoding);
			}
		} catch (final Exception e) {
			log.warn("Cannot instantiate charset object out of the database charset setting: " + dwhdbCharsetEncoding,
					e);

		}

		return cs;
	}

	public void addData(final String name, final String value) {
		data.put(name, value);
	}

	public void addData(final Map map) {
		data.putAll(map);
	}

	public void setData(final Map map) {
		data = map;
	}

	public void setData(List<Map<String, String>> list) {
		datalist = list;
		data = list.get(0);
	}

	public DFormat getDataformat() {
		return dataformat;
	}

	public void saveData() throws Exception {
		log.debug("saveData");

		if (sf != null) {
			if (sf.getParseSession() != null) {
				data.put("SESSION_ID", String.valueOf(sf.getParseSession().getSessionID()));
			} else {
				log.warn("Invalid ParseSession (== null)");
				return;
			}
			data.put("BATCH_ID", String.valueOf(sf.getBatchID()));
			// For EVENTS OSS_ID column handling
			if ((data.get("OSS_ID") == null) || (data.get("OSS_ID").length() == 0)) {
				data.put("OSS_ID", ossId);
			}

		} else {
			log.warn("Invalid SourceFile (== null)");
			return;
		}

		// No dataformat found. Skipping the write...
		if (dataformat == null) {
			return;
		}

		transformerCache = TransformerCache.getCache().getTransformer(dataformat.getTransformerID());
		if (transformerCache != null && !isTransformed()) {
			transformerCache.transform(data);
		}

		final String rStatus = data.get(rowstatus);
		if (rStatus != null && rStatus.equalsIgnoreCase(suspected)) {
			sf.setSuspectedFlag(true);
		}
		if (writer == null && bwriter == null) {

			final String firstDateID = data.get("DATE_ID");

			// get datetime_id from the first row for counter volume information
			final String firstDatetimeID = data.get("DATETIME_ID");

			final String timeLevel = data.get("TIMELEVEL");

			if (null == firstDatetimeID) {
				datetimeID = "";
				log.debug("FirstRow: Did not found DATETIME_ID");
			} else {
				datetimeID = firstDatetimeID;
				log.debug("FirstRow: DATETIME_ID = " + firstDatetimeID);
			}

			if (timeLevel == null) {
				timelevel = "";
				log.debug("FirstRow: Did not found TIMELEVEL");
			} else {
				timelevel = timeLevel;
				log.debug("FirstRow: TIMELEVEL = " + timeLevel);
			}

			// Ignore metadata = We create metadata based on the first dataline
			if (ignoreMetaData) {

				final List<String> keys = new ArrayList<String>(data.keySet());

				final List<DItem> ditems = new ArrayList<DItem>();

				Collections.sort(keys);

				final Iterator<String> i = keys.iterator();
				int ix = 1;
				while (i.hasNext()) {
					final String counter = i.next();
					final DItem ditem = new DItem(counter, ix++, counter, "");
					ditems.add(ditem);
				}
				dataformat.setItems(ditems);
			}

			/*
			 * //Fix for TR HS49435 //Fix for TR HS86464 & HS83771 if (((data.get("OSS_ID")
			 * == null) || (data.get("OSS_ID").length() == 0)) &&
			 * !((techpack.contains("IPTNMS")) || (techpack.contains("SOEM")))) {
			 * 
			 * log.info("OSS_ID ::"+data.get("OSS_ID")); if(flag) {
			 * log.warning("OSS_ID is empty for Interface "+techpack+". " +
			 * "Transformer cache might not have been refreshed correctly. " +
			 * "Refreshing the interface cache now for Interface "+techpack+"....");
			 * refreshTransformationCache();
			 * 
			 * } log.warning("Skipping first row of the data because of null values");
			 * return; }else {
			 */
			openMeasurementFile(tagID, firstDateID);
			// }
		}

		LinkedHashMap<String, String> lsm = null;


		final Iterator<DItem> iterator = dataformat.getDitems().iterator();
		
		// System.out.println("Data before persisting : "+data);
		while (iterator.hasNext()) {
			final DItem mData = iterator.next();

			String value = data.get(mData.getDataID());

			// Fix for TR HS15571

			if (nullValue != null && value == null) {
				value = nullValue;
			}

			if (nullSet != null && nullSet.contains(value)) {
				value = null;
			}

			// calculate not null counters
			if (nullValue != value && 1 == mData.getIsCounter()) {
				counterVolume++;
			}

			if (outputFormat == IQ_TEXT) {
				if (value != null) {
					writer.write(value);
				}

				writer.write(coldelimiter);

			} else if (outputFormat == IQ_BIN) {

				final Map<String, BinFormatter> formatters = BinFormatter.getClasses();
				final BinFormatter bf = formatters.get(mData.getDataType());

				if (bf == null) {
					log.info("Datatype " + mData.getDataType() + " of data item " + mData.getDataName() + " (of "
							+ dataformat.getTagID()
							+ ") is not supported in binary output mode as the BinaryFormatter is " + bf);
					log.warn("JVM cache is not updated properly.");
					// refreshTransformationCache();
				}

				final int[] sizeDetails = { mData.getDataSize(), mData.getDataScale() };
				byte[] binaryValue = null;
				try { // Convert to binary form
					binaryValue = bf.doFormat(value, sizeDetails);
				} catch (final Exception e) { // value was supposed to be a number but has
					// turned out not to be.

					log.warn("Could not convert data item " + mData.getDataName() + " (of " + dataformat.getTagID()
							+ ") to binary form. Value: " + value
							+ "\nThe entire datarow will be skipped and not written to measurement file!");

					// e.printStackTrace();

					data.clear();
					// Ensure the data in buffer (bwriter) will not be written to file,
					// and provide new buffer for next call to this method.
					bwriter = new BufferedOutputStream(fileOutputStream, rowByteSize);
					return;
				}

				/*
				 * for (int i = 0; i < bindata.length; i++) { bwriter.write(bindata[i]); }
				 */

				bwriter.write(binaryValue);

			}

			if (lsm != null) {
				lsm.put(mData.getDataID(), value);
			}

		} // foreach data value

		if (outputFormat == IQ_TEXT) { // FROM BIN
			writer.write(rowdelimiter);

			// commented the redundant writer.flush code, as the checkError will also flush
			// the stream.
			// writer.flush();

			if (writer.checkError()) {
				log.warn("Error in printwriter while writing: " + this.outputFileName);
			}
		} else if (outputFormat == IQ_BIN) {
			bwriter.flush();
		}

		rowcount++;

		
		data.clear();
		log.debug("Measurement File saved data for " + getTagID() + " in worker{" + workerName + "}" + " with Rows{"
				+ getRowCount() + "} Counters{" + getCounterVolume() + "}");

	}

	public int getRowCount() {
		return rowcount;
	}

	public long getCounterVolume() {
		return counterVolume;
	}

	public String getDatetimeID() {
		return datetimeID;
	}

	public String getTypename() {
		return typeName;
	}

	public boolean metadataFound() {
		return metadataFound;
	}

	public boolean isOpen() {
		return isMeasFileReady;
	}

	public void close() throws Exception {

		if (data != null) {
			if (data.size() > 0 && metadataFound) {
				log.debug("MeasurementFile contains data when closing outputfile for " + getTagID());
			}
			log.debug("Measurement File being closed for data " + getTagID() + " in worker{" + workerName + "}"
					+ " with Rows{" + getRowCount() + "} Counters{" + getCounterVolume() + "}");
			data.clear();
		}

		if (writer != null) { // output file was open
			writer.close();
			isMeasFileReady = false;
			log.debug("File closed " + outputFileName);
		}

		if (bwriter != null) {
			bwriter.close();
			isMeasFileReady = false;
			log.debug("File closed " + outputFileName);
		}

		writer = null;
		sf = null;
		data = null;
		nullSet = null;

	}

	public boolean hasData() {
		return data != null && !data.isEmpty();
	}

	public String toString() {
		return "MeasurementFile " + outputFileName + " type " + typeName + " TAGID " + this.tagID + " Techpack "
				+ this.techpack;

	}

	/**
	 * Opens writer to output file and writes header line
	 */
	private void openMeasurementFile(final String tagID, final String dateID) throws Exception {

		String filename = null;

		// fixed filename for testing
		if (!testMode) {
			filename = determineOutputFileName(tagID, dataformat.getFolderName()) + "_" + workerName + "_" + dateID;
		} else {
			filename = determineOutputFileName(tagID, dataformat.getFolderName()) + "_" + workerName;
		}

		sf.addMeastype(dataformat.getFolderName());

		final File outFile = new File(filename);

		if (outputFormat == IQ_BIN) {

			fileOutputStream = new FileOutputStream(outFile, true);
			bwriter = new BufferedOutputStream(fileOutputStream, rowByteSize);

		}
//        else if(outputFormat == IQ_PARQUET) {
//        	avroSchema = new Schema.Parser().parse(new File("/eniq/sw/conf/avro", dataformat.getFolderName()));
//        	
//    		Path filePath = new Path(filename);
//    		parquetWriter = new AvroParquetWriter(filePath, avroSchema);
//        	
//        } 
		else if (outputFormat == IQ_TEXT) {

			OutputStreamWriter osw = null;

			if (null == this.charsetEncoding) {
				osw = new OutputStreamWriter(new FileOutputStream(outFile, true));
			} else {
				osw = new OutputStreamWriter(new FileOutputStream(outFile, true), this.charsetEncoding);
			}

			writer = new PrintWriter(new BufferedWriter(osw));

			// Write header line to file?
			if (sf.getWriteHeader()) {

				if (delimitedHeader) {
					writer.write(rowdelimiter);
				}

				final Iterator<DItem> iterator = dataformat.getDitems().iterator();

				while (iterator.hasNext()) {

					final DItem data = iterator.next();

					writer.write(data.getDataName());
					writer.write(coldelimiter);

				}

				writer.write(rowdelimiter);

				if (delimitedHeader) {
					writer.write(rowdelimiter);
				}

			}

		} // else no such outputformat

	}

	/**
	 * Determines the directory + filename for this measurementFile
	 * 
	 * @param tagid
	 * @param typename
	 * @param typeid
	 * @param vendorid
	 * @return
	 * @throws Exception
	 */
	private String determineOutputFileName(final String tagid, final String typeName) throws Exception {
		final String eventsEtlDataDir = System.getProperty("EVENTS_ETLDATA_DIR", "/eniq/data/etldata_");

		// generate typename
		this.typeName = typeName;

		String destDir = sf.getProperty("outDir", sf.getProperty("baseDir") + File.separator + "out");

		if (!destDir.endsWith(File.separator)) {
			destDir += File.separator;
		}

		destDir = resolveDirVariable(destDir);

		// if destDir already contain /eniq/data/etldata_/, then treat as if a
		// single directory server
		if (!destDir.contains(eventsEtlDataDir)) {
			destDir = handleMultiDirSystem(eventsEtlDataDir, destDir);
		} else {
			destDir = appendTpNameAndCreateDir(destDir);
		}

		String fileName = destDir + File.separator + typeName;

		// fixed filename for testing
		if (!testMode) {
			final SimpleDateFormat loaderTimestamp = new SimpleDateFormat("yyyyMMddHH");
			fileName += "_" + loaderTimestamp.format(new Date());
		}

		this.outputFileName = fileName;

		return fileName;
	}

	public static String resolveDirVariable(String directory) {

		if (directory == null) {
			return null;
		}

		if (directory.indexOf("${") >= 0) {
			final int sti = directory.indexOf("${");
			final int eni = directory.indexOf("}", sti);

			if (eni >= 0) {
				final String variable = directory.substring(sti + 2, eni);
				final String val = System.getProperty(variable);
				final String result = directory.substring(0, sti) + val + directory.substring(eni + 1);
				directory = result;
			}
		}

		return directory;
	}

	/**
	 * @param eventsEtlDataDir
	 * @param destDir
	 * @return
	 * @throws Exception
	 */
	private String handleMultiDirSystem(final String eventsEtlDataDir, String destDir) throws Exception {
		// Check if Multi file system server.

		// ebrifol - Looked up the niq.ini and seem this property was not set so it
		// would return 0
//        final int IsMulti = CommonUtils.getNumOfDirectories(log);
//
//        if (IsMulti != 0) {
//            log.debug("The No of Directories is " + IsMulti + ".");
//            directoryNumber = getDirNumber(IsMulti);
//            preAppend = directoryNumber <= 9 ? "0" : "";
//            final String dirNoWithZeroPadding = preAppend + directoryNumber;
//
//            final String delimiter = System.getProperty(CommonUtils.ETLDATA_DIR, CommonUtils.ETLDATA_DIR_DEFAULT);
//            final String[] splitStrs = destDir.split(delimiter, 2);
//            for (int folderNumber = 0; folderNumber < IsMulti; folderNumber++) {
//                // Create directories in /eniq/data/etldata_/00..../eniq/data/etldata_/XX if they are not already
//                // created
//                final String destDirStr = createExpandedDirs(eventsEtlDataDir, folderNumber, splitStrs);
//                // use destDir that matches current value of directoryNumber
//                if (destDirStr.contains(dirNoWithZeroPadding)) {
//                    destDir = destDirStr;
//                }
//            }
//        } else {
//            destDir = appendTpNameAndCreateDir(destDir);
//        }

		destDir = appendTpNameAndCreateDir(destDir);
		return destDir;
	}

	private String createExpandedDirs(final String eventsEtlDataDir, final int folderNumber, final String[] splitStrs)
			throws Exception {

		final String zeroPadding = folderNumber <= 9 ? "0" : "";

		final StringBuilder destDirectory = new StringBuilder();
		destDirectory.append(splitStrs[0]);
		destDirectory.append(eventsEtlDataDir);
		destDirectory.append(File.separator);
		destDirectory.append(zeroPadding);
		destDirectory.append(folderNumber);
		destDirectory.append(splitStrs[1]);

		return appendTpNameAndCreateDir(destDirectory.toString());
	}

	private String appendTpNameAndCreateDir(final String destDirStr) throws Exception {
		final String destDirWithTpName = destDirStr + techpack;
		log.debug("destDir is : " + destDirWithTpName);
		createDirectories(destDirWithTpName);
		return destDirWithTpName;
	}

	private void createDirectories(final String destDir) throws Exception {
		final File ddir = new File(destDir);
		if (!ddir.exists()) {
			log.debug("Directory " + destDir + "doesn't exist. Being created now.");
			ddir.mkdirs();
		}

		if (!ddir.isDirectory() || !ddir.canWrite()) {
			throw new Exception("Unable to access ddir " + ddir);
		}
	}

	/*
	 * This method return the Directory numbers in order 0,1,2,3
	 */
	public int getDirNumber(final int fileSystems) {

		if (directory_count < fileSystems - 1) {
			return ++directory_count;
		}

		else {
			directory_count = 0;
			return 0;
		}
	}

	/**
	 * @param isTransformed the isTransformed to set
	 */
	public void setTransformed(final boolean isTransformed) {
		this.isTransformed = isTransformed;
	}

	/**
	 * @return the isTransformed
	 */
	public boolean isTransformed() {
		return isTransformed;
	}

	public String getTagID() {
		return tagID;
	}

	public String getTimeLevel() {
		return timelevel;
	}
}