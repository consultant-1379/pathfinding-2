/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2014
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/
package com.distocraft.dc5000.etl.parser;

import java.io.*;

import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.binaryformatter.BinFormatter;
//import com.distocraft.dc5000.etl.engine.common.Share;
//import com.distocraft.dc5000.etl.engine.main.EngineAdmin;
import com.distocraft.dc5000.repository.cache.*;
import com.ericsson.eniq.common.CommonUtils;
import com.ericsson.eniq.etl.utils.PWriter;
import com.ericsson.eniq.etl.utils.SchemaGenerator;

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
public class MeasurementFileImpl implements MeasurementFile {

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

	private Map<String, String> data;

	private int rowcount = 0;

	private static DataFormatCache dfCache = null;

	private DFormat dataformat = null;

	private Transformer transformerCache = null;

	private PrintWriter writer = null;

	private BufferedOutputStream bwriter = null; // For writing to measurement file in binary format mode.

	private ParquetWriter<GenericData.Record> pWriter;

	private Schema schema;

	private List<GenericData.Record> pList = new ArrayList<>();

	private int rowByteSize; // The size of a row in bytes when written to a binary format measurement file.

	private FileOutputStream fileOutputStream = null;

	private final boolean metadataFound = false;

	private String outputFileName = null;

	private String typeName = null;

	private HashSet<String> nullSet = null;

	private ParserDebugger debugger;

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

	private static RockFactory etlRock = null;

	private static Statement stmt = null;

	private static Connection con = null;

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
	MeasurementFileImpl(final SourceFile sfile, final String tagID, final String techPack, final String set_type,
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
	MeasurementFileImpl(final SourceFile sfile, final String tagID, final String techPack, final String set_type,
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
			log.finest("Parser Type is " + parserType);
			log.finest("Output Format is " + outputFormatTemp);
		} else {
			// Check output format
			log.finest("Parser Type :: " + parserType);
			log.finest("Output Format:: " + outputFormatTemp);
		}
		if (outputFormatTemp == null || outputFormatTemp.length() == 0) {
			outputFormatTemp = "0"; // Default ASCII.
			log.finest("Parser Type :: " + parserType);
			log.finest("Default Output Format:: " + outputFormatTemp);
		}
		outputFormat = Integer.parseInt(outputFormatTemp);
		ignoreMetaData = sf.getProperty("ignoreMetaData", "false").equals("true");

		coldelimiter = sf.getProperty("outputColDelimiter", "\t");

		rowdelimiter = sf.getProperty("outputRowDelimiter", "\n");

		delimitedHeader = sf.getProperty("delimitedHeader", "false").equals("true");

		nullValue = sf.getProperty("nullValue", null);

		checkTagId = "TRUE".equalsIgnoreCase(sf.getProperty("checkTagId", "false"));

		data = new HashMap<String, String>();

		if (dfCache == null) {
			dfCache = DataFormatCache.getCache();
		}

		final String interfaceName = sf.getProperty("interfaceName");
		rowstatus = sf.getProperty("rowstatus", "ROWSTATUS");
		suspected = sf.getProperty("suspected", "SUSPECTED");

		final boolean b = "TRUE".equalsIgnoreCase(sf.getProperty("useDebugger", "false"));

		if (b) {
			this.debugger = ParserDebuggerCache.getCache();
		}

		debug = "true".equalsIgnoreCase(sf.getProperty("debug", ""));

		if (interfaceName == null || interfaceName.equalsIgnoreCase("")) {
			log.log(Level.WARNING, "Invalid interface name: " + interfaceName);
			return;
		}
		log.finest("checkTagId:" + checkTagId);

		if (!checkTagId) {
			dataformat = dfCache.getFormatWithTagID(interfaceName, tagID);
		} else {
			System.out.println("dataformat with only tagId : "+tagID);
			dataformat = dfCache.getFormatWithTagIDWithoutInterface(tagID);
		}

		if (dataformat == null) {
			System.out.println("DataFormat not found for (tag: " + tagID + ") -> writing nothing.");
			log.fine("DataFormat not found for (tag: " + tagID + ") -> writing nothing.");
			return;
		}

		transformerCache = TransformerCache.getCache().getTransformer(dataformat.getTransformerID());

		System.out.println("Opening measurementFile TagID \"" + tagID + "\" Interface Name \"" + interfaceName + "\"");
		log.fine("Opening measurementFile TagID \"" + tagID + "\" Interface Name \"" + interfaceName + "\"");

		this.tagID = tagID;
		writer = null;
		bwriter = null; // FROM BIN
		pWriter = null;

		rowByteSize = Main.getRowByteSize(dataformat);

		final String nullStrings = sf.getProperty("nullStrings", null);
		if (nullStrings != null) {
			nullSet = new HashSet<String>();
			final StringTokenizer st = new StringTokenizer(nullStrings, ",");
			while (st.hasMoreTokens()) {
				nullSet.add(st.nextToken());
			}
		}

		this.charsetEncoding = getOutputCharsetEncoding();
		log.finer("Created new Measurement File for data " + getTagID() + " in worker{" + workerName + "}");
		isMeasFileReady = true;

	}

	private Charset getOutputCharsetEncoding() {
		// final Share share = Share.instance();
		Charset cs = null;
		String dwhdbCharsetEncoding = null;

		try {
			// dwhdbCharsetEncoding = (String) share.get("dwhdb_charset_encoding");
			dwhdbCharsetEncoding = "ISO8859-1";
			if (null != dwhdbCharsetEncoding) {
				cs = Charset.forName(dwhdbCharsetEncoding);
			}
		} catch (final Exception e) {
			log.log(Level.WARNING,
					"Cannot instantiate charset object out of the database charset setting: " + dwhdbCharsetEncoding,
					e);

		}

		return cs;
	}

	@Override
	public void setDebugger(final ParserDebugger debugger) {
		this.debugger = debugger;
	}

	@Override
	public void addData(final String name, final String value) {
		data.put(name, value);
	}

	@Override
	public void addData(final Map map) {
		data.putAll(map);
	}

	@Override
	public void setData(final Map map) {
		data = map;
	}

	@Override
	public DFormat getDataformat() {
		return dataformat;
	}

	@Override
	public void saveData() throws Exception {
		log.finest("saveData");

		if (testMode) {
			log.finest("TestMode active, setting fixed SESSION_ID and BATCH_ID");
			data.put("SESSION_ID", "111111111");
			if (debug) {
				log.finest("session id saved");
			}
			data.put("BATCH_ID", "55555");
			if (debug) {
				log.finest("batch id saved");
			}
		} else {
			if (sf != null) {
				if (sf.getParseSession() != null) {
					data.put("SESSION_ID", String.valueOf(sf.getParseSession().getSessionID()));
				} else {
					log.warning("Invalid ParseSession (== null)");
					return;
				}
				data.put("BATCH_ID", String.valueOf(sf.getBatchID()));
				// For EVENTS OSS_ID column handling
				if ((data.get("OSS_ID") == null) || (data.get("OSS_ID").length() == 0)) {
					data.put("OSS_ID", ossId);
				}

			} else {
				log.warning("Invalid SourceFile (== null)");
				return;
			}
		}

		// No dataformat found. Skipping the write...
		if (dataformat == null) {
			return;
		}

		transformerCache = TransformerCache.getCache().getTransformer(dataformat.getTransformerID());

		if (debugger != null) {
			if (transformerCache != null) {
				transformerCache.addDebugger(debugger);
			}
			debugger.setDatatitems(dataformat.getTransformerID(), dataformat.getDitems());
		}

		if (debug) {

			log.finest("Datavector before transformation:");

			final Iterator<String> iter = data.keySet().iterator();

			while (iter.hasNext()) {
				final String key = iter.next();
				final Object value = data.get(key);
				log.finest("  Key: '" + key + "' Value: '" + value + "'");
			}

			log.finest("End of datavector");

		}

		if (debugger != null) {
			debugger.beforeTransformer(dataformat.getTransformerID(), data);
		}

		if (transformerCache != null && !isTransformed()) {
			transformerCache.transform(data, log);
		}

		final String rStatus = data.get(rowstatus);
		if (rStatus != null && rStatus.equalsIgnoreCase(suspected)) {
			sf.setSuspectedFlag(true);
		}

		if (debug) {

			log.finest("Datavector after transformation:");

			final Iterator<String> iter = data.keySet().iterator();

			while (iter.hasNext()) {
				final String key = iter.next();
				final Object value = data.get(key);
				log.finest("  Key: '" + key + "' Value: '" + value + "'");
			}

			log.finest("End of datavector");

		}

		if (debugger != null) {
			debugger.afterTransformer(dataformat.getTransformerID(), data);
		}

		if (writer == null && bwriter == null && pWriter == null) {

			final String firstDateID = data.get("DATE_ID");

			if (firstDateID == null) {
				if (log.isLoggable(Level.FINEST)) {
					final Set<String> keys = data.keySet();
					final Iterator<String> i = keys.iterator();
					final StringBuffer sb = new StringBuffer("FirstRow: DATE_ID was not found. Available keys: ");
					while (i.hasNext()) {
						sb.append(i.next());
						if (i.hasNext()) {
							sb.append(",");
						}
					}
					log.finest(sb.toString());
				}
			} else {
				log.finest("FirstRow: DATE_ID was set " + firstDateID);
			}

			// get datetime_id from the first row for counter volume information
			final String firstDatetimeID = data.get("DATETIME_ID");

			final String timeLevel = data.get("TIMELEVEL");

			if (null == firstDatetimeID) {
				datetimeID = "";
				log.finest("FirstRow: Did not found DATETIME_ID");
			} else {
				datetimeID = firstDatetimeID;
				log.finest("FirstRow: DATETIME_ID = " + firstDatetimeID);
			}

			if (timeLevel == null) {
				timelevel = "";
				log.finest("FirstRow: Did not found TIMELEVEL");
			} else {
				timelevel = timeLevel;
				log.finest("FirstRow: TIMELEVEL = " + timeLevel);
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

		if (debugger != null) {
			lsm = new LinkedHashMap<String, String>();
		}

		final Iterator<DItem> iterator = dataformat.getDItems();
		GenericData.Record record = null;
		if (outputFormat == IQ_PARQUET) {
			record = new GenericData.Record(schema);
		}
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

			if (outputFormat == IQ_PARQUET) {

				if (value == null) {
					value = "";
				}
				// System.out.println("adding to record : dataName = " + mData.getDataName() + "
				// ## value = " + value);
				record.put(mData.getDataName(), value);
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
					log.warning("JVM cache is not updated properly.");
					// refreshTransformationCache();
				}

				final int[] sizeDetails = { mData.getDataSize(), mData.getDataScale() };
				byte[] binaryValue = null;
				try { // Convert to binary form
					binaryValue = bf.doFormat(value, sizeDetails);
				} catch (final Exception e) { // value was supposed to be a number but has
					// turned out not to be.

					log.warning("Could not convert data item " + mData.getDataName() + " (of " + dataformat.getTagID()
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

		if (outputFormat == IQ_PARQUET) {
			pList.add(record);
			// System.out.println("Accumulating records : "+pList);
		}
		if (outputFormat == IQ_TEXT) { // FROM BIN
			writer.write(rowdelimiter);

			// commented the redundant writer.flush code, as the checkError will also flush
			// the stream.
			// writer.flush();

			if (writer.checkError()) {
				log.warning("Error in printwriter while writing: " + this.outputFileName);
			}
		} else if (outputFormat == IQ_BIN) {
			bwriter.flush();
		}

		rowcount++;

		if (debugger != null) {
			debugger.result(lsm);
		}
		data.clear();
		log.finer("Measurement File saved data for " + getTagID() + " in worker{" + workerName + "}" + " with Rows{"
				+ getRowCount() + "} Counters{" + getCounterVolume() + "}");

	}

	/*
	 * private synchronized void refreshTransformationCache() throws
	 * MalformedURLException, RemoteException, NotBoundException {
	 * 
	 * final EngineAdmin admin = new EngineAdmin();
	 * log.info("Reloading Transformations."); admin.refreshTransformations(); flag
	 * = false;
	 * 
	 * }
	 */

	@Override
	public int getRowCount() {
		return rowcount;
	}

	@Override
	public long getCounterVolume() {
		return counterVolume;
	}

	@Override
	public String getDatetimeID() {
		return datetimeID;
	}

	@Override
	public String getTypename() {
		return typeName;
	}

	@Override
	public boolean metadataFound() {
		return metadataFound;
	}

	@Override
	public boolean isOpen() {
		return isMeasFileReady;
	}

	@Override
	public void close() throws Exception {

		if (data != null) {
			if (data.size() > 0 && metadataFound) {
				log.finer("MeasurementFile contains data when closing outputfile for " + getTagID());
			}
			log.finer("Measurement File being closed for data " + getTagID() + " in worker{" + workerName + "}"
					+ " with Rows{" + getRowCount() + "} Counters{" + getCounterVolume() + "}");
			data.clear();
		}

		if (writer != null) { // output file was open
			writer.close();
			isMeasFileReady = false;
			log.finer("File closed " + outputFileName);
		}

		if (bwriter != null) {
			bwriter.close();
			isMeasFileReady = false;
			log.finer("File closed " + outputFileName);
		}

		if (pWriter != null) {
			for (GenericData.Record record : pList) {
				pWriter.write(record);
			}
			pWriter.close();
		}

		writer = null;
		sf = null;
		data = null;
		nullSet = null;
		debugger = null;
		pWriter = null;
		pList.clear();
	}

	@Override
	public boolean hasData() {
		return data != null && !data.isEmpty();
	}

	@Override
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

		filename = filename.replaceAll(" ", "_").replaceAll(":", "_");
		// System.out.println("Output file spot 2:" +filename);

		sf.addMeastype(dataformat.getFolderName());

		final File outFile = new File(filename);

		if (outputFormat == IQ_BIN) {

			fileOutputStream = new FileOutputStream(outFile, true);
			bwriter = new BufferedOutputStream(fileOutputStream, rowByteSize);

		} else if (outputFormat == IQ_TEXT) {

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

				final Iterator<DItem> iterator = dataformat.getDItems();

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

		} else if (outputFormat == IQ_PARQUET) {

			schema = SchemaGenerator.getAvroSchema(dataformat.getFolderName(), dataformat.getDItems());
			Path path = new Path(filename);
			pWriter = getParquetWriter(path, schema);

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
		final String eventsEtlDataDir = System.getProperty(CommonUtils.EVENTS_ETLDATA_DIR,
				CommonUtils.EVENTS_ETLDATA_DIR_VAL);

		// generate typename
		this.typeName = typeName;

		String destDir = sf.getProperty("outDir", sf.getProperty("baseDir") + File.separator + "out");

		if (!destDir.endsWith(File.separator)) {
			destDir += File.separator;
		}

		destDir = Main.resolveDirVariable(destDir);

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

		// fileName = fileName.replaceAll(" ", "_");
		this.outputFileName = fileName;
		// System.out.println("Output file name spot 1: "+fileName);
		return fileName;
	}

	/**
	 * @param eventsEtlDataDir
	 * @param destDir
	 * @return
	 * @throws Exception
	 */
	private String handleMultiDirSystem(final String eventsEtlDataDir, String destDir) throws Exception {
		// Check if Multi file system server.
		final int IsMulti = CommonUtils.getNumOfDirectories(log);

		if (IsMulti != 0) {
			log.finest("The No of Directories is " + IsMulti + ".");
			directoryNumber = getDirNumber(IsMulti);
			preAppend = directoryNumber <= 9 ? "0" : "";
			final String dirNoWithZeroPadding = preAppend + directoryNumber;

			final String delimiter = System.getProperty(CommonUtils.ETLDATA_DIR, CommonUtils.ETLDATA_DIR_DEFAULT);
			final String[] splitStrs = destDir.split(delimiter, 2);
			for (int folderNumber = 0; folderNumber < IsMulti; folderNumber++) {
				// Create directories in /eniq/data/etldata_/00..../eniq/data/etldata_/XX if
				// they are not already
				// created
				final String destDirStr = createExpandedDirs(eventsEtlDataDir, folderNumber, splitStrs);
				// use destDir that matches current value of directoryNumber
				if (destDirStr.contains(dirNoWithZeroPadding)) {
					destDir = destDirStr;
				}
			}
		} else {
			destDir = appendTpNameAndCreateDir(destDir);
		}
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
		log.finest("destDir is : " + destDirWithTpName);
		createDirectories(destDirWithTpName);
		return destDirWithTpName;
	}

	private void createDirectories(final String destDir) throws Exception {
		final File ddir = new File(destDir);
		if (!ddir.exists()) {
			log.finer("Directory " + destDir + "doesn't exist. Being created now.");
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
	 * @param isTransformed
	 *            the isTransformed to set
	 */
	@Override
	public void setTransformed(final boolean isTransformed) {
		this.isTransformed = isTransformed;
	}

	/**
	 * @return the isTransformed
	 */
	public boolean isTransformed() {
		return isTransformed;
	}

	/**
	 * In Test Mode return always same sessionID
	 * 
	 * @param testMode
	 *            The testMode to set.
	 */
	public static void setTestMode(final boolean testMode) {
		MeasurementFileImpl.testMode = testMode;
	}

	@Override
	public String getTagID() {
		return tagID;
	}

	@Override
	public String getTimeLevel() {
		return timelevel;
	}

	private ParquetWriter<GenericData.Record> getParquetWriter(Path path, Schema schema) throws IOException {
		return AvroParquetWriter.<GenericData.Record>builder(path).withDictionaryEncoding(false).withSchema(schema)
				.withConf(new Configuration()).withCompressionCodec(CompressionCodecName.SNAPPY).withValidation(false)
				.withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE).withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
				.build();
	}
}