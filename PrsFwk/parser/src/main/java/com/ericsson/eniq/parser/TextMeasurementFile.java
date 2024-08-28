package com.ericsson.eniq.parser;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;

import com.ericsson.eniq.parser.cache.DFormat;
import com.ericsson.eniq.parser.cache.DItem;
import com.ericsson.eniq.parser.cache.DataFormatCache;
import com.ericsson.eniq.parser.cache.Transformer;
import com.ericsson.eniq.parser.cache.TransformerCache;
import com.ericsson.eniq.parser.sink.ISink;

import static com.ericsson.eniq.parser.Constants.*;

public class TextMeasurementFile implements IMeasurementFile {

	private String ossId;
	private SourceFile sf;
	private String techpack;
	private Logger log;
	private final boolean ignoreMetaData;

	// Delimiters
	private final String coldelimiter;
	private final String rowdelimiter;
	private final boolean delimitedHeader;

	private Map<String, String> data;
	private int rowcount = 0;
	private static DataFormatCache dfCache;
	private DFormat dataformat;
	private Transformer transformerCache;

	// IO Streams
	private OutputStreamWriter osw;
	private FileOutputStream fos;
	private BufferedWriter bw;
	private PrintWriter writer;

	private String outputFileName;
	private String typeName;
	private HashSet<String> nullSet;
	private boolean isTransformed;
	private boolean isMeasFileReady;
	private String workerName;
	private String rowstatus;
	private String suspected;
	private Charset charsetEncoding;
	private String nullValue;
	private long counterVolume;
	private String datetimeID = "";
	private String timelevel = "";
	String preAppend = "0";

	private final String parserType;

	private boolean checkTagId = false;
	private MeasurementFileStatus status = MeasurementFileStatus.INITIALIZED;
	
	private static final String TXT_EXTENSION = ".txt";
	
	private static final BlockingQueue<Map<String,String>> dataQueue = new LinkedBlockingQueue<>();
	
	private boolean isCloseTriggered;
	
	private ISink sink;
	
	private String folderName;
	
	private String tagId;
	
	//private ExecutorService executorService;
	
	
	
	/**
	 * 
	 * constructor
	 * 
	 * @param sfile
	 * @param tagID
	 * @param techPack
	 * @param setType
	 * @param setName
	 * @throws Exception
	 */
	TextMeasurementFile(final SourceFile sfile, final String techPack,  String tagId,
			DFormat dFormat, final Logger log, ISink sink) throws Exception{

		this.sf = sfile;
		this.techpack = techPack;
		this.dataformat = dFormat;
		this.log = log;
		this.sink = sink;
		this.folderName = dFormat.getFolderName();
		this.tagId = tagId;

		ossId = sf.getProperty("ossId", "");
		parserType = sf.getProperty("parserType", null);
		log.debug("Parser Type :: " + parserType);
		ignoreMetaData = Boolean.parseBoolean(sf.getProperty("ignoreMetaData", FALSE));
		coldelimiter = sf.getProperty("outputColDelimiter", "\t");
		rowdelimiter = sf.getProperty("outputRowDelimiter", "\n");
		delimitedHeader = Boolean.parseBoolean(sf.getProperty("delimitedHeader", FALSE));
		nullValue = sf.getProperty("nullValue", null);
		checkTagId = Boolean.parseBoolean(sf.getProperty("checkTagId", FALSE));

		data = new HashMap<>();
	
		if (dfCache == null) {
			dfCache = DataFormatCache.getCache();
		}

		//final String interfaceName = sf.getProperty("interfaceName");
		rowstatus = sf.getProperty("rowstatus", "ROWSTATUS");
		suspected = sf.getProperty("suspected", "SUSPECTED");

		//if (interfaceName == null || interfaceName.isEmpty()) {
			//log.warn("Invalid interface name: " + interfaceName);
			//return;
		//}
		log.debug("checkTagId:" + checkTagId);
		transformerCache = TransformerCache.getCache().getTransformer(dataformat.getTransformerID());

		//log.debug("Opening measurementFile TagID \"" + dataformat.getTagID() + "\" Interface Name \"" + interfaceName + "\"");

		writer = null;

		final String nullStrings = sf.getProperty("nullStrings", null);
		if (nullStrings != null) {
			nullSet = new HashSet<>();
			final StringTokenizer st = new StringTokenizer(nullStrings, ",");
			while (st.hasMoreTokens()) {
				nullSet.add(st.nextToken());
			}
		}

		this.charsetEncoding = getOutputCharsetEncoding();
		log.debug("Created new Measurement File for tagId" + dataformat.getTagID() +" in worker{" + workerName + "}");
		//Runnable task = createWriteTask();
		//executorService = Executors.newFixedThreadPool(1);
		//executorService.execute(task);
		isMeasFileReady = true;

	}
	
	private Runnable createWriteTask() {
		return new Runnable() {
			@Override
			public void run() {
				while(!isCloseTriggered) {
					try {
						if (dataQueue.take() != null) {
							saveData();
						}
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					} catch (Exception e) {
						log.log(Level.ERROR, "createWriteTask : Error while writing to measurement file", e);
					}
				}
				
			}
			
		};
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

	@Override
	public void addData(final String name, final String value) {
		data.put(name, value);
		try {
			saveData();
		} catch (Exception e) {
			log.log(Level.WARN, "Exception while saving data",e);
		}
		//addToQueue(data);
	}

	@Override
	public void addData(final Map<String, String> map) {
		data.putAll(map);
		try {
			saveData();
		} catch (Exception e) {
			log.log(Level.WARN, "Exception while saving data",e);
		}
		//addToQueue(data);
	}

	@Override
	public void setData(final Map<String, String> map) {
		data = map;
		//addToQueue(data);
	}

	public void setData(List<Map<String, String>> list) {
		data = list.get(0);
		//addToQueue(data);
	}
	
	private void addToQueue(Map<String,String> rows) {
		try {
			dataQueue.put(rows);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	public DFormat getDataformat() {
		return dataformat;
	}

	public void saveData() throws Exception {
		log.debug("saveData");
		if (!isValidWrite()) {
			return;
		}
		//log.log(Level.INFO,"Writing in progress");
		status = MeasurementFileStatus.WRITING_INPROGRESS;
		prepareForWriting();
		//writeData();
		pushMessage();
		status = MeasurementFileStatus.WRITING_COMPLETED;
		//log.log(Level.INFO,"Writing in completed");

		data.clear();
		log.debug("Measurement File saved data for " + dataformat.getTagID() + " in worker{" + workerName + "}" + " with Rows{"
				+ getRowCount() + "} Counters{" + getCounterVolume() + "}");

	}

	private boolean isValidWrite() {
		boolean result = true;
		if (sf == null) {
			log.warn("Invalid SourceFile (== null)");
			return false;
		}
		if (sf.getParseSession() == null) {
			log.warn("Invalid ParseSession (== null)");
			return false;
		}
		if (dataformat == null) {
			log.warn("dataformat not found , skipping write");
			return false;
		}
		return result;
	}

	private void prepareForWriting() throws Exception {
		data.put("SESSION_ID", String.valueOf(sf.getParseSession().getSessionID()));
		data.put("BATCH_ID", String.valueOf(sf.getBatchID()));
		// For EVENTS OSS_ID column handling
		if ((data.get(OSS_ID) == null) || (data.get(OSS_ID).length() == 0)) {
			data.put(OSS_ID, ossId);
		}

		transformerCache = TransformerCache.getCache().getTransformer(dataformat.getTransformerID());
		if (transformerCache != null && !isTransformed()) {
			long trStartTime = System.nanoTime();
			transformerCache.transform(data);
			long trEndTime = System.nanoTime();
			//log.log(Level.INFO, "Time taken for transformation in nanos: "+(trEndTime-trStartTime));
		}
		final String rStatus = data.get(rowstatus);
		if (rStatus != null && rStatus.equalsIgnoreCase(suspected)) {
			sf.setSuspectedFlag(true);
		}
	}
	
	private void pushMessage() {
		Map<String, String> tempData = new HashMap<>();
		String value;
		for (DItem item :dataformat.getDitems()) {
			value = data.get(item.getDataID());
			if (value != null) {
				tempData.put(item.getDataName(), value);
			}
		}
		sink.pushMessage(tagId,tempData);
	}
	
	/**private void pushMessage() {
		StringBuilder builder = new StringBuilder();
		dataformat.getDitems().forEach(mData -> {
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
			if (value != null) {
				builder.append(value);
			}
			builder.append(coldelimiter);
		});
		builder.append(rowdelimiter);
		sink.pushMessage(tagId, builder.toString());
		
	}*/

	private void writeData() throws Exception {
		if (writer == null) {
			openMeasurementFile();
		}
		//Collections.sort(dataformat.getDitems());
		long wrStartTime = System.nanoTime();
		dataformat.getDitems().forEach(mData -> {
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
			if (value != null) {
				writer.write(value);
			}
			writer.write(coldelimiter);
		});
		writer.write(rowdelimiter);
		long wrEndTime = System.nanoTime();
		//log.log(Level.INFO, "Time taken for writing a row in nanos : "+(wrEndTime-wrStartTime));

		if (writer.checkError()) {
			log.warn("Error in printwriter while writing: " + this.outputFileName);
		}

		rowcount++;
	}

	private void openMeasurementFile() throws Exception {
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
			final List<String> keys = new ArrayList<>(data.keySet());
			final List<DItem> ditems = new ArrayList<>();
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
		openMeasurementFile(firstDateID);
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

	public boolean isOpen() {
		return isMeasFileReady;
	}

	@Override
	public void close() {
		if (data != null) {
			log.debug("Measurement File being closed for data " + dataformat.getTagID() + " in worker{" + workerName + "}"
					+ " with Rows{" + getRowCount() + "} Counters{" + getCounterVolume() + "}");
			data.clear();
		}
		if (writer != null) { // output file was open
			writer.close();
			isMeasFileReady = false;
			log.log(Level.TRACE,"File closed " + outputFileName);
		}
		try {
			if (bw != null) {
				bw.close();
			}
			if (osw != null) {
				osw.close();
			}
			if (fos != null) {
				fos.close();
			}
			/*(if (executorService != null) {
				isCloseTriggered = true;
				executorService.shutdownNow();
			}*/
		} catch (IOException ioe) {
			log.warn("Exception while closing the io stream chain", ioe);
		} finally {
			//isCloseTriggered = true;
		}
		log.log(Level.TRACE,"all streams closed closed " + outputFileName);
		
	}

	public boolean hasData() {
		return data != null && !data.isEmpty();
	}

	@Override
	public String toString() {
		return "MeasurementFile " + outputFileName + " type " + typeName +" Techpack "
				+ this.techpack;

	}

	/**
	 * Opens writer to output file and writes header line
	 * 
	 * @throws Exception
	 */
	private void openMeasurementFile(final String dateID) throws Exception {
		String filename;
		// fixed filename for testing
		filename = determineOutputFileName(dataformat.getFolderName()) + "_" + workerName + "_" + dateID;
		log.log(Level.DEBUG,"file name created : "+filename);
		sf.addMeastype(dataformat.getFolderName());
		final File outFile = new File(filename);

		fos = new FileOutputStream(outFile, true);
		osw = (null == this.charsetEncoding) ? new OutputStreamWriter(fos)
				: new OutputStreamWriter(fos, this.charsetEncoding);
		bw = new BufferedWriter(osw);
		writer = new PrintWriter(bw);
		// Write header line to file?
		if (sf.getWriteHeader()) {
			if (delimitedHeader) {
				writer.write(rowdelimiter);
			}
			dataformat.getDitems().forEach(dItem -> writer.write(dItem.getDataName() + coldelimiter));
			writer.write(rowdelimiter);
			if (delimitedHeader) {
				writer.write(rowdelimiter);
			}
		}
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
	private String determineOutputFileName(final String folderName) throws Exception {
		this.typeName = folderName;
		String destDir = sf.getProperty("outDir", sf.getProperty("baseDir") + File.separator + "out");
		if (!destDir.endsWith(File.separator)) {
			destDir += File.separator;
		}
		destDir = resolveDirVariable(destDir);
		destDir = appendTpNameAndCreateDir(destDir);
		String fileName = destDir + File.separator + folderName;
		final SimpleDateFormat loaderTimestamp = new SimpleDateFormat("yyyyMMddHH");
		fileName += "_" + loaderTimestamp.format(new Date()) + TXT_EXTENSION;
		this.outputFileName = fileName;
		return fileName;
	}

	public static String resolveDirVariable(String directory) {
		String result = directory;
		if (directory == null) {
			return null;
		}
		if (directory.indexOf("${") >= 0) {
			final int sti = directory.indexOf("${");
			final int eni = directory.indexOf('}', sti);
			if (eni >= 0) {
				final String variable = directory.substring(sti + 2, eni);
				final String val = System.getProperty(variable);
				result = directory.substring(0, sti) + val + directory.substring(eni + 1);
			}
		}
		return result;
	}

	private String appendTpNameAndCreateDir(final String destDirStr) throws ParserException {
		final String destDirWithTpName = destDirStr + techpack;
		log.debug("destDir is : " + destDirWithTpName);
		createDirectories(destDirWithTpName);
		return destDirWithTpName;
	}

	private void createDirectories(final String destDir) throws ParserException {
		final File ddir = new File(destDir);
		if (!ddir.exists()) {
			log.debug("Directory " + destDir + "doesn't exist. Being created now.");
			ddir.mkdirs();
		}
		if (!ddir.isDirectory() || !ddir.canWrite()) {
			throw new ParserException("Unable to access ddir " + ddir);
		}
	}

	/**
	 * @param isTransformed
	 *            the isTransformed to set
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

	public String getTimeLevel() {
		return timelevel;
	}

	@Override
	public MeasurementFileStatus getStatus() {
		return status;
	}


}
