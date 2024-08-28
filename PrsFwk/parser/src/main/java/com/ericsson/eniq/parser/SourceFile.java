package com.ericsson.eniq.parser;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.logging.log4j.Logger;

public class SourceFile {

	protected final Logger log;

	protected final File file;

	protected final Properties conf;

	protected boolean writeHeader;

	protected final ParseSession psession;

	protected final List<MeasurementFile> measurementFiles;

	protected final List<String> measurementTypes;

	protected InputStream fis = null;

	protected int batchID = -1;

	protected String parsingStatus = "INITIALIZED";

	protected String errorMessage = "";

	protected long parsingstarttime;

	protected long parsingendtime;

	protected String unzip = "none";

	protected boolean error = false;

	protected boolean suspected = false;

	protected final List<ZipEntry> zipEntryList = new ArrayList<ZipEntry>();

	protected ZipFile zipFile = null;

	protected String errorMsg = "";

	protected int totalRowCount = 0;

	protected boolean totalRowCountCounted = false;

	public SourceFile(File file, Properties conf, ParseSession psession, Logger log) {

		this(file, conf, psession, "none", log);

	}

	public SourceFile(Properties conf, ParseSession psession, Logger log) {

		this(null, conf, psession, "none", log);

	}

	public SourceFile(File file, Properties conf, ParseSession psession, String useZip, Logger log) {

		this.file = file;
		this.conf = conf;
		this.psession = psession;
		this.unzip = useZip;
		this.log = log;

		measurementFiles = new ArrayList<MeasurementFile>();
		measurementTypes = new ArrayList<String>();

		try {
			String wt = getProperty("writeHeader", "false");
			writeHeader = wt.trim().equals("true");
		} catch (Exception e) {
			writeHeader = false;
		}

	}

	void addMeastype(final String type) {

		if (!measurementTypes.contains(type)) {
			measurementTypes.add(type);
		}
	}

	List getMeastypeList() {
		return measurementTypes;
	}

	/**
	 * Session id is explicitly set to avoid reserving batch id for
	 * 
	 * @param batchID the current batch ID
	 */
	void setBatchID(final int batchID) {
		this.batchID = batchID;
	}

	ParseSession getParseSession() {
		return psession;
	}

	boolean getWriteHeader() {
		return writeHeader;
	}

	boolean getErrorFlag() {
		return error;
	}

	void setErrorFlag(final boolean f) {
		error = f;
	}

	boolean getSuspectedFlag() {
		return suspected;
	}

	void setSuspectedFlag(final boolean f) {
		suspected = f;
	}

	void setErrorMsg(final String e) {
		errorMsg = e;
	}

	String getErrorMsg() {
		return errorMsg;
	}

	/**
	 * Returns lastModified of this sourceFile
	 */
	long getLastModified() {
		return file.lastModified();
	}

	int getBatchID() {
		return batchID;
	}

	public void delete() {
		log.debug("Deleting file");
		final boolean ok = file.delete();

		if (!ok) {
			log.warn("Could not delete file " + file.getName());
		} else {
			log.debug("The sourcefile deleted successfully--->" + file.getName());
		}
	}

	public void hide(final String hiddenDirName) {
		log.debug("hidding file" + file.getName());

		final File hiddenDirPath = new File(file.getParent() + File.separator + hiddenDirName);
		if (!hiddenDirPath.exists()) {
			log.debug("Making directory: " + hiddenDirPath.getPath());
			hiddenDirPath.mkdir();
		}
		File moveToPath = new File(hiddenDirPath.getPath() + File.separator + file.getName());
		log.debug("Hiding file to: " + moveToPath.getPath());
		final boolean ok = file.renameTo(moveToPath);

		if (!ok) {
			log.warn("Could not hide file " + file.getName() + " to " + moveToPath.getPath());
		}
	}

	public String getName() {
		return file.getName();
	}

	public String getDir() {
		return file.getParent();
	}

	public long getSize() {
		return file.length();
	}

	public int getRowCount() {
		int count = 0;

		if (!totalRowCountCounted) {
			for (Object measurementFile : measurementFiles) {
				final MeasurementFile mf = (MeasurementFile) measurementFile;
				count += mf.getRowCount();
			}
		} else {
			count = totalRowCount;
		}

		return count;
	}

	/**
	 * The way to read parser source file.
	 * 
	 * @return inputStream to file represented by this object
	 */
	public InputStream getFileInputStream() throws Exception {
		if (unzip.equalsIgnoreCase("zip")) {
			fis = unzip(file);
		} else if (unzip.equalsIgnoreCase("gzip")) {
			fis = gunzip(file);
		} else {
			fis = new FileInputStream(file);
		}
		return fis;

	}

	/*
	 * 
	 */
	public boolean hasNextFileInputStream() throws Exception {
		return zipEntryList != null && zipEntryList.size() > 0;
	}

	/*
	 * 
	 */
	public InputStream getNextFileInputStream() throws Exception {

		if (zipEntryList != null && zipEntryList.size() > 0) {
			return zipFile.getInputStream(zipEntryList.remove(0));
		}

		return null;

	}

	/**
	 * This method will take a single file as its parameter and then return a
	 * readable input stream to this file. Closing of the stream needs to be done by
	 * the caller.
	 * 
	 * @param f the file to read.
	 * @return an input stream for the given file. This will either be a regular
	 *         FileInputStream or a GZipInputStream depending on if the file is
	 *         compressed or not.
	 * @throws Exception
	 */
	protected InputStream gunzip(final File f) throws Exception {
		try {
			// apparently checks GZIP validity checking is done automatically already
			// when the GZIP stream is initialized, with other words there is no need
			// to manually check the data.
			final GZIPInputStream gis = new GZIPInputStream(new FileInputStream(f));

			log.debug("GZip file " + f.getName() + " is a valid GZip file. Returning decompressed data.");
			return gis;
		} catch (Exception e) {
			// We caught an exception. This indicates a problem with decompression.
			// Return the file input stream instead.
			log.debug("GZip file " + f.getName() + " is not a valid GZip file. Retrying without decompression.");

			return new FileInputStream(f);
		}
	}

	protected InputStream unzip(final File f) throws Exception {

		try {

			if (zipEntryList != null) {
				zipEntryList.clear();
			}
			zipFile = new ZipFile(f);
			final Enumeration ez = zipFile.entries();

			while (ez.hasMoreElements()) {
				final ZipEntry zipEntry = (ZipEntry) ez.nextElement();
				zipEntryList.add(zipEntry);
			}

		} catch (Exception e) {
			log.warn("Error while unzipping " + f.getName() + " " + e);
		}

		if (zipEntryList.size() == 0) {

			log.info("Zip file contains no entries, trying to use as normal data file.");
			return new FileInputStream(f);

		}

		log.info("Zip file (" + f.getName() + ") contains " + zipEntryList.size() + " entries");
		log.info("First entry is " + zipEntryList.get(0).getName());

		return zipFile.getInputStream(zipEntryList.remove(0));
	}

	/**
	 * Adds a measurementFile to list of measurementFiles.
	 * 
	 * @param mf MeasurementFile to add.
	 */
	void addMeasurementFile(final MeasurementFile mf) {
		measurementFiles.add(mf);

	}

	void removeMeasurementFile(final MeasurementFile mFile) {
		measurementFiles.remove(mFile);

	}

	/**
	 * Checks that all measurementFiles and sourceFile are closed. If open
	 * measurementFiles or sourceFile are found explicit close is performed.
	 */
	void close() {

		// Close all measurementFiles
		closeMeasurementFiles();

		// Close inputFile
		if (fis != null) {
			try {
				fis.close();
			} catch (Exception e) {
				log.warn("Error closing writer " + e.toString());
			}
		}

	}

	void closeMeasurementFiles() {
		while (measurementFiles.size() > 0) {
			final MeasurementFile mf = measurementFiles.remove(0);
			try {
				if (mf.isOpen()) {
					log.debug("Found open measurementFile: " + mf);
//	          log.info("Measurement File being closed for Vector data " + mf.getTagID()
//	            + " with Rows{"+mf.getRowCount()+"} Counters{"+mf.getCounterVolume()+"}");
					mf.close();
				}
			} catch (Exception e) {
				log.warn("Error closing MeasurementFile " + mf + " : " + e.toString());
			}
		}
	}

	/**
	 * Determines weather this SourceFile is old enough to be parsed
	 * 
	 * @return true if this file is old enough false otherwise
	 */
	boolean isOldEnoughToBeParsed() throws Exception {
		try {

			final int timeDiff = Integer.parseInt(conf.getProperty("minFileAge"));

			if ((System.currentTimeMillis() - file.lastModified()) >= (timeDiff * 60000)) {
				return true;
			}

		} catch (Exception e) {
			log.info("File modification time comparison failed.");
		}

		return false;
	}

	/**
	 * Tries to track this SourceFile name in in a text file created in target
	 * directory.
	 * 
	 * @param tgtDir Target directory category.
	 * @throws Exception in case of failure
	 */
	public void trackDuplicatePMFiles(final File tgtDir) {
		boolean fileExist = false;
		SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
		File trackFile = new File(tgtDir, "trackfile_" + sdf.format(new Date()) + ".txt");
		try {
			fileExist = trackFile.createNewFile();
			if (fileExist) {
				log.debug("Track file " + trackFile + " created.");
			} else {
				log.debug("Track file " + trackFile + " already exist");
			}
		} catch (Exception e) {
			log.warn("Track file " + trackFile + " creation failed. " + e);
			return;
		}
		Path path = Paths.get(trackFile.getPath());
		try (Stream<String> stream = Files.lines(path);
				FileOutputStream output = new FileOutputStream(trackFile, true);
				PrintWriter writer = new PrintWriter(output);) {
			if (!stream.filter(processdFile -> processdFile.contains(file.getName())).findAny().isPresent()) {
				writer.println(file.getName());
				log.debug("File " + file.getName() + " successfully tracked in the " + trackFile);
			} else {
				log.debug("File " + file.getName() + " is already tracked in the " + trackFile);
			}
			delete();
		} catch (Exception e) {
			log.warn("Error while writing  to the track file " + trackFile + " " + e);
		}

	}

	/**
	 * Tries to move this SourceFile to target directory. If file.renameTo fails
	 * move is tried via copying and deleting.
	 * 
	 * @param tgtDir Target directory category.
	 * @throws Exception in case of failure
	 */
	public void move(final File tgtDir) throws Exception {
		log.debug("Moving file");
		final File tgt = new File(tgtDir, file.getName());

		final boolean success = file.renameTo(tgt);

		if (success) {
			log.debug("The sourcefile was moved successfully: " + file.getName() + " ---> " + tgt.getPath());
			return;
		}

		final InputStream in = getFileInputStream();
		final OutputStream out = new FileOutputStream(tgt);

		final byte[] buf = new byte[1024];
		int len;
		while ((len = in.read(buf)) > 0) {
			out.write(buf, 0, len);
		}
		in.close();
		out.close();

		delete();

		log.debug("File " + file.getName() + " successfully moved via copy & delete to " + tgt.getPath());

	}

	/**
	 * Returns SessionLog entry for this sourcefile
	 */
	public Map getSessionLog() {

		final Map<String, Object> lentry = new HashMap<String, Object>();

		lentry.put("sessionID", String.valueOf(getParseSession().getSessionID()));
		lentry.put("batchID", String.valueOf(batchID));
		lentry.put("fileName", getName());

		lentry.put("source", conf.getProperty("interfaceName"));

		lentry.put("sessionStartTime", String.valueOf(parsingstarttime));
		lentry.put("sessionEndTime", String.valueOf(parsingendtime));

		lentry.put("srcLastModified", String.valueOf(getLastModified()));
		lentry.put("srcFileSize", String.valueOf(getSize()));

		final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		lentry.put("dateID", sdf.format(new Date(parsingstarttime)));

		lentry.put("status", parsingStatus);

		if (errorMessage != null) {
			lentry.put("errorMessage", errorMessage);
		}

		// get counter volume information per measurement type and rop starttime
		final HashMap counterVolumes = getCounterVolumeInfo();

		// set counter volume information to session information
		lentry.put("counterVolumes", counterVolumes);

		return lentry;

	}

	private HashMap getCounterVolumeInfo() {
		final HashMap<String, Map<String, String>> counterVolumes = new HashMap<String, Map<String, String>>();

		final Iterator i = measurementFiles.iterator();
		int totalRowCount = 0;

		while (i.hasNext()) {
			final MeasurementFile mf = (MeasurementFile) i.next();
			final String datetimeID = mf.getDatetimeID();
			final String typeName = mf.getTypename();
			final String timelevel = mf.getTimeLevel();
			if (typeName == null) {
				if (!mf.hasData()) {
					log.debug("No data generated for MeasurementFile...");
					continue;
				}
				String tagId = "Unknown-Dataformat-TagID";
				String dId = "Unknown-Dataformat";
				if (mf.getDataformat() != null) {
					tagId = mf.getDataformat().getTagID();
					dId = mf.getDataformat().getDataFormatID();
				}
				log.debug("No counter volume data for TAGID " + tagId + " in DataFormat '" + dId + "' on DATETIME_ID '"
						+ datetimeID + "'");
				continue;
			}

			// store counter volume information by measurement type and rop starttime as key
			final String key = typeName + "_" + datetimeID;
			int rowsSoFar = 0;
			int countersSoFar = 0;
			final Map<String, String> counterVolumeInfo;
			if (counterVolumes.containsKey(key)) {
				counterVolumeInfo = counterVolumes.get(key);
				rowsSoFar = Integer.valueOf(counterVolumeInfo.get("rowCount"));
				countersSoFar = Integer.valueOf(counterVolumeInfo.get("counterVolume"));
			} else {
				counterVolumeInfo = new HashMap<String, String>();
				counterVolumeInfo.put("ropStarttime", datetimeID);
				counterVolumeInfo.put("typeName", typeName);
				counterVolumeInfo.put("timelevel", timelevel);
				counterVolumes.put(key, counterVolumeInfo);
			}
			final int rowCountinFile = mf.getRowCount();
			final long counterCountInFile = mf.getCounterVolume();
			counterVolumeInfo.put("rowCount", String.valueOf(rowCountinFile + rowsSoFar));
			counterVolumeInfo.put("counterVolume", String.valueOf(counterCountInFile + countersSoFar));
			totalRowCount += rowCountinFile;
		}

		this.totalRowCount = totalRowCount;
		this.totalRowCountCounted = true;

		return counterVolumes;
	}

	/**
	 * Gets property value from attached properties object.
	 * 
	 * @param name Property name.
	 * @return Property value.
	 * @throws Exception is thrown if property is undefined.
	 */
	public String getProperty(final String name) throws Exception {
		return conf.getProperty(name);
	}

	/**
	 * Gets property value from attached properties object. If property is not
	 * defined defaultValue is returned.
	 * 
	 * @param name         Property name
	 * @param defaultValue Default value
	 * @return Property value or defaultValue if property is not defined.
	 */
	public String getProperty(final String name, final String defaultValue) {
		return conf.getProperty(name, defaultValue);
	}

	/**
	 * 
	 * return the size of the file in bytes.
	 * 
	 * @return The file size in bytes
	 */
	public long fileSize() {
		return file.length();
	}

	public String getParsingStatus() {
		return parsingStatus;
	}

	public void setParsingStatus(final String parsingStatus) {
		this.parsingStatus = parsingStatus;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(final String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public long getParsingendtime() {
		return parsingendtime;
	}

	public void setParsingendtime(final long parsingendtime) {
		this.parsingendtime = parsingendtime;
	}

	public long getParsingstarttime() {
		return parsingstarttime;
	}

	public void setParsingstarttime(final long parsingstarttime) {
		this.parsingstarttime = parsingstarttime;
	}

}
