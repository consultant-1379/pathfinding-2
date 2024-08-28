package com.distocraft.dc5000.etl.parser;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

import ssc.rockfactory.RockFactory;



public class ZipEntrySource extends SourceFile {
	
	ZipEntry zipEntry;
	
	InputStream zipEntryInputStream = null;

	public ZipEntrySource(File zipFile, ZipEntry zipEntry, int memoryConsumptionMB, Properties conf, RockFactory rf,  ParseSession psession,
			ParserDebugger debugger, Logger log) {
		super(zipFile, memoryConsumptionMB, conf, rf, psession, debugger, "none", log);
		this.zipEntry = zipEntry;
	}

	@Override
	void addMeastype(String type) {
		super.addMeastype(type);
	}

	@Override
	List getMeastypeList() {
		return super.getMeastypeList();
	}

	@Override
	void setBatchID(int batchID) {
		super.setBatchID(batchID);
	}

	@Override
	RockFactory getRockFactory() {
		return super.getRockFactory();
	}

	@Override
	RockFactory getRepRockFactory() {
		return super.getRepRockFactory();
	}

	@Override
	ParseSession getParseSession() {
		return super.getParseSession();
	}

	@Override
	boolean getWriteHeader() {
		return super.getWriteHeader();
	}

	@Override
	boolean getErrorFlag() {
		return super.getErrorFlag();
	}

	@Override
	void setErrorFlag(boolean f) {
		super.setErrorFlag(f);
	}

	@Override
	boolean getSuspectedFlag() {
		return super.getSuspectedFlag();
	}

	@Override
	void setSuspectedFlag(boolean f) {
		super.setSuspectedFlag(f);
	}

	@Override
	void setErrorMsg(String e) {
		super.setErrorMsg(e);
	}

	@Override
	String getErrorMsg() {
		return super.getErrorMsg();
	}

	@Override
	long getLastModified() {
		return super.getLastModified();
	}

	@Override
	int getBatchID() {
		return super.getBatchID();
	}

	@Override
	public void delete() {
		
	}

	@Override
	public void hide(String hiddenDirName) {
		
	}

	@Override
	public String getName() {
		return zipEntry.getName();
	}
	
	/**
	 * Get the zip file name in which the zip entry is present
	 * 
	 * @return parentName
	 */
	public String getParentName() {
		return super.getName();
	}
	
	/**
	 * Get the size of the zip file in which the zip entry is present
	 * @return
	 */
	public long getParentSize() {
		return super.getSize();
	}

	@Override
	public String getDir() {
		return super.getDir();
	}

	@Override
	public long getSize() {
		return zipEntry.getSize();
	}

	@Override
	public int getRowCount() {
		return super.getRowCount();
	}

	@Override
	public InputStream getFileInputStream() throws Exception {
		if (zipFile == null){
			zipFile = new ZipFile(file);
		}
		zipEntryInputStream = zipFile.getInputStream(zipEntry);
		return zipEntryInputStream;
	}

	@Override
	public boolean hasNextFileInputStream() throws Exception {
		return false;
	}

	@Override
	public InputStream getNextFileInputStream() throws Exception {
		return null;
	}

	@Override
	protected InputStream unzip(File f) throws Exception {
		return null;
	}

	@Override
	protected InputStream gunzip(File f) throws Exception {
		return null;
	}

	@Override
	void addMeasurementFile(MeasurementFile mf) {
		super.addMeasurementFile(mf);
	}

	@Override
	void removeMeasurementFile(MeasurementFile mFile) {
		super.removeMeasurementFile(mFile);
	}

	@Override
	void close() {
		closeMeasurementFiles();
		if (zipEntryInputStream != null) {
			try {
				zipEntryInputStream.close();
			} catch (Exception e) {
				log.warning("Error closing zip Entry Input Stream" + e.toString());
			}
		}
		if(zipFile != null) {
			try {
				zipFile.close();
			} catch (Exception e) {
				log.warning("Error closing zip file " + e.toString());
			}
		}
	}
	
	@Override
	void closeMeasurementFiles() {
		super.closeMeasurementFiles();
	}

	@Override
	boolean isOldEnoughToBeParsed() throws Exception {
		return super.isOldEnoughToBeParsed();
	}

	@Override
	public void move(File tgtDir) throws Exception {
	}

	@Override
	public Map getSessionLog() {
		return super.getSessionLog();
	}

	@Override
	public String getProperty(String name) throws Exception {
		return super.getProperty(name);
	}

	@Override
	public String getProperty(String name, String defaultValue) {
		return super.getProperty(name, defaultValue);
	}

	@Override
	public long fileSize() {
		return super.fileSize();
	}

	@Override
	public String getParsingStatus() {
		return super.getParsingStatus();
	}

	@Override
	public void setParsingStatus(String parsingStatus) {
		super.setParsingStatus(parsingStatus);
	}

	@Override
	public String getErrorMessage() {
		return super.getErrorMessage();
	}

	@Override
	public void setErrorMessage(String errorMessage) {
		super.setErrorMessage(errorMessage);
	}

	@Override
	public long getParsingendtime() {
		return super.getParsingendtime();
	}

	@Override
	public void setParsingendtime(long parsingendtime) {
		super.setParsingendtime(parsingendtime);
	}

	@Override
	public long getParsingstarttime() {
		return super.getParsingstarttime();
	}

	@Override
	public void setParsingstarttime(long parsingstarttime) {
		super.setParsingstarttime(parsingstarttime);
	}

	@Override
	public int getMemoryConsumptionMB() {
		return super.getMemoryConsumptionMB();
	}

	@Override
	public void setMemoryConsumptionMB(int memoryConsumptionMB) {
		super.setMemoryConsumptionMB(memoryConsumptionMB);
	}
	
	

}
