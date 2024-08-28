package com.ericsson.eniq.loadfilebuilder.outputstream;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;



public final class LoadFile implements ILoadFile{
	
	private String outDir;
	private String outFileName;
	private String moName;
	private String charsetEncoding;
	//private PrintWriter writer;
	private static final int THRESHOLD_NUMBER = 20;
	private static final int THRESHOLD_TIME = 120000;
	private static final String COL_DELIMITER = "\t";
	private static final String ROW_DELIMITER = "\n";
	private static final String EXTENSION = ".txt";
	private int count = 0;
	private long startTime;
	private long currentId;
	private String hostName;
	private static final Logger LOG = LogManager.getLogger(LoadFile.class);
	
	
	LoadFile (String outDir, String folderName) {
		this.outDir = outDir + File.separator + folderName + File.separator;
		moName = folderName;
	}
	
	private void rotate() {
		currentId = System.currentTimeMillis();
	}
	
	private void write(String message) {
		createDirectories(outDir);
		if (currentId == 0) {
			currentId = System.currentTimeMillis();
			outFileName = outDir+File.separator+moName+"_"+getHostName()+"_"+currentId+EXTENSION;
			LOG.log(Level.INFO,Thread.currentThread().getName()+" : Load file created : "+outFileName);
		} else {
			outFileName = outDir+File.separator+moName+"_"+getHostName()+"_"+currentId+EXTENSION;
		}
		
		final File outFile = new File(outFileName);
		
		try(PrintWriter writer = new PrintWriter( new BufferedWriter((null == this.charsetEncoding) ? new OutputStreamWriter(new FileOutputStream(outFile, true))
				: new OutputStreamWriter(new FileOutputStream(outFile, true), this.charsetEncoding)), true);) {
			writer.write(message);
		} catch (FileNotFoundException | UnsupportedEncodingException e) {
			LOG.log(Level.WARN, Thread.currentThread().getName()+" : Exception while creating the load file",e);
		}
	}
	
	private void createDirectories(final String destDir) {
		final File ddir = new File(destDir);
		if (!ddir.exists()) {
			LOG.info("Directory " + destDir + "doesn't exist. Being created now.");
			ddir.mkdirs();
		}
		if (!ddir.isDirectory() || !ddir.canWrite()) {
			LOG.info("Unable to access ddir " + ddir);
		}
	}
	
	
	/*@Override
	public void save(String message) {
		if (count >= THRESHOLD_NUMBER) {

			LOG.log(Level.INFO, Thread.currentThread().getName() + " : File being closed is : " + outFileName);
			rotate();
			count = 0;
		}
		if (message != null) {
			write(message + ROW_DELIMITER);
		}
		count++;
	}*/
	
	@Override
	public void save(GenericRecord record) {
		
		if(count >= THRESHOLD_NUMBER) {
			
			LOG.log(Level.INFO,Thread.currentThread().getName()+" : File being closed is : "+outFileName);
			rotate();
			count = 0;
			
		}
		StringBuilder builder = new StringBuilder();
		String colName;
		for (Field field : record.getSchema().getFields()) {
			colName = field.name();
			if (colName != null) {
				builder.append(record.get(field.name()));
				builder.append(COL_DELIMITER);
			} else {
				LOG.log(Level.INFO,moName+" : Column name is null");
			}
			
		}
		String message = builder.toString();
		if(message != null) {
			write(message+ROW_DELIMITER);
		}
		count++;
		
	}
	

	public String getOutFileName() {
		return outFileName;
	}
	
	private String getHostName() {
		if (hostName == null) {
			hostName = System.getenv("HOSTNAME");
		}
		return hostName;
	}
	

}
