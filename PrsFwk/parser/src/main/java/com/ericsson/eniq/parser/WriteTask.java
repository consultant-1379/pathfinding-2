package com.ericsson.eniq.parser;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ericsson.eniq.parser.MeasurementFileFactory.Data;

public class WriteTask implements Runnable {
	
	private static final Logger logger = LogManager.getLogger(WriteTask.class);

	private BlockingQueue<Data> queue = new LinkedBlockingQueue<>();
	
	private boolean signalIfEmpty;

	@Override
	public void run() {
		logger.log(Level.INFO, "Writer thread started "+this.toString());
		IMeasurementFile measFile;
		while (true) { 
			try {
				Data data = queue.take();
				measFile = MeasurementFileFactory.getMeasurementFile(data.getFormat() , data.getFolderName());
				if (measFile != null) {
					measFile.addData(data.getRows());
				}else {
					logger.log(Level.INFO, "Measurement file is null for folder "+data.getFolderName());
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} catch (Exception e) {
				logger.log(Level.WARN, "Exception while writing ",e);
			}
		}

	}
	
	void addData(Data data) {
		try {
			queue.put(data);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	public boolean isSignalIfEmpty() {
		return signalIfEmpty;
	}

	public void setSignalIfEmpty(boolean signalIfEmpty) {
		this.signalIfEmpty = signalIfEmpty;
	}
	
	public boolean isQueueEmpty() {
		return queue.isEmpty();
	}
	
}
