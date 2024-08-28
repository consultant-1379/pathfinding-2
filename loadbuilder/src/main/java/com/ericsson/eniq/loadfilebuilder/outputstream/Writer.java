package com.ericsson.eniq.loadfilebuilder.outputstream;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ericsson.eniq.loadfilebuilder.controller.KafkaMOConsumer.Data;


public class Writer implements Runnable {
	
	private static final Logger LOG = LogManager.getLogger(Writer.class);
	
	private BlockingQueue<Data> queue = new LinkedBlockingQueue<>();
	
	private boolean isShutDownTriggered = false;
	
	@Override
	public void run() {
		ILoadFile file;
		GenericRecord message;
		//String message;
		LOG.log(Level.INFO, "Write thread started");
		while (!isShutDownTriggered) { 
			try {
				Data data = queue.take();
				file = data.getLoadFile();
				message = data.getMessage();
				if (file != null && message != null) {
					file.save(message);
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				LOG.log(Level.WARN, "Thread got interrupted",e);
			}
		}
		LOG.log(Level.INFO, "Write thread exited");
	}

	public boolean isShutDownTriggered() {
		return isShutDownTriggered;
	}

	public void setShutDownTriggered(boolean isShutDownTriggered) {
		this.isShutDownTriggered = isShutDownTriggered;
	}
	
	public void add(Data data) {
		try {
			queue.put(data);
		} catch (InterruptedException e) {
			LOG.log(Level.WARN, "Thread got interrupted",e);
			Thread.currentThread().interrupt();
		}
	}

}
