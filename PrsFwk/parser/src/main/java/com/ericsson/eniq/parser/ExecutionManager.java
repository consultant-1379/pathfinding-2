package com.ericsson.eniq.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ExecutionManager {

	private static final Logger LOG = LogManager.getLogger(ExecutionManager.class);
	private static ExecutionManager em = null;
	private ExecutorService executionservice;
	private int numOfActiveThreads = 5;
	private static final Long TIMEOUT = 14L;
	
	private ExecutionManager(){
		LOG.log(Level.INFO,"No of threads in Execution Manager : " + numOfActiveThreads);
		executionservice = Executors.newFixedThreadPool(numOfActiveThreads);
	}
	
	/*public void addParserToExecution(Runnable parser) {
		executionservice.execute(parser);
	}*/
	
	public void addParserToExecution(List<Callable<Boolean>> parsers) {
		long startTime = System.currentTimeMillis();
		if(parsers == null) {
			return;
		}
		List<Future<Boolean>> futures = new ArrayList<>();
		for(Callable<Boolean> task : parsers) {
			futures.add(executionservice.submit(task));
		}
		LOG.log(Level.INFO,"parser tasks triggered");
		for (Future<Boolean> future : futures) {
				try {
					future.get(TIMEOUT, TimeUnit.MINUTES);
				} catch (InterruptedException | ExecutionException | TimeoutException e) {
					LOG.log(Level.INFO, "Exception while waiting for the parser completion",e);
				}
		}
		LOG.log(Level.INFO,"parser tasks completed");
		triggerMeasFileClosure();
		long endTime = System.currentTimeMillis();
		LOG.log(Level.INFO,"Measurement files closed completed");
		LOG.log(Level.INFO,"Time taken to complete the batch : "+(endTime - startTime));
				
	}
	
	private void triggerMeasFileClosure() {
		MeasurementFileFactory.closeMeasurementFiles(LOG);
	}
	
	
	public void shutdown(){
		executionservice.shutdown();
	}
	
	public static ExecutionManager getInstance(){
		if(em == null) {
			em = new ExecutionManager();
		}
		return em;
	}
}
