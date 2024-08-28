package com.ericsson.eniq.parsercontroller.service;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import io.fabric8.kubernetes.api.model.batch.Job;


public class BatchJobTracker {
	
	private static final Logger LOG = LogManager.getLogger(BatchJobTracker.class);
	
	ScheduledExecutorService es = Executors.newScheduledThreadPool(1);
		
	KubeClient client;
	
	Long delay;
	
	public BatchJobTracker(KubeClient client, Long delay) {
		this.client = client;
		this.delay = delay;
	}
	
	public void init() {
		Runnable tracker = () -> {
			LOG.log(Level.INFO, "Job Tracker running");
			List<Job> completedJobs = client.getCompletedJobs();
			if (!completedJobs.isEmpty()) {
				boolean status = client.clearJobs(completedJobs);
				LOG.log(Level.INFO, "Job Tracker, job deletion status : {}",status);
			}
		};
		es.scheduleWithFixedDelay(tracker, 0, delay, TimeUnit.SECONDS);
		LOG.log(Level.INFO, "Job Tracker initialized");
	}
	
	public void shutdown() {
		es.shutdown();
	}
	

}
