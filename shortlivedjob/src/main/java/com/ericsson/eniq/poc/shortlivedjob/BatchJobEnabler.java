package com.ericsson.eniq.poc.shortlivedjob;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "batch-job", name = "enable", havingValue = "true")
public class BatchJobEnabler{
	
	//private static final Object LOCK = new Object();
	private static final Logger LOG = LogManager.getLogger(BatchJobEnabler.class);
	
	//private boolean isListenerIdle = false;
	
	//@Value("${batch-job.enable}")
	//String batchEnabled;

	/*@Override
	public void run(ApplicationArguments args) throws Exception {
		LOG.log(Level.INFO, "Task started, batch-job enabled = {}", batchEnabled);
		synchronized (LOCK) {
			while (!isListenerIdle) {
				LOCK.wait();
			}
			LOG.log(Level.INFO, "The container is Idle, time to exit");
			shutDown();
		}
	}
	
	@EventListener
	public void kafkaListener(ListenerContainerIdleEvent event) {
		LOG.log(Level.INFO, "IdleEvent received : " + event);
		synchronized (LOCK) {
			isListenerIdle = true;
			LOG.log(Level.INFO, "Notifying the waiting main thread");
			LOCK.notifyAll();
		}
	}*/
	@EventListener
	public void kafkaListener(ListenerContainerIdleEvent event) {
		LOG.log(Level.INFO, "IdleEvent received : " + event);
		shutDown();
	}
	
	
	private void shutDown() {
		System.exit(0);
	}
	
}
