package com.ericsson.eniq.poc.shortlivedjob;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.event.ListenerContainerIdleEvent;

@SpringBootApplication(scanBasePackages = { "com.ericsson.eniq.poc.shortlivedjob",
		"com.ericsson.eniq.poc.shortlivedjob.config", "com.ericsson.eniq.poc.shortlivedjob.service" })
public class ShortlivedjobApplication {
	
	private static final Logger LOG = LogManager.getLogger(ShortlivedjobApplication.class);
	
	public static void main(String[] args) {
		SpringApplication.run(ShortlivedjobApplication.class, args);
	}
	
	@EventListener
	public void kafkaListener(ListenerContainerIdleEvent event) {
		LOG.log(Level.INFO, "IdleEvent received : " + event);
	}
	
}
