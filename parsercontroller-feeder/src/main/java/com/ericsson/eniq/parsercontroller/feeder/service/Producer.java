package com.ericsson.eniq.parsercontroller.feeder.service;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;


@Service
public class Producer {
	
	private static final Logger LOG = LogManager.getLogger(Producer.class);
	
	@Autowired
	KafkaTemplate<String, String> kafkaTemplate;
	
	public void produceMessages(String topic, String message) {
		LOG.log(Level.INFO, "producing message : {} ", message);
		kafkaTemplate.send(topic, message);
	}

}
