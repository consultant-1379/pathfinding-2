package com.ericsson.eniq.flsmock.service;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;


@Service
public class Producer {
	
	private static final Logger log = LogManager.getLogger(Producer.class);
	
	
	KafkaTemplate<String, String> kafkaTemplate;
	
	@Autowired
	public Producer(KafkaTemplate<String, String> kafkaTemplate ) {
		this.kafkaTemplate = kafkaTemplate;
	}
	
	public void produceMessages(String topic, String message) {
		log.log(Level.INFO, "producing message : "+ message);
		kafkaTemplate.send(topic, message);
	}

}
