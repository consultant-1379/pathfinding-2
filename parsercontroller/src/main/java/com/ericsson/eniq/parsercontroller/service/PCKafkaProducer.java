package com.ericsson.eniq.parsercontroller.service;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;


@Service
public class PCKafkaProducer {
	
private static final Logger log = LogManager.getLogger(PCKafkaProducer.class);
	
	
	KafkaTemplate<String, String> kafkaTemplate;
	
	@Autowired
	public PCKafkaProducer(KafkaTemplate<String, String> kafkaTemplate ) {
		this.kafkaTemplate = kafkaTemplate;
	}
	
	public void produceMessages(String topic, String key, String message) {
		log.log(Level.INFO, "producing message : "+ message);
		kafkaTemplate.send(topic, key, message);
	}

}
