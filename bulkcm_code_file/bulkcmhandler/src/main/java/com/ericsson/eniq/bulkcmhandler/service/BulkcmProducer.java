package com.ericsson.eniq.bulkcmhandler.service;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class BulkcmProducer {
	
private static final Logger log = LogManager.getLogger(BulkcmProducer.class);
	
	@Autowired
	KafkaTemplate<String, String> kafkaTemplate;
	
	public void producerBulkcmMessages(String topic, String message) {
		log.log(Level.INFO, "producing message : "+ message);
		kafkaTemplate.send(topic, message);
	}

}
