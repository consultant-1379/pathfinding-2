package com.ericsson.eniq.parsercontroller.feeder.service;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import com.ericsson.eniq.parsercontroller.feeder.core.Processor;

@Service
public class Consumer {
	
	private static final Logger LOG = LogManager.getLogger(Consumer.class);
	
	@Autowired
	private Processor processor;
	
	@KafkaListener(id = "#{'${consumer.id}'}", topics = "#{'${kafka-consumer.topics}'.split(',')}", groupId = "#{'${spring.kafka.consumer.group-id}'}", containerFactory = "batchFactory")
	void listen(List<ConsumerRecord<String, String>> consumerRecords, Acknowledgment acknowledgement) {
		for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
			String topic = consumerRecord.topic();
			String key = consumerRecord.key();
			String message = consumerRecord.value();
			LOG.log(Level.INFO, "Record details : partition = {} ,offset = {}",consumerRecord.partition(), consumerRecord.offset());
			LOG.log(Level.INFO, "TOPIC = {}, KEY = {}, VALUE = {}",topic, key, message);
			processor.processMessage(topic, key, message);
		}
		acknowledgement.acknowledge();
	}

}
