//package com.ericsson.eniq.sbkafka.controller;
//
//import java.util.Map;
//
//import org.apache.logging.log4j.Level;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//import org.springframework.kafka.core.KafkaTemplate;
//
//import com.ericsson.eniq.parser.sink.ISink;
//
//public class SbKafkaTextProducer implements ISink{
//	private KafkaTemplate<String, String> kafkaTemplate;
//
//	private static final Logger LOG = LogManager.getLogger(SbKafkaTextProducer.class);
//
//	public SbKafkaTextProducer(KafkaTemplate<String, String> kafkaTemplate) {
//		this.kafkaTemplate = kafkaTemplate;
//	}
//	
//	@Override
//	public void pushMessage(String tagId, String record) {
//		long pushStartTime = System.nanoTime();
//		kafkaTemplate.send("PM_E_ERBS_DATA", tagId, record);
//		long pushEndTime = System.nanoTime();
//		//LOG.log(Level.INFO, "Time taken for push in nanos: "+(pushEndTime-pushStartTime));
//	}
//
//	@Override
//	public void pushMessage(String tagId, Map<String, String> data)  {
//		
//		throw new UnsupportedOperationException();
//	}
//
//}
