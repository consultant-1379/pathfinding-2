package com.ericsson.eniq.bulkcmhandler.controller;

import java.io.File;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import com.ericsson.eniq.bulkcmhandler.pojo.ParserInputForm;
import com.ericsson.eniq.parser.ParseSession;
import com.ericsson.eniq.parser.SourceFile;
import com.google.gson.Gson;

@Service
public class BulkcmKafkaListener {

	private static final Logger logger = LogManager.getLogger(BulkcmKafkaListener.class);

	BulkcmSplitter bulkcmSplitter;
	
	@Autowired
	public BulkcmKafkaListener(BulkcmSplitter bulkcmSplitter) {
		this.bulkcmSplitter = bulkcmSplitter;
		System.out.println("Bulkcm splitter : "+bulkcmSplitter);
	}
	@KafkaListener(id = "bulkcmgroup", topics = "bulkcmIn", containerFactory = "batchFactory")
	void listen(ConsumerRecords<String, String> consumerRecords, Acknowledgment acknowledgment) {
		for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
			String message = consumerRecord.value();
			logger.log(Level.INFO, "Record details : partitiion = " + consumerRecord.partition() + " ,offset = "
					+ consumerRecord.offset());
			logger.log(Level.INFO, "KafkaHandler[String] {}" + message);
			createWorker(message);
		}
		acknowledgment.acknowledge();
}
	private void createWorker(String message) {
		Gson gson = new Gson();
		ParserInputForm input = gson.fromJson(message, ParserInputForm.class);
		File inputFile = new File(input.getInputFile());
		Properties conf = new Properties();
		conf.putAll(input.getActionContents());
		ParseSession session = new ParseSession(8888, conf);
		SourceFile sf = new SourceFile(inputFile, conf, session, conf.getProperty("useZip", "gzip"), logger);
		try {
			
			bulkcmSplitter.bulkData(sf);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
}
