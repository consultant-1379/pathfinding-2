package com.ericsson.eniq.sbkafkabulkcm.controller;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import com.ericsson.eniq.bulkcmparser.parser.Parser;
import com.ericsson.eniq.parser.ExecutionManager;
import com.ericsson.eniq.parser.ParseSession;
import com.ericsson.eniq.parser.SourceFile;

@Service
public class ParserListener {
	
	@Value("${interfaceName}")
	private String interfaceName;
	
	private List<Callable<Boolean>> workers = new ArrayList<>();
	
	private static final Logger logger = LogManager.getLogger(ParserListener.class);
	
	@KafkaListener(id = "bulkcmgroup", topics = "bulkcmOut", containerFactory = "batchFactory")
	void listen(ConsumerRecords<String, String> consumerRecords, Acknowledgment acknowledgment) {
		
		for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
		String message = consumerRecord.value();
		
		logger.log(Level.INFO, "Record details : partitiion = " + consumerRecord.partition() + " ,offset = "
				+ consumerRecord.offset());
		sendMessage(message);
		}
		ExecutionManager.getInstance().addParserToExecution(workers);
		workers.clear();
		acknowledgment.acknowledge();
}

	private void sendMessage(String message) {
		Properties conf = new Properties();
		conf.put("BCDParser.datetimeIdPattern", "AOM\\d{6}\\_R.+\\_.+\\_(\\d{12})[-+]\\d{4}\\_.+\\.*");
		conf.put("BCDParser.releasePattern","AOM\\\\d{6}\\\\_R.+\\\\_(.+)\\\\_\\\\d{12}[-+]\\\\d{4}\\\\_.+\\\\.*" );
		conf.put("BCDParser.aomPattern","\"(AOM\\\\d{6})\\\\_R.+\\\\_.+\\\\_\\\\d{12}[-+]\\\\d{4}\\\\_.+\\\\.*\"");
		conf.put("BCDParser.cuPattern","AOM\\d{6}\\_(R.+)\\_.+\\_\\d{12}[-+]\\d{4}\\_.+\\.*");
		conf.put("BCDParser.hostnamePattern","AOM\\\\d{6}\\\\_R.+\\\\_.+\\\\_\\\\d{12}[-+]\\\\d{4}\\\\_(.+)\\\\_.+\\\\.*");
		conf.put("BCDParser.timelevel","24HOUR");
		conf.put("parserType", "bcd");
		conf.put("interfaceName", "INTF_DC_E_BULK_CM");
		conf.put("outDir","/home/indir/sbkafkabulkcm/");
		conf.put("baseDir","/home/indir/bulkcmhandler/");
		conf.put("loaderDir","/home/indir/bulkcmhandler/");
		conf.put("ProcessedFiles.processedDir","/home/indir/bulkcmhandler/");
		conf.put("afterParseAction","delete");
		conf.put("outputFormat","0");
		conf.put("maxFilesPerRun","32765");
		//conf.putAll(inputData.getActionContents());
		File file = new File("/home/indir/bulkcmhandler/MKT_054_dynamic_ENUM-True_CM-Export.txt");
		ParseSession session = new ParseSession(8888, conf);
		SourceFile sf = new SourceFile(file,conf, session, logger);
		
		Parser parser = new Parser(sf,message,interfaceName);
		workers.add(parser);
	}
}
