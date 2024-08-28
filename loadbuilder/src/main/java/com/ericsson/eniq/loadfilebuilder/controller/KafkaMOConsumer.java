package com.ericsson.eniq.loadfilebuilder.controller;



import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import com.ericsson.eniq.loadfilebuilder.cache.TableNameCache;
import com.ericsson.eniq.loadfilebuilder.outputstream.ILoadFile;
import com.ericsson.eniq.loadfilebuilder.outputstream.LoadFileRepo;
import com.ericsson.eniq.loadfilebuilder.outputstream.Writer;




@Service
public class KafkaMOConsumer {

	private static final Logger LOG = LogManager.getLogger(KafkaMOConsumer.class);
	
	@Value("${out.dir}")
	private String outDir;

	@Value("${batch.size}")
	private String batchSize;
	
	@Value("${num.of.writers}")
	private int numOfWriters;
	
	private static final Map<Integer,Writer> writers = new HashMap<>();
	
	
	@KafkaListener(id = "#{'${consumer.id}'}", topics = "#{'${spring.kafka.consumer.topic}'}", groupId = "#{'${spring.kafka.consumer.group-id}'}", containerFactory = "batchFactory")
	void listen(ConsumerRecords<String, GenericRecord> consumerRecords, Acknowledgment acknowledgment) {
		if (writers.isEmpty()) {
			ExecutorService execService = Executors.newFixedThreadPool(numOfWriters);
			Writer writer;
			LOG.log(Level.INFO, "number of writers to be created = "+numOfWriters);
			LOG.log(Level.INFO, "outDir = "+outDir);
			LOG.log(Level.INFO, "batchSize = "+batchSize);
			for (int i = 0 ; i < numOfWriters ; i++) {
				writer = new Writer();
				writers.put(i,writer);
				execService.execute(writer);
				LOG.log(Level.INFO, "Writer created, index = "+i);
			}
		}
		
		for (ConsumerRecord<String, GenericRecord> consumerRecord : consumerRecords) {
			String key = consumerRecord.key();
			GenericRecord message = consumerRecord.value();
			//LOG.log(Level.INFO, "Record details : partitiion = " + consumerRecord.partition() + " ,offset = "
					//+ consumerRecord.offset());
			//LOG.log(Level.INFO, "KafkaMOConsumer {} : key = " + key + " ### message = " + message);
			ILoadFile loadFile = LoadFileRepo.getLoadFile(outDir + File.separator + TableNameCache.getTpName(key), TableNameCache.getFolderName(key));
			Data data = new Data(message, loadFile);
			getWriteThread(key).add(data);
					
		}
		acknowledgment.acknowledge();
	}
	
	
	private Writer getWriteThread(String folderName) {
		int index = Math.abs(folderName.hashCode()%numOfWriters);
		return writers.get(index);
	}
	
	public final class Data {
		GenericRecord message;
		//String message;
		ILoadFile loadFile;
		
		public Data(GenericRecord message, ILoadFile loadFile) {
			this.message = message;
			this.loadFile = loadFile;
		}

		public GenericRecord getMessage() {
			return message;
		}

		public ILoadFile getLoadFile() {
			return loadFile;
		}
	}

	
}
