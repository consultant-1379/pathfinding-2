package com.ericsson.eniq.parsercontroller.feeder.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.ericsson.eniq.parsercontroller.feeder.service.Producer;

@Component
public class Processor {
	
	private static final Logger LOG = LogManager.getLogger(Processor.class);
	
	private static Map<String, Long> trackerMap = new ConcurrentHashMap<>();
	
	@Value("${feeder.jobkey.quiet-interval}")
	private Long quietInterval;
	
	@Value("${producer.topic.prefix}")
	private String topicPrefix;
	
	@Autowired
	private Producer producer;
	
	public void processMessage(String topic, String key, String message) {
		if (canRouteMessage(topic, key)) {
			LOG.log(Level.INFO,"Message IS routed to Parser Controller ");
			producer.produceMessages(topicPrefix.concat(topic), message);
		}
		LOG.log(Level.INFO,"Message IS NOT routed to Parser Controller ");
	}
	
	private Boolean canRouteMessage(String topic, String key) {
		String jobKey = topic.concat("_").concat(key);
		Long lastSentRequestTime = trackerMap.get(jobKey);
		if (lastSentRequestTime == null || 
				(System.currentTimeMillis() - lastSentRequestTime) >= quietInterval) {
			trackerMap.put(jobKey, System.currentTimeMillis());
			return true;
		}
		return false;
	}

}
