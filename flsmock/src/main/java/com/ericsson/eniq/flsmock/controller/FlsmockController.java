package com.ericsson.eniq.flsmock.controller;

import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.ericsson.eniq.flsmock.pojo.ParserInput;
import com.ericsson.eniq.flsmock.pojo.ProducerInput;
import com.ericsson.eniq.flsmock.service.Producer;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

@Controller
public class FlsmockController {

	private static final Logger log = LogManager.getLogger(FlsmockController.class);
	
	//private static final String PM_INTF_TAG = "INTF_DC";
	//private static final String TOPO_INTF_TAG = "INTF_DIM";
	//private static final String PM_TOPIC_PREFIX = "PM";
	//private static final String TOPO_TOPIC_PREFIX = "TOPO";
	//private static final String TOPIC_SUFFIX = "_FILES";
	
	//private static final String INTF_PATTERN = "^.+(_E_.+)$";

	int batchId = 1000;

	
	Producer producer;
	
	@Autowired
	public FlsmockController(Producer producer) {
		this.producer = producer;
	}
	

	@RequestMapping(value = "/produce", method = RequestMethod.POST, consumes = "application/json", produces = "text/plain")
	@ResponseBody
	public String produceMessages(@RequestBody ProducerInput input) {

		log.log(Level.INFO, "input Received : " + input.toString());

		for (int i = 0; i < input.getTotalBatches(); i++) {
			batchId++;
			produceBatch(batchId, input);

		}
		return "Messages produced";

	}

	/*private ParserInput getParserInputTemplate() {

		try (InputStream ris = resourceFile.getInputStream();
				JsonReader reader = gson.newJsonReader(new InputStreamReader(ris))) {
			return gson.fromJson(reader, ParserInput.class);
		} catch (Exception e) {
			log.log(Level.ERROR, "Exception while reading json", e);
		}

		return null;
	}*/

	private void produceBatch(int batchId, ProducerInput producerInput) {
		boolean repeat = producerInput.getRepeat();
		log.log(Level.INFO, "repeat is set to : " + repeat);
		if (repeat) {
			produceDuplicates(batchId, producerInput);
		}
		
	}

	private void produceDuplicates(int batchId, ProducerInput producerInput) {
		log.log(Level.INFO, "producing duplicates for batchId : " + batchId);
		int batchSize = producerInput.getMessagesPerBatch();
		for (int i = 0; i < batchSize; i++) {
			producer.produceMessages(producerInput.getTopic(), producerInput.getInputFile());
		}
	}
	
	/*public static String getTopic(String intf) {
		String topic = null;
		String intfGroup;
		if (intf != null && (intfGroup = getGroup(intf, INTF_PATTERN)) != null) {
			if (intf.startsWith(PM_INTF_TAG)) {
				topic = PM_TOPIC_PREFIX+intfGroup+TOPIC_SUFFIX;
			}else if (intf.startsWith(TOPO_INTF_TAG)) {
				topic = TOPO_TOPIC_PREFIX+intfGroup+TOPIC_SUFFIX;
			}
		} else {
			log.log(Level.INFO, "no valid input provided for intf, intf = "+intf);
		}
		return topic;
	}*/
	
	/*private static String getGroup(String input, String pattern) {
		String group = null;
		Pattern p = Pattern.compile(pattern);
		Matcher m = p.matcher(input);
		if (m.matches()) {
			group =  m.group(1);
		}
		return group;
	}*/

}
