package com.ericsson.eniq.sbkafka.controller;

import java.lang.management.ManagementFactory;
import java.util.Map;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
//import org.springframework.boot.actuate.autoconfigure.metrics.export.prometheus.PrometheusProperties;
//import org.springframework.boot.actuate.autoconfigure.metrics.export.prometheus.PrometheusProperties.Pushgateway;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.ericsson.eniq.parser.sink.ISink;
import com.ericsson.eniq.sbkafka.cache.DataFormatCacheImpl;

//import io.micrometer.core.instrument.Counter;
//import io.micrometer.core.instrument.MeterRegistry;
//import io.prometheus.client.CollectorRegistry;

@Service
public class SbKafkaProducer implements ISink, ICustomMetricsMBean {

	
	private KafkaTemplate<String, GenericRecord> kafkaTemplate;

	private static final Logger LOG = LogManager.getLogger(SbKafkaProducer.class);
	
	String topic;
	
		
	//private CollectorRegistry collectorRegistry;
	
	//private final MeterRegistry meterRegistry;
	
	//private Counter counter;
	
	private boolean isRegisteredForMetrics;
	
	
	int rowCount;
	
	ObjectName objectName;

	@Autowired 
	public SbKafkaProducer(KafkaTemplate<String, GenericRecord> kafkaTemplate, @Value("#{'${producer.topic}'}") String topic) {
		this.kafkaTemplate = kafkaTemplate;
		this.topic = topic;
		//this.meterRegistry = meterRegistry;
		//this.counter = Counter.builder("rows-counter").register(meterRegistry);
		registerForMetrics();
		
		
						
	}
	
	@Override
	public int getTotalRows() {
		return rowCount;
	}
	
	private void registerForMetrics() {
		MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
		final Class<? extends ICustomMetricsMBean> objectClass = this.getClass();
		try {
			StandardMBean mbean = new StandardMBean(this, ICustomMetricsMBean.class);
			ObjectName objectName = new ObjectName(
					String.format("%s:type=basic,name=%s", objectClass.getPackage().getName(), objectClass.getName()));
			ObjectInstance instance = mBeanServer.registerMBean(mbean, objectName);
			LOG.log(Level.INFO, "Registered the customMetricsMBean, objectName = {}, instance = {}", objectName,
					instance);
		} catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException
				| NotCompliantMBeanException e) {
			LOG.log(Level.WARN, "Not able to create ObjectName : ", e);
		}
	}

	@Override
	public void pushMessage(String tagId, Map<String, String> data) {
		
		long convStartTime = System.nanoTime();
		Schema schema = DataFormatCacheImpl.getSchema(tagId);
		if (schema != null) {
			GenericRecord record = new GenericData.Record(schema);
			String colName;
			String value;
			for (Field field : record.getSchema().getFields()) {
				colName = field.name();
				value = data.get(colName);
				if (value == null) {
					value = "";
				}
				record.put(colName, value);
			}
			long convEndTime = System.nanoTime();
			 
			    
			//LOG.log(Level.INFO, "Time taken for conv in nanos: "+(convEndTime-convStartTime));
			//String pushValue = record.toString();
			long pushStartTime = System.nanoTime();
			if (tagId != null) {
				kafkaTemplate.send("PM_E_ERBS_DATA", tagId, record);
				//counter.increment();
				rowCount++;
			} else {
				LOG.log(Level.WARN, "tagId is null for : " + tagId);
			}
			
			
			long pushEndTime = System.nanoTime();
			//LOG.log(Level.INFO, "Time taken for push in nanos: "+(pushEndTime-pushStartTime));
			//LOG.log(Level.INFO, "Time taken for conv + push in nanos: "+(pushEndTime-convStartTime));
		} else {
			LOG.log(Level.WARN, "No schema found for foldername : " + tagId);
		}
		
	}
	

	@Override
	public void pushMessage(String tagId, String record) {
		throw new UnsupportedOperationException();
		
	}

}
