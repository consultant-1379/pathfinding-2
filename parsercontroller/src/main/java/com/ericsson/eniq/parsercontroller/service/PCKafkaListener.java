package com.ericsson.eniq.parsercontroller.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import com.ericsson.eniq.parsercontroller.service.KubeClient.JobStatus;

import io.fabric8.kubernetes.api.model.batch.Job;


@Service
public class PCKafkaListener {
	
			
	private static final Logger LOG = LogManager.getLogger(PCKafkaListener.class);
	
	KubeClient kubeClient;
	
	PCKafkaProducer producer;
	
	Environment env;
	
	BatchJobTracker tracker;
	
	@Value("${k8s.job.creation.max-retry}")
	int jobCreationMaxRetry;
	
	@Value("${k8s.job.creation.retry-interval}")
	Long jobCreationRetryInterval;
	
	@Autowired
	public PCKafkaListener(KubeClient kubeClient, PCKafkaProducer producer, Environment env,@Value("${cleanup-job.frequency}") Long delay ) {
		this.kubeClient = kubeClient;
		this.producer = producer;
		this.env = env;
		LOG.log(Level.INFO, "kubeClient = {}, producer = {}, env = {} , delay = {}",kubeClient, producer, env, delay);
		tracker = new BatchJobTracker(kubeClient, delay);
		tracker.init();
		LOG.log(Level.INFO, "Tracker Initialized");
	}
	
	@KafkaListener(id = "#{'${consumer.id}'}", topics = "#{'${kafka-consumer.topics}'.split(',')}", groupId = "#{'${spring.kafka.consumer.group-id}'}", containerFactory = "batchFactory")
	public void listen(ConsumerRecords<String, String> consumerRecords, Acknowledgment acknowledgment) {
		handleRecords(consumerRecords);
		acknowledgment.acknowledge();
	}
	
	public void handleRecords(ConsumerRecords<String, String> consumerRecords) {
		for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
			LOG.log(Level.INFO, "Record details : partition = " + consumerRecord.partition() + " ,offset = "
					+ consumerRecord.offset());
			String jobFile = getJobFile(consumerRecord.topic());
			if (jobFile != null) {
				LOG.log(Level.INFO, "Check for job creation");
				checkJobCreation(jobFile);
			} else {
				LOG.log(Level.WARN, "JobFile does not exist for ingress Topic : {}",consumerRecord.topic());
			}
		}
	}
	
	private void checkJobCreation(String jobFile) {
		JobStatus status = kubeClient.getJobStatus(jobFile);
		switch(status) {
		case NOT_EXIST:
			LOG.log(Level.INFO, "Job Does not exists for file {} , going to trigger creation", jobFile);
			createJob(jobFile);
			break;
		case EXIST_NOT_COMPLETED:
			LOG.log(Level.INFO, "Job exists and not in completed state for file {} , no action needed", jobFile);
			break;
		case EXIST_COMPLETED:
			LOG.log(Level.INFO, "Job exists but in completed state for file {} , going to trigger deletion and then re-creation", jobFile);
			kubeClient.clearJob(jobFile);
			createJob(jobFile);
			break;
			default:
				LOG.log(Level.INFO, "Not a recognized status : {}",status);
		}
	}
	
	private void createJob(String jobFile) {
		Job job = null;
		int retryCount = 0;
		while (job == null && retryCount < jobCreationMaxRetry) {
			retryCount++;
			job = kubeClient.triggerParser(jobFile);
			if (job == null) {
				JobStatus status = kubeClient.getJobStatus(jobFile);
				if (status != JobStatus.NOT_EXIST) {
					return;
				}
				try {
					LOG.log(Level.INFO, "Not able to create job, retrying after {} seconds", jobCreationRetryInterval);
					Thread.sleep(jobCreationRetryInterval*1000);
				} catch (InterruptedException e) {
					LOG.log(Level.WARN, "Thread interrupted while retrying for Job creation", e);
					Thread.currentThread().interrupt();
				}
			} else {
				LOG.log(Level.INFO, "Job creation Successful, job = {}",job.getMetadata().getName());
			}
		}
	}
	
	private void routeMessage(ConsumerRecord<String, String> consumerRecord) {
		String outTopic = getOutTopic(consumerRecord.topic());
		if (outTopic != null) {
			producer.produceMessages(outTopic, consumerRecord.key(), consumerRecord.value());
		} else {
			LOG.log(Level.INFO, "OUT Topic is not found for the In Topic :  " + consumerRecord.topic());
		}
		LOG.log(Level.INFO, "Routed message : " + consumerRecord.value());
	}
	
	private String getOutTopic(String inTopic) {
		return env.getProperty("config.topic.outtopic."+inTopic);
	}
	
	private String getJobFile(String inTopic) {
		return env.getProperty("config.topic.job."+inTopic);
	}
	

}
