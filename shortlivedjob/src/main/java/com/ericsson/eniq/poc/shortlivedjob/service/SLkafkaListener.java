package com.ericsson.eniq.poc.shortlivedjob.service;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class SLkafkaListener {
	
	private static final Logger LOG = LogManager.getLogger(SLkafkaListener.class);
	
	//private  int countDownStartVal = 120;
	//private  int countDownCurrentVal = 0;
	//private  boolean isCountDownReset = false;
	//private  boolean isCountDownTimerRunning = false;
	//private  IdleConsumerListener listener;
	
	
	//ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
	
	@KafkaListener(id = "#{'${consumer.id}'}", topics = "#{'${kafka-consumer.topics}'.split(',')}", groupId = "#{'${spring.kafka.consumer.group-id}'}", containerFactory = "batchFactory")
	public void listen(ConsumerRecords<String, String> consumerRecords, Acknowledgment acknowledgment) {
		//startCountDownTimer();
		handleRecords(consumerRecords);
		acknowledgment.acknowledge();
	}
	
	public void handleRecords(ConsumerRecords<String, String> consumerRecords) {
		for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
			String message = consumerRecord.value();
			LOG.log(Level.INFO, "Record details : partition = " + consumerRecord.partition() + " ,offset = "
					+ consumerRecord.offset());
			LOG.log(Level.INFO, "message : " + message);
		}
	}
	
	/*private void startCountDownTimer() {
		if (isCountDownTimerRunning) {
			isCountDownReset = true;
			countDownCurrentVal = 0;
			return;
			
		}
		Runnable countDownTimerTask = () -> {
			if ((countDownStartVal - countDownCurrentVal) > 0){
				countDownCurrentVal += 1;
			} else {
				notifyListener();
				isCountDownTimerRunning = false;
				
			}
		};
		isCountDownTimerRunning = true;
		executorService.scheduleAtFixedRate(countDownTimerTask, 0, 1, TimeUnit.SECONDS);
	}*/
	
	/*private void shutCountDownTimer() {
		executorService.shutdownNow();
	}*/
	
	/*public void registerListener(IdleConsumerListener listener) {
		this.listener = listener;
	}*/
	
	/*private void notifyListener() {
		if (listener != null) {
			listener.onIdle();
		}
	}*/

}
