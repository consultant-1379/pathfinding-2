package com.ericsson.eniq.flsmock.pojo;

import lombok.ToString;

@ToString
public class ProducerInput {
	
	String topic;
	String inputFile;
	Integer totalBatches;
	Integer messagesPerBatch;
	Boolean repeat;
	
	
	public String getTopic() {
		return topic;
	}
	public void setTopic(String topic) {
		this.topic = topic;
	}
	public String getInputFile() {
		return inputFile;
	}
	public void setInputFile(String inputFile) {
		this.inputFile = inputFile;
	}
	public Integer getTotalBatches() {
		return totalBatches;
	}
	public void setTotalBatches(Integer totalBatches) {
		this.totalBatches = totalBatches;
	}
	public Integer getMessagesPerBatch() {
		return messagesPerBatch;
	}
	public void setMessagesPerBatch(Integer messagesPerBatch) {
		this.messagesPerBatch = messagesPerBatch;
	}
	public Boolean getRepeat() {
		return repeat;
	}
	public void setRepeat(Boolean repeat) {
		this.repeat = repeat;
	}
	

}
