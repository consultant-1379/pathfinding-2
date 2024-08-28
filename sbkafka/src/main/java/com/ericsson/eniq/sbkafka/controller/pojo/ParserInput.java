package com.ericsson.eniq.sbkafka.controller.pojo;

import java.util.Map;

import lombok.ToString;

@ToString
public final class ParserInput {
	
	//private String inputFile;
	//private String tp;
	//private String setType;
	//private String setName;
	private Map<String, String> actionContents;
	
	public Map<String, String> getActionContents() {
		return actionContents;
	}
	public void setActionContents(Map<String, String> actionContents) {
		this.actionContents = actionContents;
	}
	
	
}
