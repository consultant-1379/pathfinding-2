package com.ericsson.eniq.sbkafka.controller.pojo;

import java.util.Map;

import lombok.ToString;

@ToString
public class InterfaceProperties {
	
private Map<String, String> actionContents;
	
	public Map<String, String> getActionContents() {
		return actionContents;
	}
	public void setActionContents(Map<String, String> actionContents) {
		this.actionContents = actionContents;
	}

}
