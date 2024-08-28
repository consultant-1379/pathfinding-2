package com.ericsson.eniq.sbkafka.cache;

import java.util.List;

import com.google.gson.annotations.SerializedName;

public class Field {
	
	private String name;
	private List<String> type;
	
	@SerializedName(value = "default")
	private Object defaultValue;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
		
	
	public List<String> getType() {
		return type;
	}
	public void setType(List<String> type) {
		this.type = type;
	}
	public Object getDefaultValue() {
		return defaultValue;
	}
	public void setDefaultValue(Object defaultValue) {
		this.defaultValue = defaultValue;
	}
	
	
	
	

}
