package com.ericsson.eniq.sbkafka.cache;
import java.util.List;

public class AvrSchema {
	private String type;
	private String name;
	private List<Field> fields;
	
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public List<Field> getFields() {
		return fields;
	}
	public void setFields(List<Field> fields) {
		this.fields = fields;
	}
	@Override
	public String toString() {
		return "AvrSchema [type=" + type + ", name=" + name + ", fields=" + fields + "]";
	}
	
	
}
