package com.ericsson.eniq.sbkafka.cache;

public class Subject {
	
	//"subject":"PM_E_ERBS_DATA-CapacityConnectedUsers_V","version":1,"id":50,"schema":{}
	
	String subject;
	
	int version;
	
	int id;
	
	String schema;

	public String getSubject() {
		return subject;
	}

	public void setSubject(String subject) {
		this.subject = subject;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	
	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	@Override
	public String toString() {
		return "Subject [subject=" + subject + ", version=" + version + ", id=" + id + ", schema=" + schema + "]";
	}

}
