package com.ericsson.eniq.etl.sb_rest_cs.pojo;

import java.util.Map;

public class ParserInputForm {

	private String inputFile;
	private String tp;
	private String setType;
	private String setName;
	private Map<String, String> actionContents;
	public String getInputFile() {
		return inputFile;
	}
	public void setInputFile(String inputFile) {
		this.inputFile = inputFile;
	}
	public String getTp() {
		return tp;
	}
	public void setTp(String tp) {
		this.tp = tp;
	}
	public String getSetType() {
		return setType;
	}
	public void setSetType(String setType) {
		this.setType = setType;
	}
	public String getSetName() {
		return setName;
	}
	public void setSetName(String setName) {
		this.setName = setName;
	}
	public Map<String, String> getActionContents() {
		return actionContents;
	}
	public void setActionContents(Map<String, String> actionContents) {
		this.actionContents = actionContents;
	}
	@Override
	public String toString() {
		return "ParserInputForm [inputFile=" + inputFile + ", tp=" + tp + ", setType=" + setType + ", setName="
				+ setName + ", actionContents=" + actionContents + "]";
	}
	
	
}
