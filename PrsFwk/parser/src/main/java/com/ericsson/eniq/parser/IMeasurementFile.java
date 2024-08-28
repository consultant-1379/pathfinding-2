package com.ericsson.eniq.parser;

import java.util.Map;

public interface IMeasurementFile {
	
	/**
	 * Adds one name value pair to MeasurementFile
	 * 
	 * @param name
	 *            attribute name
	 * @param value
	 *            attribute value
	 */
	void addData(String name, String value);

	/**
	 * Adds a Map of data to MeasurementFile
	 * 
	 * @param map
	 *            Map of data
	 */
	void addData(Map<String, String> map);

	/**
	 * Sets the data to MeasurementFile
	 */
	void setData(Map<String, String> map);
	
	/**
	 * Returns the current status of Measurement File
	 * @return
	 */
	
	/**
	 * writes the data to the file
	 * @throws Exception
	 */
	void saveData() throws Exception;
	
	MeasurementFileStatus getStatus();
	
	/**
	 * Closes the MeasurementFile. 
	 * Will be taken care automatically (At the end of each parser batch), if not invoked explicitly
	 */
	void close();

}
