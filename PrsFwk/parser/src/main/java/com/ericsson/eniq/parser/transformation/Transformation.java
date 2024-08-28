package com.ericsson.eniq.parser.transformation;

import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.Logger;

public interface Transformation {

	void configure(String name, String srcKey, String tgtKey, Properties props, Logger logger) throws Exception;

	void transform(Map data, Logger logger) throws Exception;

	String getSource() throws Exception;

	String getTarget() throws Exception;

	String getName() throws Exception;
	

}
