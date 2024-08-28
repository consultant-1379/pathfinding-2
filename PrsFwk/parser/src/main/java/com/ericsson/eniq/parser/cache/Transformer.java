package com.ericsson.eniq.parser.cache;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ericsson.eniq.parser.transformation.Transformation;
import com.ericsson.eniq.parser.transformation.*;

public class Transformer {
	
	private static final Logger logger = LogManager.getLogger(Transformer.class);
	private final List<Transformation> transformations = new ArrayList<Transformation>();
	
	public void addTransformation(String transformationType, String source, String target, String config) throws Exception {
		
		Transformation transformation = createTransformationInstance(transformationType);
		
		Properties configuration = new Properties();
        try {
		
		configuration.load(new StringReader(config));
		
		transformation.configure(transformationType, source, target, configuration, logger);
		
		transformations.add(transformation);
        } catch (Exception e) {
        	System.out.println("exception : "+e.getMessage());
        	System.out.println("config = " + config);
        	
        }
		
	}
	
	private Transformation createTransformationInstance(String transformationType) {
		Transformation transformation = null;
		
		try {
			transformation = (Transformation) Class.forName("com.ericsson.eniq.paser.transformation." + transformationType).newInstance();
		} catch (Exception e) {
			transformation = lookupByType(transformationType);
		}
		
		
		return transformation;
		
	}
	
	private Transformation lookupByType(String transformationType) {
		Transformation t = null;
		
		if (transformationType.equalsIgnoreCase("lookup")) {
	        t = new Lookup();
	      } else if (transformationType.equalsIgnoreCase("databaselookup")) {
	        t = new DatabaseLookup();
	      } else if (transformationType.equalsIgnoreCase("condition")) {
	        t = new Condition();
	      } else if (transformationType.equalsIgnoreCase("radixConverter")) {
	        t = new RadixConverter();
	      } else if (transformationType.equalsIgnoreCase("dateformat")) {
	        t = new DateFormat();
	      } else if (transformationType.equalsIgnoreCase("defaulttimehandler")) {
	        t = new DefaultTimeHandler();
	      } else if (transformationType.equalsIgnoreCase("fixed")) {
	        t = new Fixed();
	      } else if (transformationType.equalsIgnoreCase("postappender")) {
	        t = new PostAppender();
	      } else if (transformationType.equalsIgnoreCase("preappender")) {
	        t = new PreAppender();
	      } else if (transformationType.equalsIgnoreCase("reducedate")) {
	        t = new ReduceDate();
	      } else if (transformationType.equalsIgnoreCase("roundtime")) {
	        t = new RoundTime();
	      } else if (transformationType.equalsIgnoreCase("switch")) {
	        t = new Switch();
	      } else if (transformationType.equalsIgnoreCase("copy")) {
	        t = new Copy();
	      } else if (transformationType.equalsIgnoreCase("propertytokenizer")) {
	        t = new PropertyTokenizer();
	      } else if (transformationType.equalsIgnoreCase("fieldtokenizer")) {
	        t = new FieldTokenizer();
	      } else if (transformationType.equalsIgnoreCase("calculation")) {
	        t = new Calculation();
	      } else if (transformationType.equalsIgnoreCase("currenttime")) {
	        t = new CurrentTime();
	      } else if (transformationType.equalsIgnoreCase("dstparameters")) {
	        t = new DSTParameters();
	      } else if (transformationType.equalsIgnoreCase("convertipaddress")) {
	        t = new ConvertIpAddress();
	      } else if (transformationType.equalsIgnoreCase("roptime")) {
	        t = new ROPTime();
	      } else if (transformationType.equalsIgnoreCase("configlookup")) {
	    	t = new ConfigLookup();
	      }
		
		return t;
	}
	
	
	
	public void transform(Map data) throws Exception {
		
		for(Transformation transformation : transformations) {
			transformation.transform(data, logger);
		}
		
	}
	
	public void listTransformations() throws Exception {
		for(Transformation trans : transformations) {
			System.out.println("\tTransformation [Name=" + trans.getName() 
			+ ", Source=" + trans.getSource() + ", Target=" + trans.getTarget()+ "]");
		}
		
	}
	
	

}
