package com.ericsson.eniq.parser.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class TransformerCache {

	private static final Logger logger = LogManager.getLogger(TransformerCache.class);
	private static volatile Map<String, Transformer> transformers = new ConcurrentHashMap<>();
	
	private static class InstanceHolder {
		static final TransformerCache tfc = new TransformerCache();
	}
	
	public Transformer getTransformer(final String transformerID) {
		if( transformerID != null && !transformerID.isEmpty()) {
			Transformer t = transformers.get(transformerID);
			if (t != null) {
				return t;
			} 
		}
		return null;
	}
	
	
	public void addTransformation(final String transformerID, String transformationType, String source, String target, String config) throws Exception {
		Transformer transformer;
		
		if(transformers.containsKey(transformerID)) {
			transformer = transformers.get(transformerID);
		}else {
			transformer = new Transformer();
		}
		
		transformer.addTransformation(transformationType, source, target, config);
		
		transformers.put(transformerID, transformer);
	}
	
	
	public static TransformerCache getCache() {
		return InstanceHolder.tfc;
	}
	
	public void listTransformer() throws Exception {
		for (Map.Entry<String, Transformer> entry : transformers.entrySet()) {
			String id = entry.getKey();
			Transformer trans = entry.getValue();
			
		    System.out.println(id);
		    trans.listTransformations();
			
		}
	}
	
	
}
