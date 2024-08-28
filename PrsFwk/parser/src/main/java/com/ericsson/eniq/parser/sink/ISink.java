package com.ericsson.eniq.parser.sink;

import java.util.Map;

public interface ISink {
	
	//void pushMessage(String key, String message);
	
	void pushMessage(String tagId,Map<String, String> data);
	
	void pushMessage(String tagId, String record);

}
