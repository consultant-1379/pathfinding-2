package com.ericsson.eniq.loadfilebuilder.outputstream;

import org.apache.avro.generic.GenericRecord;

public interface ILoadFile {
	
	void save(GenericRecord message);
	
	//void save(String message);

}
