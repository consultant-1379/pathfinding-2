package com.ericsson.eniq.sbkafka.cache;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import com.ericsson.eniq.parser.cache.DItem;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class SchemaGenerator {
	
	public static final String DEFAULT_SCHEMA_TYPE = "record";
	public static final String DEFAULT_FIELD_TYPE_STRING = "string";
	//public static final Object DEFAULT_VALUE = "null";
	public static final String[] DEFAULT_FIELD_TYPE = {"null", "string"};
	private static final Logger LOG = LogManager.getLogger(SchemaGenerator.class);
	
	private SchemaGenerator() {
		
	}
	
	public static String getAvrSchemaString(AvrSchema schema) {
		Gson gson = new GsonBuilder().serializeNulls().create();
		return gson.toJson(schema, AvrSchema.class);
	}
	
	public static Schema getAvroSchema (String name, Iterator<DItem> dItems) {
		AvrSchema tempSchema = new AvrSchema();
		tempSchema.setType(DEFAULT_SCHEMA_TYPE);
		tempSchema.setName(name);
		List<Field> fields = new ArrayList<>();
		while (dItems.hasNext()) {
			DItem item = dItems.next();
			Field field = new Field();
			field.setName(item.getDataName());
			
			field.setType(new ArrayList(Arrays.asList(DEFAULT_FIELD_TYPE)));
			field.setDefaultValue(null);
			fields.add(field);
		}
		tempSchema.setFields(fields);
		Schema.Parser parser = new Schema.Parser();
		String schemaString = getAvrSchemaString(tempSchema);
		LOG.info("Schema String : "+schemaString);
		return parser.parse(schemaString);
	}
	
	public static Schema getAvroSchema (AvrSchema schema) {
		Gson gson = new GsonBuilder().serializeNulls().create();
		String schemaString = gson.toJson(schema, AvrSchema.class);
		LOG.info("Schema String : "+schemaString);
		Schema.Parser parser = new Schema.Parser();
		return parser.parse(schemaString);
	}
	
	
	public static void main(String[] args) {
		Field field1 = new Field();
		field1.setName("Col1");
		field1.setType(new ArrayList(Arrays.asList(DEFAULT_FIELD_TYPE)));
		field1.setDefaultValue(null);
		Field field2 = new Field();
		field2.setName("Col2");
		field2.setType(new ArrayList(Arrays.asList(DEFAULT_FIELD_TYPE)));
		field2.setDefaultValue(null);
		List<Field> fields = new ArrayList<>();
		fields.add(field1);
		fields.add(field2);
		AvrSchema schema = new AvrSchema();
		schema.setType("record");
		schema.setName("schema1");
		schema.setFields(fields);
		String schemaString = SchemaGenerator.getAvrSchemaString(schema);
		System.out.println("Schema String :" + schemaString);
		Schema.Parser parser = new Schema.Parser();
		GenericRecord record = new GenericData.Record(parser.parse(schemaString));
		Map<String, String> tempMap = new HashMap<>();
		tempMap.put("Col2", "");
		tempMap.forEach(record::put);
		System.out.println("record out : " + record.toString());
		
		//PWriter.write(null, schemaString);
	}

}
