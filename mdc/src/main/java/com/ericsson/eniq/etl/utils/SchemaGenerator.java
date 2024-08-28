package com.ericsson.eniq.etl.utils;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;

import com.distocraft.dc5000.repository.cache.DItem;
import com.google.gson.Gson;

public class SchemaGenerator {
	
	public static final String DEFAULT_SCHEMA_TYPE = "record";
	public static final String DEFAULT_FIELD_TYPE = "string";
	
	
	public static String getAvrSchemaString(AvrSchema schema) {
		Gson gson = new Gson();
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
			field.setType(DEFAULT_FIELD_TYPE);
			fields.add(field);
		}
		tempSchema.setFields(fields);
		Schema.Parser parser = new Schema.Parser();
		String schemaString = getAvrSchemaString(tempSchema);
		System.out.println("Schema String : "+schemaString);
		return parser.parse(schemaString);
	}
	
	
	public static void main(String[] args) {
		Field field1 = new Field();
		field1.setName("Col1");
		field1.setType("string");
		Field field2 = new Field();
		field2.setName("Col2");
		field2.setType("string");
		List<Field> fields = new ArrayList<>();
		fields.add(field1);
		fields.add(field2);
		AvrSchema schema = new AvrSchema();
		schema.setType("record");
		schema.setName("schema1");
		schema.setFields(fields);
		String schemaString = SchemaGenerator.getAvrSchemaString(schema);
		System.out.println("Schema String :" + schemaString);
		PWriter.write(null, schemaString);
	}

}
