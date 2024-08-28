package com.ericsson.eniq.etl.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;


public class PWriter {
	
	public static void write(List<GenericData.Record> records, String schemaString) {
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(schemaString);
		write(getRecords(schema), schema);
	}
	
	
	private static void write(List<GenericData.Record> records, Schema schema) {
		Path outputPath = new Path("H:\\WDP\\temp\\parquet\\out2.parquet");
		//ParquetWriter<GenericData.Record> writer = null;
		try(ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(outputPath)
					.withDictionaryEncoding(false)
					.withSchema(schema)
					.withConf(new Configuration())
					.withCompressionCodec(CompressionCodecName.SNAPPY)
					.withValidation(false)
					.withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
					.withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
					.build();) {
			
			for (GenericData.Record record : records) {
				writer.write(record);
			}
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}
	private static List<GenericData.Record> getRecords(Schema schema) {
		
		List<GenericData.Record> recordList = new ArrayList<>();
		GenericData.Record record = new GenericData.Record(schema);
		record.put("Col1", '3');
		record.put("Col2", "test_record3");
		recordList.add(record);
		
		record = new GenericData.Record(schema);
		record.put("Col1", '4');
		record.put("Col2", "test_record4");
		recordList.add(record);
		
		return recordList;
	}

}
